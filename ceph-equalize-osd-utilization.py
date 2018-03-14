# ceph-equalize-osd-utilization
# Copyright (C) 2018 Stephen Taylor
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import ast
import os
import subprocess
import time

# Change these to command-line arguments
cluster = 'ceph'
target_variance = 0.005
max_iterations = 10000

# Run a shell command and return its output
def command_output(command):
    return subprocess.check_output(command, shell=True).decode('utf-8')

# run a shell command and return its return code
def run_command(command):
    try:
        subprocess.check_call(command, shell=True)
        return 0
    except subprocess.CalledProcessError as e:
        return e.returncode

# Check for peering
def peering():
    return run_command('ceph --cluster {} health detail | grep peering > /dev/null'.format(cluster)) == 0

# The OSD and pool data are expected to remain static during the reweighting process
# Get a list of OSDs, a dictionary of OSD disk sizes, and a dictionary of per-OSD pool storage based on replication strategy
osds = sorted([int(x) for x in command_output('ceph --cluster {} osd ls'.format(cluster)).split('\n') if x != ''])
osd_disk_size = {int(fields[0]): int(fields[1]) * 1073741824 for fields in [x.split() for x in command_output("""ceph --cluster {} osd df | head -n {} | tail -n {} | awk '{{print $1 " " $5}}' | cut -d'G' -f1""".format(cluster, len(osds) + 1, len(osds))).split('\n') if x != '']}
pool_dividend = {int(fields[0]): int(fields[2]) if fields[1] == 'erasure' else 1 for fields in [x.split() for x in command_output("""ceph --cluster {} osd dump | grep pool | awk '{{print $2 " " $4 " " $8}}'""".format(cluster)).split('\n') if x != '']}

# Return a dictionary of osd crush weights
def get_osd_crush_weight():
    return {int(fields[0]): float(fields[1]) for fields in [x.split() for x in command_output("""ceph --cluster {} osd df | head -n {} | tail -n {} | awk '{{print $1 " " $3}}'""".format(cluster, len(osds) + 1, len(osds))).split('\n') if x != '']}

# Return a dictionary of computed osd variances based on pg sizes and mappings
def get_osd_variance():
    osd_size = {osd: 0 for osd in osds}
    pg_data = [x for x in command_output("""ceph --cluster {} pg dump pgs 2> /dev/null | grep -v "PG_STAT" | awk '{{print $1 " " $7 " " $15}}'""".format(cluster)).split('\n') if x != '']
    pgs = []
    pg_size = {}
    pg_osds = {}

    # Use the pg dump data to build a list of pgs and dictionaries of pg sizes and osd mappings
    for pg_datum in pg_data:
        pg_fields = pg_datum.split()
        pgs.append(pg_fields[0])
        pg_size[pg_fields[0]] = int(pg_fields[1])
        pg_osds[pg_fields[0]] = ast.literal_eval(pg_fields[2])

    # Use the pg sizes and mappings to compute used space for each osd
    for pg in pgs:
        pool = int(pg.split('.')[0])
        for osd in pg_osds[pg]:
            osd_size[osd] += pg_size[pg] / pool_dividend[pool]

    # Compute the full percentage for each OSD, compute an average, then compute each OSD's variance and return a dictionary
    osd_percent_full = {osd: float(osd_size[osd]) / osd_disk_size[osd] for osd in osds}
    osd_average = float(sum(osd_percent_full.values())) / len(osd_size)
    osd_variance = {osd: float(osd_percent_full[osd]) / osd_average for osd in osds}
    return osd_variance

# Use the current OSD variances and a target threshold to determine if reweighting is needed
def need_reweight(osd_variance, threshold):
    need_reweight = False

    for osd in osds:
        variance = osd_variance[osd]
        if variance < 1 - threshold or variance > 1 + threshold:
            need_reweight = True

    return need_reweight

# Set nobackfill and norecover flags on the cluster to prevent data movement while reweighting is in progress
run_command('ceph --cluster {} osd set nobackfill'.format(cluster))
run_command('ceph --cluster {} osd set norecover'.format(cluster))

# Save a crush map in case we can't find a better balance than the current state
run_command('ceph --cluster {} osd getcrushmap -o crushmap.original 2> /dev/null'.format(cluster))

# Get current OSD variances, the maximum variance, and determine our temporary target threshold
# The temporary target threshold will be right in the middle of where we are now and where we want to be eventually
osd_variance = get_osd_variance()
original_max_variance = max([abs(1 - variance) for variance in osd_variance.values()])
max_variance = original_max_variance
threshold = (max_variance + target_variance) / 2

# Loop until the maximum variance is less then or equal to the target variance
while max_variance > target_variance:
    iteration_count = 0

    # Save a crush map for the current state of the cluster so we can revert back to it if the target is unreachable
    crush_map = 'crushmap.{}'.format(threshold)
    run_command('ceph --cluster {} osd getcrushmap -o {} 2> /dev/null'.format(cluster, crush_map))

    while iteration_count < max_iterations and need_reweight(osd_variance, threshold):
        crush_weight = get_osd_crush_weight()

        # Reweight every OSD outside of the current target threshold by a small amount to try to bring it within the target
        for osd in osds:
            variance = osd_variance[osd]
            new_weight = crush_weight[osd]
            # Use a conservative weight increment/decrement that is 1% of the OSD's variance
            increment = new_weight * abs(1 - variance) / 100
            if variance < 1 - threshold:
                new_weight += increment
            elif variance > 1 + threshold:
                new_weight -= increment
            if new_weight != crush_weight[osd]:
                print('osd.{} has a variance of {}, reweighting to {}'.format(osd, variance, new_weight))
                run_command('ceph --cluster {} osd crush reweight osd.{} {} 2> /dev/null'.format(cluster, osd, new_weight))
                time.sleep(5)

                # Wait for peering in between each reweight to minimize blocked I/O requests
                while peering():
                    time.sleep(5)

        # Get a new list of variances and find the maximum for the next iteration
        osd_variance = get_osd_variance()
        max_variance = max([abs(1 - variance) for variance in osd_variance.values()])
        iteration_count += 1

    if iteration_count >= max_iterations:
        # If we have reached the maximum number of iterations and we are already really close to what we're aiming for,
        # give up and stick with the crush map we saved from the previous iteration
        if max_variance - threshold <= 0.001:
            restore_crush_map = crush_map

            # We're giving up, so revert to the original crush map if the current state isn't better
            if max_variance >= original_max_variance:
                restore_crush_map = 'crushmap.original'

            run_command('ceph --cluster {} osd setcrushmap -i {}'.format(cluster, restore_crush_map))
            os.remove(crush_map)
            break

        # If we aren't super close, compute a less aggressive threshold and try again
        threshold = (max_variance + threshold) / 2
    else:
        # If we reached our target threshold within the allowed iterations, set a more aggressive threshold
        os.remove(crush_map)
        threshold = (max_variance + target_variance) / 2

# We have either achieved our target or have gotten as close as we can within the defined iteration limit
# Unset the flags and let the balancing begin
#os.remove('crushmap.original')
run_command('ceph --cluster {} osd unset norecover'.format(cluster))
run_command('ceph --cluster {} osd unset nobackfill'.format(cluster))
