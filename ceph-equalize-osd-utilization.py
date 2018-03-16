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
import operator
import os
import subprocess
import time

# Change these to command-line arguments
cluster = 'ceph'
max_reweight_attempts = 1000

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

# Wait for peering
def wait_for_peering():
    time.sleep(5)
    while peering():
        time.sleep(1)

# The OSD and pool data are expected to remain static during the reweighting process
# Get a list of OSDs, a dictionary of OSD disk sizes, and a dictionary of per-OSD pool storage based on replication strategy
osds = sorted([int(x) for x in command_output('ceph --cluster {} osd ls'.format(cluster)).split('\n') if x != ''])
osd_df_columns = {label:index + 1 for index, label in enumerate(command_output('ceph --cluster {} osd df | head -n1'.format(cluster)).lower().split())}
osd_disk_size = {int(fields[0]): int(fields[1]) * 1073741824 for fields in [x.split() for x in command_output("""ceph --cluster {} osd df | head -n {} | tail -n {} | awk '{{print ${} " " ${}}}' | cut -d'G' -f1""".format(cluster, len(osds) + 1, len(osds), osd_df_columns['id'], osd_df_columns['size'])).split('\n') if x != '']}
pool_dividend = {int(fields[0]): int(fields[2]) if fields[1] == 'erasure' else 1 for fields in [x.split() for x in command_output("""ceph --cluster {} osd dump | grep pool | awk '{{print $2 " " $4 " " $8}}'""".format(cluster)).split('\n') if x != '']}
crush_weight = {int(fields[0]): float(fields[1]) for fields in [x.split() for x in command_output("""ceph --cluster {} osd df | head -n {} | tail -n {} | awk '{{print ${} " " ${}}}'""".format(cluster, len(osds) + 1, len(osds), osd_df_columns['id'], osd_df_columns['weight'])).split('\n') if x != '']}
pg_dump_columns = {label:index + 1 for index, label in enumerate(command_output('ceph --cluster {} pg dump pgs 2> /dev/null | head -n1'.format(cluster)).lower().replace('_stamp', ' stamp').split())}

# Return a dictionary of computed osd variances based on pg sizes and mappings
def get_osd_variance():
    osd_size = {osd: 0 for osd in osds}
    pg_data = [x for x in command_output("""ceph --cluster {} pg dump pgs 2> /dev/null | grep -iv "PG_STAT" | awk '{{print ${} " " ${} " " ${}}}'""".format(cluster, pg_dump_columns['pg_stat'], pg_dump_columns['bytes'], pg_dump_columns['up'])).split('\n') if x != '']
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

# Set nobackfill and norecover flags on the cluster to prevent data movement while reweighting is in progress
run_command('ceph --cluster {} osd set nobackfill'.format(cluster))
run_command('ceph --cluster {} osd set norecover'.format(cluster))
run_command('ceph --cluster {} osd getcrushmap -o crushmap.best 2> /dev/null'.format(cluster))

# Get current OSD variances and the initial maximum variance
osd_variance = get_osd_variance()
best_variance = max([abs(1 - variance) for variance in osd_variance.values()])
num_reweight_attempts = 0

while num_reweight_attempts < max_reweight_attempts:
    osd = max({osd:abs(1 - variance) for osd, variance in osd_variance.items()}.iteritems(), key=operator.itemgetter(1))[0]
    variance = osd_variance[osd]
    increment = crush_weight[osd] * abs(1 - variance) / 100

    if osd_variance[osd] > 1.0:
        crush_weight[osd] -= increment
    else:
        crush_weight[osd] += increment

    print('osd.{} has a variance of {}, reweighting to {}'.format(osd, variance, crush_weight[osd]))
    run_command('ceph --cluster {} osd crush reweight osd.{} {} 2> /dev/null'.format(cluster, osd, crush_weight[osd]))
    wait_for_peering()
    osd_variance = get_osd_variance()
    max_variance = max([abs(1 - variance) for variance in osd_variance.values()])

    if max_variance < best_variance:
        run_command('ceph --cluster {} osd getcrushmap -o crushmap.best 2> /dev/null'.format(cluster))
        best_variance = max_variance
        num_reweight_attempts = 0
    else:
        num_reweight_attempts += 1

# We have gotten as close as we can within the maximum number of unsuccessful attempts
# Set the optimal crush map, delete the crush map from disk, unset the flags, and let the backfilling begin
run_command('ceph --cluster {} osd setcrushmap -i crushmap.best 2> /dev/null'.format(cluster))
os.remove('crushmap.best')
run_command('ceph --cluster {} osd unset norecover'.format(cluster))
run_command('ceph --cluster {} osd unset nobackfill'.format(cluster))
