# ceph-equalize-osd-utilization
# Copyright (C) 2018-2020 Stephen Taylor
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
num_reweight_osds_per_iteration = 1
max_reweight_attempts = 1000000
devnull = open(os.devnull, 'w')


# Run a shell command and return its output
def command_output(command):
    return subprocess.check_output(command, shell=True, stderr=devnull).decode('utf-8')


# run a shell command and return its return code
def run_command(command):
    try:
        subprocess.check_call(command, shell=True, stdout=devnull, stderr=devnull)
        return 0
    except subprocess.CalledProcessError as e:
        return e.returncode


# The OSD and pool data are expected to remain static during the reweighting process
print('Downloading osdmap from Ceph cluster and extracting crushmap')
run_command('ceph --cluster {} osd getmap -o osdmap'.format(cluster))
run_command('osdmaptool osdmap --export-crush crushmap.best'.format(cluster))
run_command('cp crushmap.best crushmap')

# Get a list of OSDs, a dictionary of OSD disk sizes, and a dictionary of per-OSD pool storage based on replication strategy
print('Getting the OSD list')
osds = sorted([int(x) for x in
               command_output("""crushtool -i crushmap --tree | grep osd | sort | uniq | awk '{print $1}'""").split(
                   '\n') if x != ''])

print('Getting disk sizes')
osd_df_columns = {label: index + 1 for index, label in
                  enumerate(command_output('ceph --cluster {} osd df | head -n1'.format(cluster)).lower().split())}
osd_disk_size = {int(fields[0]): float(fields[1]) for fields in [x.split() for x in command_output(
    """ceph --cluster {} osd df | head -n {} | tail -n {} | awk '{{print ${} " " ${}}}'""".format(cluster,
                                                                                                  len(osds) + 1,
                                                                                                  len(osds),
                                                                                                  osd_df_columns['id'],
                                                                                                  osd_df_columns[
                                                                                                      'size'])).split(
    '\n') if x != '']}

for id in osd_disk_size:
    osd_disk_size[id] = int(command_output('numfmt --from=iec {}T'.format(osd_disk_size[id])))

print('Getting pool information')
pool_dividend = {int(fields[0]): int(fields[2]) if fields[1] == 'erasure' else 1 for fields in [x.split() for x in
                                                                                                command_output(
                                                                                                    """osdmaptool osdmap --dump | grep pool | awk '{{print $2 " " $4 " " $10}}'""").split(
                                                                                                    '\n') if x != '']}

print('Getting current OSD crush weights')
crush_weight = {int(fields[0]): float(fields[1]) for fields in [x.split() for x in command_output(
    """crushtool -i crushmap --tree | grep osd | sort | uniq | awk '{{print $1 " " $3}}'""").split('\n') if x != '']}

print('Getting sizes for each PG in the cluster')
pg_size = {fields[0]: int(fields[1]) for fields in [x.split() for x in command_output(
    """ceph --cluster {} pg ls | head -n-2 | tail -n+2 | awk '{{print $1 " " $6}}'""".format(cluster)).split('\n') if
                                                    x != '']}


# Return a dictionary of computed osd variances based on pg sizes and mappings
def get_osd_variance():
    osd_size = {osd: 0 for osd in osds}
    pg_data = [x for x in command_output(
        """osdmaptool osdmap --test-map-pgs-dump | awk '/./ && /\[/ && /\]/{{print $1 " " $2}}'""").split('\n') if
               x != '']
    pgs = []
    pg_osds = {}

    # Use the pg dump data to build a list of pgs and dictionaries of pg sizes and osd mappings
    for pg_datum in pg_data:
        pg_fields = pg_datum.split()
        pgs.append(pg_fields[0])
        pg_osds[pg_fields[0]] = ast.literal_eval(pg_fields[1])

    # Use the pg sizes and mappings to compute used space for each osd
#    num_pgs = 0
    for pg in pgs:
        pool = int(pg.split('.')[0])
        for osd in pg_osds[pg]:
            osd_size[osd] += pg_size[pg] / pool_dividend[pool]
#            if osd == 5:
#                num_pgs += 1
#                print('{} {} {}'.format(num_pgs, pg, osd_size[osd]))

    # Compute the full percentage for each OSD, compute an average, then compute each OSD's variance and return a dictionary
    osd_percent_full = {osd: float(osd_size[osd]) / osd_disk_size[osd] for osd in osds}
    osd_average = float(sum(osd_percent_full.values())) / len(osd_size)
    osd_variance = {osd: float(osd_percent_full[osd]) / osd_average for osd in osds}
    return osd_variance


# Get current OSD variances and the initial maximum variance
osd_variance = get_osd_variance()
best_variance = max([abs(1 - variance) for variance in osd_variance.values()])
num_reweight_attempts = 0

while num_reweight_attempts < max_reweight_attempts:
    osd_variances = {osd: abs(1 - variance) for osd, variance in osd_variance.items()}
    i = 0
    while i < num_reweight_osds_per_iteration:
        osd = sorted(osd_variances, key=osd_variances.get, reverse=True)[0]
        osd_variances.pop(osd, None)
        variance = osd_variance[osd]
        increment = crush_weight[osd] * abs(1 - variance) / 100

        if osd_variance[osd] > 1.0:
            crush_weight[osd] -= increment
        else:
            crush_weight[osd] += increment

        if ((max_reweight_attempts - num_reweight_attempts - 1) % 10 == 0):
            print('osd.{} has a variance of {}, reweighting to {}, {} iterations until exit'.format(osd, variance,
                                                                                                    crush_weight[osd],
                                                                                                    max_reweight_attempts - num_reweight_attempts - 1))
        run_command(
            'crushtool -i crushmap --reweight-item osd.{} {} -o crushmap'.format(osd, crush_weight[osd]))
        run_command('osdmaptool osdmap --import-crush crushmap')

        i += 1

    osd_variance = get_osd_variance()
    max_variance = max([abs(1 - variance) for variance in osd_variance.values()])

    if max_variance < best_variance:
        print('Found a new map with max variance {}, resetting iteration counter'.format(max_variance))
        run_command('cp crushmap crushmap.best')
        best_variance = max_variance
        num_reweight_attempts = 0
    else:
        num_reweight_attempts += 1

# We have gotten as close as we can within the maximum number of unsuccessful attempts
# Set the optimal crush map and delete the map files from disk
print('Uploading best-case crushmap to the Ceph cluster')
run_command('ceph --cluster {} osd setcrushmap -i crushmap.best'.format(cluster))

print('Cleaning up temporary files')
os.remove('crushmap.best')
os.remove('crushmap')
os.remove('osdmap')
devnull.close()
print('Done')
