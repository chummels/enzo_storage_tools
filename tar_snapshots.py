"""
-----------------------------------------------------------------------------
 Copyright (c) Cameron Hummels <chummels@gmail.com>.  All rights reserved.

 Distributed under the terms of the Modified BSD License.

 The full license is in the file LICENSE, distributed with this software.
-----------------------------------------------------------------------------

Use MPI to tar all enzo snapshots into tarfiles for storage.
(bw: `module load bwpy bwpy-mpi`); Have tested up to 20 cores with
no problems.  BW helpdesk suggests we can go higher.
Produces tar_snapshots.log with log info.

Usage: aprun -n 32 python tar_snapshots.py
Usage: aprun -n 32 python tar_snapshots.py file_list.txt
"""

import glob
import os
import sys
import tarfile
from contextlib import closing
from mpi4py import MPI
from verify_tar import \
    split
from verify_snapshots import \
    create_log, \
    log, \
    grab_all, \
    grab_from_file

def make_tarfile(source_dir):
    """
    Generated a gzipped tarfile from the provided source directory
    """
    output_filename = "%s.tar.gz" % source_dir
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))
    OK = "Created %s OK\n" % output_filename
    print(OK)
    return OK

if __name__ == '__main__':

    COMM = MPI.COMM_WORLD
    standard_dir_list = True

    if COMM.rank == 0:
        
        errors = 0
        # if no command line arguments, use all DD and RD directories
        if len(sys.argv) == 1:
            dir_list = grab_all()
            dir_list_RD = grab_all(basename='RD')
        # if command line argument get files from file list
        elif len(sys.argv) == 2:
            standard_dir_list = False
            dir_list = grab_from_file(sys.argv[1])
        else:
            print("Usage: aprun -n 20 python tar_snapshots.py <dir_list.txt>")
            print("Usage: aprun -n 20 python tar_snapshots.py")
            sys.exit()

        f = create_log('tar_snapshots.log')

        # Do RD directories too?
        if standard_dir_list:
            if len(dir_list_RD) > 0:
                dir_list += dir_list_RD

        # Prepare tar_list for use with COMM.scatter
        dir_list = split(dir_list, COMM.size)

    else:
        # Make dir_list None on all other cores
        dir_list = None

    # Scatter jobs across cores.
    dir_list = COMM.scatter(dir_list, root=0)

    # Now each rank just does its jobs and collects everything in a results list.
    # Make sure to not use super big objects in there as they will be pickled to be
    # exchanged over MPI.
    results = []
    for dir in dir_list:
        # Do something meaningful here...
        results.append(make_tarfile(dir))

    # Gather results on rank 0.
    results = MPI.COMM_WORLD.gather(results, root=0)
    
    if COMM.rank == 0:
        # Flatten list of lists.
        results = [_i for temp in results for _i in temp]
        if any(results):
            results = list(filter(None, results))
            results.sort()
            [log(f, r) for r in results]
            print("Summary of tar files in tar_snapshots.log")
        else:
            log(f, "Tar didn't operate properly.", stdout=True)
        f.close()
