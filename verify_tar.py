"""
Check to assure all Tempest tar files are present and valid gzip files; uses MPI
(bw: `module load bwpy bwpy-mpi`)

Usage: aprun -n 20 python verify_tar.py 
"""
import glob
import sys
import os
from mpi4py import MPI
import subprocess
from verify_snapshots import \
    create_log, \
    log, \
    grab_all, \
    grab_from_file, \
    check_if_all_present
    

def split(container, count):
    """
    Simple function splitting a container into equal length chunks.
    Order is not preserved but this is potentially an advantage depending on
    the use case.
    """
    return [container[_i::count] for _i in range(count)]

def verify_tar(tar_file):
    """
    Runs `gzip -t` to test each tar file to verify it is valid.
    """
    command = "gzip -t %s" % tar_file
    proc = subprocess.Popen(command.split(), shell=False,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout, stderr) = proc.communicate()
    if stderr: 
        return stderr
    else:
        return

if __name__ == '__main__':
    
    COMM = MPI.COMM_WORLD
    standard_tar_list = True

    if COMM.rank == 0:

        errors = 0
        # if no command line arguments, use all DD and RD tar files
        if len(sys.argv) == 1:
            tar_list = grab_all(suffix='.tar.gz')
            tar_list_RD = grab_all(basename='RD', suffix='.tar.gz')
        # if command line argument get files from file list
        elif len(sys.argv) == 2:
            standard_tar_list = False
            tar_list = grab_from_file(sys.argv[1])
        else:
            print("Usage: aprun python verify_tar.py <file_list.txt>")
            print("Usage: aprun python verify_tar.py")
            sys.exit()

        f = create_log('verify_tar.log')
        if len(tar_list) > 0:
            mesg = "Verifying tar files from %s to %s." % (tar_list[0], tar_list[-1])
            log(f, mesg, stdout=True)
            errors += check_if_all_present(tar_list, f, file_type='tar files', suffix='.tar.gz')
        else:
            log(f, 'No DD tar files present.', stdout=True)

        # Do RD tar files too?
        if standard_tar_list:
            if len(tar_list_RD) > 0:
                mesg = "Verifying tar files from %s to %s." % (tar_list_RD[0], tar_list_RD[-1])
                log(f, mesg, stdout=True, whitespace=True)
                errors += check_if_all_present(tar_list_RD, f, file_type='tar files', suffix='.tar.gz')
                tar_list += tar_list_RD
            else:
                log(f, 'No RD tar files present.', stdout=True)
        f.flush()

        # Prepare tar_list for use with COMM.scatter
        tar_list = split(tar_list, COMM.size)
            
    else:
        # Make tar_list None on all other cores
        tar_list = None

    # Scatter jobs across cores
    tar_list = COMM.scatter(tar_list, root=0)

    # Actually verify the tar files on each core, storing stderr to results
    results = []
    for tar in tar_list:
        results.append(verify_tar(tar))

    # gather results on rank 0
    results = MPI.COMM_WORLD.gather(results, root=0)

    if COMM.rank == 0:
        # Flatten list of lists.
        results = [_i for temp in results for _i in temp]
        if any(results):
            results = list(filter(None, results))
            results.sort()
            [log(f, r, stdout=True) for r in results]
            print("*** SOME TAR FILES INCOMPLETE. ***")
        else:
            log(f, 'All tar files verified as valid gzip files.', stdout=True)
        f.close()
