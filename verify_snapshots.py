"""
-----------------------------------------------------------------------------
 Copyright (c) Cameron Hummels <chummels@gmail.com>.  All rights reserved.

 Distributed under the terms of the Modified BSD License.

 The full license is in the file LICENSE, distributed with this software.
-----------------------------------------------------------------------------

Check to assure all enzo directories are consecutive and actually have their 
HDF5 files and enzo files present as non-zero-sized files.  Can operate 
by finding all relevant DD* and RD* directories, or altneratively provide a 
text file as an argument, with each line of the text file as a directory to 
check.  This code assumes that your files are of the format where directory 
and filename are the same (e.g., DD0000/DD0000).
Looks for required files: 

DD0000
DD0000.boundary
DD0000.boundary.hdf
DD0000.configure
DD0000.hierarchy
DD0000.memorymap

Outputs some relevant text to STDOUT and all to verify_snapshots.log.

Usage:

python verify_snapshots.py 
python verify_snapshots.py dir_list.txt
"""
import glob
import sys
import os

def create_log(fn):
    """
    Opens a log file handle for the provided filename; if it already exists, append to it, otherwise write to it.
    """
    if os.path.isfile(fn):
        mode = 'a'
    else:
        mode = 'w'
    f = open(fn, mode)
    return f


def log(f, message, stdout=False, whitespace=False):
    """
    Logs relevant information to STDOUT and to a log file
    """
    if whitespace:
        if stdout:
            print("")
        f.write("\n")
    if stdout:
        print(message)
    f.write("%s\n" % message)


def grab_all(basename='DD', directory='.', suffix=''):
    """
    Use glob to grab a list of all the enzo snapshot output directories in a provided directory.
    e.g. DD???? or RD????
    """
    dir_list = glob.glob(os.path.join(directory, basename+"????"+suffix))
    dir_list.sort()
    dir_list = [d.lstrip('./') for d in dir_list]
    return dir_list


def grab_from_file(fn):
    """
    Opens a provided text file where each line is an enzo snapshot directory and reads these 
    directories into a list
    """
    with open(fn, 'r') as dirs:
        dir_list = dirs.read().splitlines()
        dir_list.sort()
        return dir_list


def check_if_all_present(dir_list, log_handle, file_type='directories', suffix=''):
    """
    Assure we have directories continuously covering DD series (e.g. DD0000 - DD0100)
    """
    errors = 0
    first = dir_list[0].rstrip(suffix)
    last = dir_list[-1].rstrip(suffix)
    first_num = int(first[-4:])
    last_num = int(last[-4:])

    n_dirs = last_num - first_num + 1
    if n_dirs != len(dir_list):
        log(log_handle, "*** MISSING %s. EXPECTED %d BUT FOUND %d ***" % (file_type.upper(), n_dirs, len(dir_list)), stdout=True)
        for i in range(first_num, last_num):
            fn = "%s%04d%s" % (first[:-4], i, suffix)
            if fn not in dir_list:
                log(log_handle, '*** MISSING: %s ***' % fn, stdout=True)
                errors += 1
    else:
        log(log_handle, "%d %s from %s to %s. OK"% (len(dir_list), file_type, dir_list[0], dir_list[-1]))
    return errors


def check_if_all_enzo_files_present(dir_list, log_handle):
    """
    Assures that all .cpu files are present in consecutive order for a snapshot directory.
    Additionally checks to assure that X.boundary, X.boundary.hdf, X.configure, X.hierarchy, and X.memorymap
    are present.
    """
    errors = 0
    # Step into each snapshot directory and count cpu files
    for dir in dir_list:
        fn_list = glob.glob(os.path.join(dir, dir+'.cpu????'))
        fn_list.sort()
        if len(fn_list) > 0:
            last = int(fn_list[-1][-4:])
            if len(fn_list) != last+1:
                log(log_handle, "*** MISSING CPU FILES IN %s. EXPECTED %d BUT FOUND %d ***" % (dir, last+1, len(fn_list) ), stdout=True)

                for i in range(last):
                    cpu_fn = os.path.join(dir, dir+'.cpu%04d' % i)
                    if cpu_fn not in fn_list:
                        log(log_handle, "*** MISSING: %s ***" % cpu_fn, stdout=True)
                        errors += 1
            else:
                log(log_handle, "%d cpu files in %s. OK"% (len(fn_list), dir))
            for file in fn_list:
                if os.stat(file).st_size == 0: 
                    log(log_handle, "*** %s FILE IS ZERO-SIZED ***" % file, stdout=True)
                    errors += 1
        else:
            log(log_handle, "*** NO CPU FILES IN %s ***" % dir, stdout=True)

        fn_list = glob.glob(os.path.join(dir, dir+'*'))
        # Assure enzo required files are present
        req_files = [dir+r for r in required]
        for file in req_files:
            if os.path.join(dir,file) not in fn_list: 
                log(log_handle, '*** %s MISSING ***' % file, stdout=True)
                errors += 1
            elif os.stat(os.path.join(dir,file)).st_size == 0: 
                log(log_handle, "*** %s FILE IS ZERO-SIZED ***" % file, stdout=True)
                errors += 1
    return errors


if __name__ == '__main__':

    required = ['', ".boundary", ".boundary.hdf", ".configure", ".hierarchy", ".memorymap"]
    errors = 0
    standard_dir_list = True
    # if no command line arguments, use all DD and RD dirs
    if len(sys.argv) == 1:
        dir_list = grab_all(basename='DD')
        dir_list_RD = grab_all(basename='RD')
    # if command line argument get files from file list
    elif len(sys.argv) == 2:
        standard_dir_list = False
        dir_list = grab_from_file(sys.argv[1])
    else:
        print("Usage: python verify_snapshots.py <dir_list.txt>")
        print("Usage: python verify_snapshots.py")
        sys.exit()

    f = create_log('verify_snapshots.log')
    
    if len(dir_list) > 0:
        mesg = "Checking directories from %s to %s." % (dir_list[0], dir_list[-1])
        log(f, mesg, stdout=True)
        errors += check_if_all_present(dir_list, f, file_type='directories')
        errors += check_if_all_enzo_files_present(dir_list, f)
    else:
        log(f, "No DD files present.", stdout=True)
    f.flush()
        

    if standard_dir_list:
        dir_list = dir_list_RD
        if len(dir_list) > 0:
            mesg = "Checking directories from %s to %s." % (dir_list[0], dir_list[-1])
            log(f, mesg, stdout=True, whitespace=True)
            errors += check_if_all_present(dir_list, f, file_type='directories')
            errors += check_if_all_enzo_files_present(dir_list, f)
        else:
            log(f, "No RD files present.", stdout=True)

    if errors == 0:
        log(f, "All expected files present. Ready for tar and storage.", stdout=True)
    else:
        log(f, "%d errors detected.  Please see verify_snapshots.log file for more info." % errors, stdout=True)
    f.close()
