# Enzo Storage Tools

These tools are useful for packaging and storing Enzo snapshots.  If youre like me,
sometimes you download/generate enzo snapshots that sometimes get partially purged
by cluster rules of inactivity.  These tools were created for verifying that your
snapshots are all present, for tarring/compressing them in parallel, and then for
verifying that the tar files are valid, prior to transferring them to long-term storage.
ALWAYS RUN THESE ON COMPUTE NODES, at the very least the two MPI enabled ones.

 * verify_snapshots.py -- Assure that all consecutive snapshots (and enzo files) are present

 * tar_snapshots.py -- Tar and gzip all consecutive snapshots (MPI)

 * verify_tar.py -- Verify that the tar files are all present and valid gzip files (MPI)

## Usage

```
cd enzo_run_directory
python verify_snapshots.py
aprun -n 32 python tar_snapshots.py
aprun -n 32 python verify_tar.py
```

Each script produces a log file of the form: <script_name.log>.  I encourage you
to examine each logfile at each step before moving to the next step.

Alternate modes exist for explicitly specifying which directories/files to verify, 
but default operations are applied to DD* and RD* directories found.  MPI runs
tar and gzip operations in parallel to provide useful speedups.  Have tested to 32 
cores on BlueWaters without problem and with the blessing of the helpdesk.

If you're feeling lucky, you can skip the verify_tar.py step, or if you want 
to be really sure your tar files are correct, you could try untarring one
of your tar files in a separate directory and testing to assure its contents
are bitwise identical to the original.  But this is probably unnecessary.

```
mkdir temp
tar -zxvf some_file.tar -C temp
diff -arq some_file temp/some_file
```
