# HDFS-PROD-BK

Backup of /PROD directory on HDFS

All Scripts and config file for verious flux(s)

- /PROD/SCRIPTS
    All the scripts for
     -Create TT tables
     -Create IT tables
     -Create FT tables
     -Insert in to IT Tables
     -Compute and insert into FT tables

- /PROD/COMPRESSION
    -Compress a directory using PIG
    -Decompress a directory using PIG

-  /PROD/CONF
    -system-cron-task.conf is the scheduler or orchestrator of all tasks to launched on the cluster with specified timeout(precised execution time limit).

    -load-it* : load data in to IT tables. This operation actually moves data from /PROD/RAW/FLUXNAME to /PROD/TT/FLUXNAME and achives the that flux to /PROD/ARCH/FLUXNAME
    
    - load-ft: compute and insert data into respective FT tables. Data completion check is done for before the computation.

    NB: Load scripts are launched via the CRONMANAGER