set pig.maxCombinedSplitSize 100000000000;
set output.compression.enabled true;
set output.compression.codec org.apache.hadoop.io.compress.GzipCodec;
set default_parallel 1;
input0 = LOAD '$IN_DIR' USING PigStorage();
STORE input0 INTO '$OUT_DIR' USING PigStorage();
 
-- use this command pig -f hdfs://ocmhdp/PROD/SCRIPTS/COMPRESSION/compress.pig -p IN_DIR=<INPUT_PATH> -p OUT_DIR=<OUTPUT_PATH>
