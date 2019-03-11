set pig.maxCombinedSplitSize 100000000000;
set output.compression.enabled false;
set output.compression.codec org.apache.hadoop.io.compress.GzipCodec;
set default_parallel 1;
input0 = LOAD '$IN_DIR' USING PigStorage();
STORE input0 INTO '$OUT_DIR' USING PigStorage();
 
-- use this command pig -D mapred.output.compress=false -f hdfs://ocmhdp/PROD/SCRIPTS/COMPRESSION/decompress.pig -p IN_DIR=<INPUT_PATH> -p OUT_DIR=<OUTPUT_PATH>
