INSERT INTO MON.BACKUP_SPARK_DT_BASE_IDENTIFICATION PARTITION(PROCESSING_DATE)
SELECT a.*,'###SLICE_VALUE###' FROM DIM.SPARK_DT_BASE_IDENTIFICATION a