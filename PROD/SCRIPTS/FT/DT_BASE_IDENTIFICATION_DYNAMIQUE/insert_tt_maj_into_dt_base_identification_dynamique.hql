TRUNCATE TABLE DIM.SPARK_DT_BASE_IDENTIFICATION_DYNAMIQUE;
INSERT INTO DIM.SPARK_DT_BASE_IDENTIFICATION_DYNAMIQUE
SELECT *
FROM TT.SPARK_TT_MAJ_DT_BASE_IDENTIFICATION_DYNAMIQUE;