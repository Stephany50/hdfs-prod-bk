SELECT IF(A.AG_IRIS_REVENU_SPLIT = 0 and B.CDR_ZTE_ADJUST > 0,"OK","NOK") FROM
 (SELECT COUNT(*) AG_IRIS_REVENU_SPLIT FROM AGG.SPARK_FT_A_IRIS_REVENU_SPLIT WHERE EVENT_DATE= '###SLICE_VALUE###') A,
 (SELECT COUNT(DISTINCT file_date) CDR_ZTE_ADJUST FROM cdr.spark_it_zte_adjustment WHERE file_date ='###SLICE_VALUE###') B

