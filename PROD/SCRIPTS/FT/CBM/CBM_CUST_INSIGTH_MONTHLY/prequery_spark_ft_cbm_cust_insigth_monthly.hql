SELECT IF(
    A.EXIST_INSIGTH_MONTHLY = 0 AND 
    B.EXIST_INSIGTH_DAILY > DATEDIFF(LAST_DAY(CONCAT('###SLICE_VALUE###','-01')), CONCAT('###SLICE_VALUE###','-01')) ,"OK","NOK") 
FROM
 (SELECT COUNT(*) EXIST_INSIGTH_MONTHLY FROM MON.SPARK_FT_CBM_CUST_INSIGTH_MONTHLY WHERE PERIOD = TO_DATE('###SLICE_VALUE###')) A,
 (SELECT COUNT(DISTINCT PERIOD) EXIST_INSIGTH_DAILY FROM MON.SPARK_FT_CBM_CUST_INSIGTH_DAILY 
  WHERE PERIOD BETWEEN TO_DATE(CONCAT('###SLICE_VALUE###','-01')) AND LAST_DAY(CONCAT('###SLICE_VALUE###','-01'))) B
