SELECT IF(F1.FT_EXISTS = 0 AND
         F2.FT_EXISTS > 0 AND
         F3.FT_EXISTS > 0 AND
         F4.CDR_NBR > DATEDIFF(LAST_DAY(TO_DATE('###SLICE_VALUE###' || '-01')), TO_DATE('###SLICE_VALUE###' || '-01'))
          ,'OK','NOK')
FROM 
(SELECT COUNT(*) AS FT_EXISTS from MON.SPARK_FT_CRM_OUTPUT_B2B where EVENT_MONTH = '###SLICE_VALUE###') F1,
(SELECT COUNT(*) AS FT_EXISTS from MON.SPARK_FT_MARKETING_DATAMART_MONTH where EVENT_MONTH = '###SLICE_VALUE###') F2,
(SELECT COUNT(*) AS FT_EXISTS FROM CDR.SPARK_IT_ZEBRA_MASTER WHERE TRANSACTION_DATE = LAST_DAY(TO_DATE('###SLICE_VALUE###' || '-01'))) F3,
(SELECT COUNT(DISTINCT ORIGINAL_FILE_DATE) AS CDR_NBR FROM CDR.IT_CRM_ABONNEMENT_HIERARCH WHERE ORIGINAL_FILE_DATE BETWEEN TO_DATE('###SLICE_VALUE###' || '-01') AND LAST_DAY(TO_DATE('###SLICE_VALUE###' || '-01'))) F4,