SELECT IF(T_1.SPOOL_COUNT = 0 AND T_2.FT_COUNTA > 0 AND T_3.FT_COUNTB  > 0 AND T_4.FT_COUNTC  > 0 AND T_5.FT_COUNTD  > 0 AND T_6.FT_COUNTE  > 0 AND T_7.FT_COUNTF  > 0 AND T_8.FT_COUNTG > 0 AND T_9.FT_COUNTI  > 0 AND T_10.FT_COUNTJ  > 0 ,"OK","NOK") FT_EXIST
FROM
    (SELECT COUNT(*) SPOOL_COUNT FROM SPOOL.SPOOL_OM_BICEC_TRANS_RECONCILIATION WHERE EVENT_DATE ='###SLICE_VALUE###') T_1,
    (SELECT COUNT(*) FT_COUNTA FROM CDR.SPARK_IT_OMNY_TRANSACTIONS WHERE TRANSFER_DATETIME ='###SLICE_VALUE###') T_2,
    (SELECT COUNT(*) FT_COUNTB FROM CDR.SPARK_IT_OM_ALL_BALANCE WHERE ORIGINAL_FILE_DATE ='###SLICE_VALUE###') T_3,
    (SELECT COUNT(*) FT_COUNTC FROM CDR.SPARK_IT_OM_ALL_BALANCE WHERE ORIGINAL_FILE_DATE = DATE_SUB('###SLICE_VALUE###', 1)) T_4,
    (SELECT COUNT(*) FT_COUNTD FROM CDR.SPARK_IT_OM_ALL_BALANCE WHERE ORIGINAL_FILE_DATE = DATE_SUB('###SLICE_VALUE###', 2)) T_5,
    (SELECT COUNT(*) FT_COUNTE FROM CDR.SPARK_IT_OM_ALL_BALANCE WHERE ORIGINAL_FILE_DATE = DATE_SUB('###SLICE_VALUE###', 3)) T_6,
    (SELECT COUNT(*) FT_COUNTF FROM CDR.SPARK_IT_OM_ALL_BALANCE WHERE ORIGINAL_FILE_DATE = DATE_SUB('###SLICE_VALUE###', 4)) T_7,
    (SELECT COUNT(*) FT_COUNTG FROM CDR.SPARK_IT_OM_ALL_BALANCE WHERE ORIGINAL_FILE_DATE = DATE_SUB('###SLICE_VALUE###', 5)) T_8,
    (SELECT COUNT(*) FT_COUNTI FROM CDR.SPARK_IT_OM_ALL_BALANCE WHERE ORIGINAL_FILE_DATE = DATE_SUB('###SLICE_VALUE###', 6)) T_9,
    (SELECT COUNT(*) FT_COUNTJ FROM CDR.SPARK_IT_OM_ALL_BALANCE WHERE ORIGINAL_FILE_DATE = DATE_SUB('###SLICE_VALUE###', 7)) T_10




