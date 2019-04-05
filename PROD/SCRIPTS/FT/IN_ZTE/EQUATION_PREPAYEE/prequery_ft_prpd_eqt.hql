SELECT IF(FT_NOT_EXISTS = 0 and FT_A_EXISTS>0 and SUBS_EXTRACT_NEXT_EXISTS>0  and PROD_EXTRACT_NEXT_EXISTS>0
    and  PRICE_PLAN_EXTRACT_NEXT_EXISTS>0 and BAL_SNAP_EXISTS=3 and BAL_SNAP_PREV_EXISTS=3 ,"OK","NOK") TABLES_EXIST
FROM
(SELECT COUNT(*) FT_NOT_EXISTS FROM MON.FT_PRPD_EQT WHERE EVENT_DAY='###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_A_EXISTS FROM MON.FT_A_EDR_PRPD_EQT WHERE EVENT_DAY='###SLICE_VALUE###') B,
(SELECT COUNT(*) SUBS_EXTRACT_NEXT_EXISTS FROM CDR.IT_ZTE_SUBS_EXTRACT WHERE ORIGINAL_FILE_DATE=DATE_SUB('###SLICE_VALUE###',-1)) D,
(SELECT COUNT(*) PROD_EXTRACT_NEXT_EXISTS FROM CDR.IT_ZTE_PROD_EXTRACT WHERE ORIGINAL_FILE_DATE=DATE_SUB('###SLICE_VALUE###',-1)) F,
(SELECT COUNT(*) PRICE_PLAN_EXTRACT_NEXT_EXISTS FROM CDR.IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE=DATE_SUB('###SLICE_VALUE###',-1)) H,
(SELECT COUNT(DISTINCT ORIGINAL_FILE_NAME) BAL_SNAP_EXISTS FROM CDR.IT_ZTE_BAL_SNAP WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') I,
(SELECT COUNT(DISTINCT ORIGINAL_FILE_NAME) BAL_SNAP_PREV_EXISTS FROM CDR.IT_ZTE_BAL_SNAP WHERE ORIGINAL_FILE_DATE=DATE_SUB('###SLICE_VALUE###',1)) J

;
