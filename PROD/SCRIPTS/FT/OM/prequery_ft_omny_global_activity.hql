SELECT IF(T_1.FT_EXIST = 0 and T_2.FT_PREV_EXIST>0 and T_3.IT_OM_TRANSACTIONS_EXIST>0,"OK","NOK") EXTRACT_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.FT_OMNY_GLOBAL_ACTIVITY WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_PREV_EXIST FROM MON.FT_OMNY_GLOBAL_ACTIVITY WHERE EVENT_DATE=DATE_SUB('###SLICE_VALUE###',1)) T_2,
(SELECT COUNT(*) IT_OM_TRANSACTIONS_EXIST FROM CDR.IT_OM_TRANSACTIONS WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_3
