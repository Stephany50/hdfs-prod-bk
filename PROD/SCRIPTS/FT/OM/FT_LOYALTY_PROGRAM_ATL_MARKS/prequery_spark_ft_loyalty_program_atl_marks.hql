SELECT IF(T_1.FT_EXIST = 0 AND T_2.IT_OMNY_TRANSACTIONS_EXIST>0 AND T_3.IT_OM_SUBSCRIBERS>0 AND T_4.FT_EDR_PRPD_EQT>0, "OK", "NOK")
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_LOYALTY_PROGRAM_ATL_MARKS WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) IT_OMNY_TRANSACTIONS_EXIST FROM CDR.SPARK_IT_OMNY_TRANSACTIONS WHERE TRANSFER_DATETIME='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) IT_OM_SUBSCRIBERS FROM CDR.SPARK_IT_OM_SUBSCRIBERS WHERE MODIFICATION_DATE='###SLICE_VALUE###') T_3,
(SELECT COUNT(*) FT_EDR_PRPD_EQT FROM MON.SPARK_FT_EDR_PRPD_EQT WHERE EVENT_DATE='###SLICE_VALUE###') T_4
