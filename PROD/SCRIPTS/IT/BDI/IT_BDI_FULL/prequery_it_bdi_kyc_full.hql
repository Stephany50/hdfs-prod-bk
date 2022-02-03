SELECT IF(A.nbr = 0  and B.nbr >= 12000000 and C.nbr > 0 and D.nbr >= 12000000 and E.nbr > 0 and F.nbr > 0 and G.nbr > 0 AND H.hlr_nb_file_is_ok=6,'OK','NOK') FROM
(SELECT COUNT(*) nbr FROM CDR.SPARK_IT_KYC_BDI_FULL WHERE original_file_date='###SLICE_VALUE###') A,
(SELECT COUNT(*) nbr FROM CDR.SPARK_IT_KYC_BDI_FULL WHERE original_file_date=DATE_SUB('###SLICE_VALUE###',1)) B,
(SELECT COUNT(*) nbr FROM CDR.SPARK_IT_KYC_CRM_B2C where original_file_date='###SLICE_VALUE###') C,
(SELECT COUNT(*) nbr FROM CDR.SPARK_IT_KYC_ZSMART a where original_file_date = '###SLICE_VALUE###') D,
(SELECT COUNT(*) nbr FROM CDR.SPARK_IT_BDI_HLR where original_file_date='###SLICE_VALUE###') E,
(SELECT COUNT(*) nbr FROM MON.SPARK_FT_MSISDN_IMEI_LOCALISATION_TO_BDI WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###',1)) F,
(SELECT COUNT(*) nbr FROM MON.SPARK_FT_KYC_CRM_B2B where event_date=DATE_SUB('###SLICE_VALUE###',1)) G,
(SELECT count(distinct  original_file_name) hlr_nb_file_is_ok FROM CDR.SPARK_IT_BDI_HLR WHERE original_file_date='###SLICE_VALUE###') H