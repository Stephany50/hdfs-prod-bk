SELECT IF(A.COUNT_FT = 0 and B.COUNT_IT>0,"OK","NOK") FROM
(SELECT count(*) COUNT_FT FROM MON.FT_SMSC_TRANSACTION_A2P WHERE TRANSACTION_BILLING_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) COUNT_IT FROM CDR.IT_SMSC_MVAS_A2P WHERE WRITE_DATE='###SLICE_VALUE###') B;
