SELECT IF(FT_A_EXISTS=0 AND FT_EXISTS>0 ,'OK','NOK' )
FROM
(SELECT COUNT(*) FT_A_EXISTS FROM AGG.FT_A_CREDIT_TRANSFER_REVENUE WHERE REFILL_DATE='###SLICE_VALUE###')T1,
(SELECT COUNT(*) FT_EXISTS FROM MON.FT_CREDIT_TRANSFERT WHERE REFILL_DATE='###SLICE_VALUE###')T2