SELECT IF(A.FT_A_REFILL_REVENUE = 0 and B.FT_REFILL>0,"OK","NOK") FROM
(SELECT count(*) FT_A_REFILL_REVENUE FROM AGG.SPARK_FT_A_REFILL_REVENUE WHERE REFILL_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_REFILL FROM MON.SPARK_FT_REFILL WHERE REFILL_DATE='###SLICE_VALUE###') B
;
