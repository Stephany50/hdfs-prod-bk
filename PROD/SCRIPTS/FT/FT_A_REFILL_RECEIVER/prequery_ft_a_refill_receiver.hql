SELECT IF(A.FT_A_REFILL_RECEIVER_hold = 0 and B.FT_REFILL>0,"OK","NOK") FROM
(SELECT count(*) FT_A_REFILL_RECEIVER_hold FROM MON.FT_A_REFILL_RECEIVER WHERE REFILL_MONTH= DATE_FORMAT('###SLICE_VALUE###','yyyy-MM') ) A,
(SELECT COUNT(*) FT_REFILL FROM MON.FT_REFILL WHERE REFILL_MONTH=DATE_FORMAT('###SLICE_VALUE###','yyyy-MM')) B
;
