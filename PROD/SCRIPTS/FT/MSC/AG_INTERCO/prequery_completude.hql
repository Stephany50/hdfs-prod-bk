SELECT IF(A.FT_EXIST = 0  AND C.FT_MSC_EXISTS >0,"OK","NOK") FROM
(SELECT count(*) FT_EXIST FROM AGG.FT_AG_INTERCO WHERE SDATE='###SLICE_VALUE###') A,
--(SELECT COUNT(*) FT_PREVIEW_DAY_EXIST FROM AGG.FT_AG_INTERCO WHERE SDATE=DATE_SUB('###SLICE_VALUE###',1))B,
(SELECT COUNT(*) FT_MSC_EXISTS FROM MON.FT_MSC_TRANSACTION WHERE TRANSACTION_DATE='###SLICE_VALUE###')C

