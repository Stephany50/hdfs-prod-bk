SELECT IF (A.FT_EXITS = 0 AND B.BILLED_TRANSACTION_POSTPAID_EXITS >datediff(last_day(concat('###SLICE_VALUE###','-01')), concat('###SLICE_VALUE###','-01')) AND C.CRA_GPRS_POST_EXITS>datediff(last_day(concat('###SLICE_VALUE###','-01')), concat('###SLICE_VALUE###','-01')),"OK","NOK") FROM
(SELECT COUNT(*) FT_EXITS FROM MON.FT_MSISDN_POST_MONTHLY WHERE MOIS = '###SLICE_VALUE###')A,
(SELECT COUNT(DISTINCT TRANSACTION_DATE) BILLED_TRANSACTION_POSTPAID_EXITS FROM MON.FT_BILLED_TRANSACTION_POSTPAID WHERE TRANSACTION_DATE BETWEEN concat('###SLICE_VALUE###','-01') and last_day(concat('###SLICE_VALUE###','-01')))B,
(SELECT COUNT(DISTINCT SESSION_DATE) CRA_GPRS_POST_EXITS FROM MON.FT_CRA_GPRS_POST WHERE SESSION_DATE BETWEEN concat('###SLICE_VALUE###','-01') and last_day(concat('###SLICE_VALUE###','-01')))C




