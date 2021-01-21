SELECT IF(
    A.nb_ft_bdi > 0 and B.nb_it_zsmart > 0 and C.nb_photo > 0 and D.nb_ft_cllsd > 0
    and E.nb_ft_susp = 0 and F.nb_ft_susp_prev > 0
    , "OK"
    , "NOK"
)
FROM
(SELECT COUNT(*) nb_ft_bdi FROM MON.SPARK_FT_BDI_1A WHERE event_date  = '###SLICE_VALUE###') A,
(SELECT COUNT(*) nb_it_zsmart FROM cdr.spark_it_bdi_zsmart WHERE original_file_date = date_add('###SLICE_VALUE###',1)) B,
(SELECT COUNT(*) nb_photo FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE event_date = date_add('###SLICE_VALUE###',1)) C,
(SELECT COUNT(*) nb_ft_cllsd FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE event_date = '###SLICE_VALUE###') D,
(SELECT COUNT(*) nb_ft_susp FROM MON.SPARK_FT_SUSPENSIONS_TELCO WHERE event_date = '###SLICE_VALUE###') E,
(SELECT COUNT(*) nb_ft_susp_prev FROM MON.SPARK_FT_SUSPENSIONS_TELCO WHERE event_date = date_sub('###SLICE_VALUE###',1)) F