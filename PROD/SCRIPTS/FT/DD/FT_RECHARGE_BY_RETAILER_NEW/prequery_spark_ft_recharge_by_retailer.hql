SELECT IF(
    A.FT_EXIST = 0 and 
    E.FT_TMP_EXIST = 0 and 
    B.nbr_om > 0 and 
    C.nbr_refill > 0 and 
    D.nbr_retail > 0 and
    F.nb_last_site_day > 0
    ,"OK", "NOK") FROM
(SELECT COUNT(*) FT_EXIST FROM DD.SPARK_FT_RECHARGE_BY_RETAILER WHERE REFILL_DATE = '###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_TMP_EXIST FROM DD.TMP_SPARK_FT_RECHARGE_BY_RETAILER WHERE REFILL_DATE = '###SLICE_VALUE###') E,
(SELECT COUNT(*) nbr_om FROM CDR.SPARK_IT_OMNY_TRANSACTIONS WHERE FILE_DATE ='###SLICE_VALUE###') B,
(SELECT COUNT(*) nbr_refill FROM MON.SPARK_FT_REFILL WHERE FILE_DATE = DATE_SUB('###SLICE_VALUE###',1)) C,
(SELECT COUNT(*) nbr_retail FROM MON.SPARK_FT_RETAIL_BASE_DETAILLANT  WHERE REFILL_DATE = DATE_SUB('###SLICE_VALUE###',1)) D,
(SELECT COUNT(*) nb_last_site_day FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###',1)) F