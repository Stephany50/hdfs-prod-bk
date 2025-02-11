SELECT IF(
    E.nb_rupt_telco < 4 AND
    F.nb_rupt_tmp < 4 AND
    G.nb_rupt_om > 0 AND
    B.nb_zebra_master > 0 AND
    C.nb_last_month > 0 AND
    D.nb_last_site_day > 0
    ,"OK","NOK")
FROM
(SELECT COUNT(distinct event_time) nb_rupt_telco FROM DD.SPARK_FT_RUPT_RETAILER_TELCO WHERE EVENT_DATE = '###SLICE_VALUE###') E,
(SELECT COUNT(distinct event_time) nb_rupt_tmp FROM TMP.FT_RUPT_RETAILER_TELCO WHERE EVENT_DATE = '###SLICE_VALUE###') F,
(SELECT COUNT(*) nb_rupt_om FROM DD.SPARK_FT_RUPT_RETAILER_OM WHERE EVENT_DATE = '###SLICE_VALUE###') G,
(SELECT COUNT(*) nb_zebra_master FROM CDR.SPARK_IT_ZEBRA_MASTER_BALANCE WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###',1)) B,
(SELECT COUNT(*) nb_last_month from DD.TT_RECHARGE_BY_RETAILLER_MONTH_TELCO where EVENT_MONTH = SUBSTR(REPLACE(ADD_MONTHS(DATE_SUB('###SLICE_VALUE###',1), -1), '-',''), 0, 6)) C,
(SELECT COUNT(*) nb_last_site_day FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###',1)) D