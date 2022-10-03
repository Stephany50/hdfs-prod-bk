SELECT 
IF(
    A.FT_EXIST = 0 and 
    B.IT_COUNT > 20000 and
    C.FT_EXIST = datediff(last_day(concat(substr(add_months('###SLICE_VALUE###', -1), 1, 7), '-01')), concat(substr(add_months('###SLICE_VALUE###', -1), 1, 7), '-01')) + 1 and
    D.FT_EXIST > 0 and
    E.FT_EXIST > 0 and 
    F.FT_EXIST > 0
    , "OK","NOK"
) FROM
(SELECT COUNT(*) FT_EXIST FROM mon.spark_ft_crm_reporting WHERE event_date='###SLICE_VALUE###') A,
(SELECT COUNT(*) IT_COUNT FROM cdr.spark_it_crm_reporting where original_file_date='###SLICE_VALUE###') B,
(SELECT COUNT(distinct event_date) FT_EXIST FROM MON.SPARK_FT_CBM_ARPU_MOU where event_date between concat(substr(add_months('###SLICE_VALUE###', -1), 1, 7), '-01') and last_day(concat(substr(add_months('###SLICE_VALUE###', -1), 1, 7), '-01')) ) C,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CONTRACT_SNAPSHOT where EVENT_DATE = DATE_ADD('###SLICE_VALUE###',1) ) D,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY where EVENT_DATE = '###SLICE_VALUE###' ) E,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE='###SLICE_VALUE###') F

