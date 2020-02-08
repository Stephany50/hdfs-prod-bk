INSERT INTO into TMP.TT_FT_OTARIE_IMEI_ONLINE
select
    imei,
    NULL IMSI,
    msisdn,
    count(*) NBRE,
    , 'OTARIE|' SRC_TABLE
    , CURRENT_TIMESTAMP INSERT_DATE
    transaction_date sdate
from MON.FT_OTARIE_DATA_TRAFFIC_DAY
where transaction_date = '###SLICE_VALUE###'
group by
transaction_date,
msisdn,
imei