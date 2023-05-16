SELECT
msisdn,
std_code,
profile,
depot,
usage,
unite,
event_date
FROM
MON.SPARK_DEPOT_CLIENT_B2B WHERE EVENT_DATE = concat('###SLICE_VALUE###','-','01')