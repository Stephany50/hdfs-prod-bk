SELECT
msisdn,
std_code,
profile,
depot,
usage,
unite,
event_date
FROM
MON.SPARK_SOLDE_CLIENT_B2B WHERE EVENT_DATE = '###SLICE_VALUE###'