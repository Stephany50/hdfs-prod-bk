SELECT
msisdn,
std_code,
profile,
sum(CAST(REPLACE(depot,',','') AS DECIMAL(19,3))) depot,
usage,
unite,
event_date
FROM
MON.SPARK_DEPOT_CLIENT_B2B WHERE EVENT_DATE = concat('###SLICE_VALUE###','-','01') AND std_code in ('1','59')
GROUP BY msisdn,std_code,profile,usage,unite,event_date

UNION

SELECT
distinct msisdn,
std_code,
profile,
depot,
usage,
unite,
event_date
FROM
MON.SPARK_DEPOT_CLIENT_B2B WHERE EVENT_DATE = concat('###SLICE_VALUE###','-','01') AND std_code not in ('1','59')