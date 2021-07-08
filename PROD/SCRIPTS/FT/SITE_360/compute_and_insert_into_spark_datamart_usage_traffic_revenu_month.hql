INSERT INTO MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_MONTH
SELECT
    SITE_NAME,
    TECHNO_DEVICE,
    COUNT(DISTINCT MSISDN) PARC,
    SUM(NVL(TRAFIC_VOIX, 0)) TRAFIC_VOIX, 
    SUM(NVL(TRAFIC_DATA, 0)) TRAFIC_DATA, 
    SUM(NVL(TRAFIC_SMS, 0)) TRAFIC_SMS,
    SUM(NVL(REVENU_VOIX_PYG, 0)) REVENU_VOIX_PYG,
    SUM(NVL(REVENU_VOIX_SUBS, 0)) REVENU_VOIX_SUBS, 
    SUM(NVL(REVENU_DATA, 0)) REVENU_DATA, 
    SUM(NVL(REVENU_SMS_PYG, 0)) REVENU_SMS_PYG,
    SUM(NVL(REVENU_SMS_SUBS, 0)) REVENU_SMS_SUBS, 
    CURRENT_TIMESTAMP INSERT_DATE,
    '###SLICE_VALUE###' EVENT_MONTH
FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
WHERE EVENT_DATE BETWEEN '###SLICE_VALUE###-01' AND DATE_SUB(ADD_MONTHS('###SLICE_VALUE###-01', 1), 1)
GROUP BY 
    SITE_NAME,
    TECHNO_DEVICE


create table tmp.tab12 as
select
substr(event_date, 1, 7) month,
site_name,
device_type,
TECHNO_DEVICE,
cast(round(sum(1/nber_times_in_parc_groupe)) as bigint) parc_groupe
from
( 
select
event_date,
msisdn,
device_type,
TECHNO_DEVICE,
SITE_NAME
FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
WHERE EVENT_DATE BETWEEN '2021-03-01' AND '2021-03-31' and est_parc_groupe='OUI'
) a
left JOIN
(
select
msisdn,
count(*) nber_times_in_parc_groupe
from MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR 
where event_date between '2021-03-01' AND '2021-03-31' and est_parc_groupe='OUI'
group by msisdn
) b on a.msisdn = b.msisdn
group by 
substr(event_date, 1, 7),
site_name,
device_type,
TECHNO_DEVICE
order by 1, 2, 3, 4 desc


hive --hiveconf tez.queue.name=compute --outputFormat=csv2 -e "  select    substr(event_date, 1, 7) month,    site_name,    device_type,    TECHNO_DEVICE,    cast(round(sum(1/nber_times_in_parc_groupe)) as bigint) parc_groupe    from    (     select    event_date,    msisdn,    device_type,    TECHNO_DEVICE,    SITE_NAME    FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR    WHERE EVENT_DATE BETWEEN '2021-03-01' AND '2021-03-31' and est_parc_groupe='OUI'    ) a    left JOIN    (    select    msisdn,    count(*) nber_times_in_parc_groupe    from MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR     where event_date between '2021-03-01' AND '2021-03-31' and est_parc_groupe='OUI'    group by msisdn    ) b on a.msisdn = b.msisdn    group by     substr(event_date, 1, 7),    site_name,    device_type,    TECHNO_DEVICE    order by 1, 2, 3, 4 desc     " > EXTRACT_month_april_2021.csv  


hive --hiveconf tez.queue.name=compute --outputFormat=csv2 -e " select * from tmp.tab1 " > EXTRACT_month_mai_2021.csv  
