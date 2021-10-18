INSERT INTO MON.SPARK_FT_TECHNO_TRAFIC_GEO_REPORT_360_MONTH
select 
    site_name,
    TOWN,
    REGION,
    COMMERCIAL_REGION,
    techno_device,
    sum(trafic_voix) trafic_voix,
    sum(trafic_data) trafic_data,
    sum(trafic_sms) trafic_sms,
    CURRENT_TIMESTAMP() INSERT_DATE,
    substr(event_date, 1, 7) event_month
from MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR 
where event_date between concat(substr('###SLICE_VALUE###', 1, 7), '-01') and last_day(concat(substr('###SLICE_VALUE###', 1, 7), '-01'))
group by 
    substr(event_date, 1, 7), 
    site_name, 
    TOWN,
    REGION,
    COMMERCIAL_REGION,
    techno_device
