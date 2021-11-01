INSERT INTO MON.SPARK_FT_TECHNO_TRAFIC_GEO_REPORT_360_WEEK
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
    concat(year('###SLICE_VALUE###'), 'S', case when length(WEEKOFYEAR('###SLICE_VALUE###')) = 1 then concat('0', WEEKOFYEAR('###SLICE_VALUE###')) else WEEKOFYEAR('###SLICE_VALUE###') end) EVENT_WEEK
from MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR 
where event_date between '###SLICE_VALUE###' and date_add('###SLICE_VALUE###', 6)
group by 
    substr(event_date, 1, 7), 
    site_name, 
    TOWN,
    REGION,
    COMMERCIAL_REGION,
    techno_device

