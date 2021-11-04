INSERT INTO MON.SPARK_FT_TECHNO_TRAFIC_GEO_REPORT_360_WEEKC
select 
    site_name,
    TOWN,
    REGION,
    COMMERCIAL_REGION,
    techno_device,
    case 
        when nvl(trafic_voix, 0) > 0 then 'VOIX' 
        when nvl(trafic_data, 0) > 0 then 'DATA' 
        when nvl(trafic_sms, 0) > 0 then 'SMS'
        else null
    end TYPE_DE_TRAFFIC,
    count(
        distinct
        case
            when nvl(trafic_voix, 0) > 0 then msisdn
            when nvl(trafic_data, 0) > 0 then msisdn
            when nvl(trafic_sms, 0) > 0 then msisdn
            else null
        end
    ) traffic,
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
    techno_device,
    case 
        when nvl(trafic_voix, 0) > 0 then 'VOIX' 
        when nvl(trafic_data, 0) > 0 then 'DATA' 
        when nvl(trafic_sms, 0) > 0 then 'SMS'
        else null
    end

