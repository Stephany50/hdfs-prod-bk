INSERT INTO MON.SPARK_FT_TECHNO_TRAFIC_GEO_REPORT_360_MONTHC
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
    substr(event_date, 1, 7) event_month
from MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR 
where event_date between concat(substr('###SLICE_VALUE###', 1, 7), '-01') and last_day(concat(substr('###SLICE_VALUE###', 1, 7), '-01'))
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
