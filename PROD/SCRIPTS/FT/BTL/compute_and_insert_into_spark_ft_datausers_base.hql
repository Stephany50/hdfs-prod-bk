insert into MON.SPARK_FT_DATAUSERS_DAY
select distinct
    msisdn, 
    case 
        when nvl(trafic_30d,0) > 1 then 'active_30d' 
        when nvl(trafic_30d,0) <= 1 and nvl(trafic_90d,0) > 1 then 'active_31-90d' 
        when nvl(trafic_90d,0) <= 1 and nvl(trafic_120d,0) > 1 then 'active_91-120d'
        else 'inactive_data'
    end segment_data,
    last_day_month,
    current_timestamp insert_date,
    '###SLICE_VALUE###' event_month 
from 
(
    SELECT
        CHARGED_PARTY_MSISDN MSISDN,
        last_day(last_day('###SLICE_VALUE###')) last_day_month,            
        SUM(case when session_date between date_sub(last_day('###SLICE_VALUE###'),29) AND last_day('###SLICE_VALUE###') then NVL(BYTES_RECEIVED,0) + NVL(BYTES_SENT,0) end )/1024/1024 trafic_30d,
        SUM(case when session_date between date_sub(last_day('###SLICE_VALUE###'),89) AND last_day('###SLICE_VALUE###') then NVL(BYTES_RECEIVED,0) + NVL(BYTES_SENT,0) end )/1024/1024 trafic_90d,
        SUM(case when session_date between date_sub(last_day('###SLICE_VALUE###'),119) AND last_day('###SLICE_VALUE###') then NVL(BYTES_RECEIVED,0) + NVL(BYTES_SENT,0) end )/1024/1024 trafic_120d
    FROM MON.SPARK_FT_CRA_GPRS
    WHERE SESSION_DATE BETWEEN date_sub(last_day('###SLICE_VALUE###'), 119) AND last_day('###SLICE_VALUE###')
    AND NVL(MAIN_COST, 0) >= 0
    GROUP BY 
        CHARGED_PARTY_MSISDN,
        last_day(last_day('###SLICE_VALUE###'))
) T