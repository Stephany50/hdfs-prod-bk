-- total voice revenue
INSERT INTO FT_BUSINESS_ACTIVITY
select TRANSACTION_DATE, 'VO_VOX_REV_ALL' USAGE_CODE, 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE, SUM(MAIN_RATED_AMOUNT) KPI, SYSDATE
from ft_gsm_traffic_revenue_daily
where TRANSACTION_DATE IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_VOX_REV_ALL'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef)) AND service_code LIKE '%VOX%'
GROUP BY TRANSACTION_DATE;

COMMIT;


-- total voice revenu onnet
INSERT INTO FT_BUSINESS_ACTIVITY
select transaction_date, 'VO_VOX_REV_ONNET' usage_code, 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE, sum(MAIN_RATED_AMOUNT) kpi, sysdate
from FT_GSM_TRAFFIC_REVENUE_DAILY
where transaction_date IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_VOX_REV_ONNET'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
 and CALL_DESTINATION_CODE in ('OCM_D','ONNET')
 and service_code LIKE '%VOX%'
group by transaction_date;

commit;


-- total voice revenu offnet
INSERT INTO FT_BUSINESS_ACTIVITY
select transaction_date, 'VO_VOX_REV_OFFNET' usage_code, 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE, sum(MAIN_RATED_AMOUNT) kpi, sysdate
from FT_GSM_TRAFFIC_REVENUE_DAILY
where transaction_date IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_VOX_REV_OFFNET'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
 and CALL_DESTINATION_CODE in ('NEXTTEL','MTN','CAM')
 and service_code LIKE '%VOX%'
group by transaction_date;

commit;


-- total voice revenu international
INSERT INTO FT_BUSINESS_ACTIVITY
select transaction_date, 'VO_VOX_REV_INTL' usage_code, 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE, sum(MAIN_RATED_AMOUNT) kpi, sysdate
from FT_GSM_TRAFFIC_REVENUE_DAILY
where transaction_date IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_VOX_REV_INTL'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
 and CALL_DESTINATION_CODE = 'INT'
 and service_code LIKE '%VOX%'
group by transaction_date;

commit;

-- PayGo revenue
INSERT INTO FT_BUSINESS_ACTIVITY
SELECT
    TRANSACTION_DATE,
    'VO_PAYGO_REV' USAGE_CODE,
    'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE,
    SUM(MAIN_RATED_AMOUNT) KPI,
    SYSDATE INSERT_DATE
FROM FT_GSM_TRAFFIC_REVENUE_DAILY
WHERE TRANSACTION_DATE IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_PAYGO_REV'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
AND SERVICE_CODE LIKE '%VOX%'
AND CALL_DESTINATION_CODE NOT IN ('VAS', 'EMERG')
GROUP BY TRANSACTION_DATE;

COMMIT;


-- Bundle Voice
insert into ft_business_activity
SELECT
    TRANSACTION_DATE,
    'VO_BDLE_REV' USAGE_CODE,
    'FT_SUBSCRIPTION' SOURCE_TABLE,
    SUM(AMOUNT_VOICE_ONNET + AMOUNT_VOICE_OFFNET + AMOUNT_VOICE_INTER + AMOUNT_VOICE_ONNET) KPI,
    SYSDATE INSERT_DATE
FROM FT_SUBSCRIPTION
WHERE TRANSACTION_DATE IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_BDLE_REV'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
GROUP BY TRANSACTION_DATE;

commit;


-- arpu voice
insert into ft_business_activity
select event_date , 'VO_ARPU_VOICE' USAGE_CODE, 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE, SUM(CONSO_TEL)/COUNT(MSISDN) KPI, SYSDATE
from ft_conso_msisdn_day
WHERE EVENT_DATE IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_ARPU_VOICE'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
GROUP BY EVENT_DATE;

commit;


-- ARPU VOICE ONNET
insert into ft_business_activity
select event_date , 'VO_ARPU_VOX_ONNET' USAGE_CODE, 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE, SUM(ONNET_MAIN_TEL_CONSO)/COUNT(MSISDN) KPI, SYSDATE
from ft_conso_msisdn_day
WHERE EVENT_DATE IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_ARPU_VOX_ONNET'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
GROUP BY EVENT_DATE;

commit;


-- ARPU VOICE OFFNET
insert into ft_business_activity
select event_date , 'VO_ARPU_VOX_OFFNET' USAGE_CODE, 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE, SUM(MTN_MAIN_TEL_CONSO+NEXTTEL_MAIN_TEL_CONSO+CAMTEL_MAIN_TEL_CONSO)/COUNT(MSISDN) KPI, SYSDATE
from ft_conso_msisdn_day
WHERE EVENT_DATE IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_ARPU_VOX_OFFNET'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
GROUP BY EVENT_DATE;

commit;


-- ARPU VOICE INTERNATIONAL
insert into ft_business_activity
select event_date , 'VO_ARPU_VOX_INTL' USAGE_CODE, 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE, SUM(INTERNATIONAL_MAIN_TEL_CONSO)/COUNT(MSISDN) KPI, SYSDATE
from ft_conso_msisdn_day
WHERE EVENT_DATE IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_ARPU_VOX_INTL'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
GROUP BY EVENT_DATE;

commit;


-- AUPU VOICE
insert into ft_business_activity
select event_date , 'VO_AUPU_VOICE' USAGE_CODE, 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE, SUM(TEL_DURATION)/COUNT(MSISDN) KPI, SYSDATE
from ft_conso_msisdn_day
WHERE EVENT_DATE IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_AUPU_VOICE'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
GROUP BY EVENT_DATE;

COMMIT;



-- AUPU VOICE ONNET
insert into ft_business_activity
select event_date , 'VO_AUPU_VOX_ONNET' USAGE_CODE, 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE, SUM(ONNET_DURATION)/COUNT(MSISDN) KPI, SYSDATE
from ft_conso_msisdn_day
WHERE EVENT_DATE IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_AUPU_VOX_ONNET'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
GROUP BY EVENT_DATE;

commit;


-- AUPU VOICE OFFNET
insert into ft_business_activity
select event_date , 'VO_AUPU_VOX_OFFNET' USAGE_CODE, 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE, SUM(MTN_DURATION + CAMTEL_DURATION + NEXTTEL_DURATION)/COUNT(MSISDN) KPI, SYSDATE
from ft_conso_msisdn_day
WHERE EVENT_DATE IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_AUPU_VOX_OFFNET'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
GROUP BY EVENT_DATE;

commit;


-- AUPU VOICE INTERNATIONAL
insert into ft_business_activity
select event_date , 'VO_AUPU_VOX_INTL' USAGE_CODE, 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE, SUM(INTERNATIONAL_DURATION)/COUNT(MSISDN) KPI, SYSDATE
from ft_conso_msisdn_day
WHERE EVENT_DATE IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_AUPU_VOX_INTL'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
GROUP BY EVENT_DATE;

commit;


-- PPM VOICE
insert into ft_business_activity
select EVENT_DATE, 'VO_PPM_VOICE' USAGE_CODE, 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE, SUM(CONSO_TEL)/SUM(TEL_DURATION) KPI, SYSDATE
from ft_conso_msisdn_day
WHERE EVENT_DATE IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_PPM_VOICE'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
GROUP BY EVENT_DATE;

commit;

-- PPM VOICE ONNET
insert into ft_business_activity
select EVENT_DATE, 'VO_PPM_VOX_ONNET' USAGE_CODE, 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE, SUM(ONNET_MAIN_TEL_CONSO)/SUM(ONNET_DURATION) KPI, SYSDATE
from ft_conso_msisdn_day
WHERE EVENT_DATE IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_PPM_VOX_ONNET'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
GROUP BY EVENT_DATE;

commit;

-- PPM VOICE OFFNET
insert into ft_business_activity
select EVENT_DATE, 'VO_PPM_VOX_OFFNET' USAGE_CODE, 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE, SUM(MTN_MAIN_TEL_CONSO+NEXTTEL_MAIN_TEL_CONSO+CAMTEL_MAIN_TEL_CONSO)/SUM(MTN_DURATION + CAMTEL_DURATION + NEXTTEL_DURATION) KPI, SYSDATE
from ft_conso_msisdn_day
WHERE EVENT_DATE IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_PPM_VOX_OFFNET'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
GROUP BY EVENT_DATE;

commit;



-- PPM VOICE INTERNATIONAL
insert into ft_business_activity
select EVENT_DATE, 'VO_PPM_VOX_INTL' USAGE_CODE, 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE, SUM(INTERNATIONAL_MAIN_TEL_CONSO)/SUM(INTERNATIONAL_DURATION) KPI, SYSDATE
from ft_conso_msisdn_day
WHERE EVENT_DATE IN ( SELECT DATECODE
            FROM (SELECT DISTINCT DATECODE FROM DIM.DT_DATES
                     WHERE DATECODE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
                   MINUS
              SELECT DISTINCT TRANSACTION_DATE FROM FT_BUSINESS_ACTIVITY
                    WHERE USAGE_CODE = 'VO_PPM_VOX_INTL'
                    AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-dashboard_kpi_day_from) AND TRUNC(SYSDATE-dashboard_kpi_day_bef))
GROUP BY EVENT_DATE;

commit;
