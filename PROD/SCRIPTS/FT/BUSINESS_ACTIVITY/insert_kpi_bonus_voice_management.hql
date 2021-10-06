
-- Total minutes
select transaction_date, 'BVM_TOT_MIN' USAGE_CODE, 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE
, sum(duration)/60 TOTAL_MINUTES, CURRENT_TIMESTAMP
from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
where transaction_date ='###SLICE_VALUE###'
 and service_code = 'VOI_VOX'
group by transaction_date



-- Main account minutes
UNION ALL 
select transaction_date, 'BVM_MA_MIN' USAGE_CODE, 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE, sum(rated_duration)/60 MAIN_MINUTES, CURRENT_TIMESTAMP
from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
where transaction_date ='###SLICE_VALUE###'
 and service_code = 'VOI_VOX'
group by transaction_date



-- Ratio
UNION ALL 
select transaction_date, 'BVM_MA_TOT' USAGE_CODE, 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE, sum(rated_duration)/sum(duration) ratio, CURRENT_TIMESTAMP
from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
where transaction_date ='###SLICE_VALUE###'
 and service_code = 'VOI_VOX'
group by transaction_date




-- Total Bonus Minutes
UNION ALL 
select transaction_date, 'BVM_TOT_BONUS_MIN' usage_code, 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE, sum(time_used_volume)/60 TOTAL_BONUS_MIN, CURRENT_TIMESTAMP
from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
where transaction_date ='###SLICE_VALUE###'
 and service_code = 'VOI_VOX'
group by transaction_date





-- BONUS MIN ONNET
UNION ALL 
select transaction_date, 'BVM_MIN_ONNET' usage_code, 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE, sum(time_used_volume)/60 bonus_min_onnet, CURRENT_TIMESTAMP
from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
where transaction_date ='###SLICE_VALUE###'
 and CALL_DESTINATION_CODE in ('OCM_D','ONNET')
 and service_code = 'VOI_VOX'
group by transaction_date



-- BONUS MIN OFFNET
UNION ALL 
select transaction_date, 'BVM_MIN_OFFNET' usage_code, 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE, sum(time_used_volume)/60 bonus_min_offnet, CURRENT_TIMESTAMP
from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
where transaction_date ='###SLICE_VALUE###'
 and CALL_DESTINATION_CODE in ('NEXTTEL','MTN','CAM')
 and service_code = 'VOI_VOX'
group by transaction_date





-- BONUS MINUTES INTERNATIONAL
UNION ALL 
select transaction_date, 'BVM_MIN_INT' usage_code, 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE, sum(time_used_volume)/60 bonus_min_international, CURRENT_TIMESTAMP
from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
where transaction_date ='###SLICE_VALUE###'
 and CALL_DESTINATION_CODE = 'INT'
 and service_code = 'VOI_VOX'
group by transaction_date



-- BONUS MIN ONNET pourcentage
UNION ALL 
select transaction_date, 'BVM_MIN_ONNET_P' usage_code, 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE, sum(time_used_volume)*100/SUM(DURATION) KPI, CURRENT_TIMESTAMP
from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
where transaction_date ='###SLICE_VALUE###'
 and service_code = 'VOI_VOX'
group by transaction_date



-- BONUS MIN OFFNET pourcentage
UNION ALL 
select transaction_date, 'BVM_MIN_OFFNET_P' usage_code, 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE, sum(time_used_volume)*100/SUM(DURATION) KPI, CURRENT_TIMESTAMP
from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
where transaction_date ='###SLICE_VALUE###'
 and CALL_DESTINATION_CODE in ('NEXTTEL','MTN','CAM')
 and service_code = 'VOI_VOX'
group by transaction_date




-- BONUS MINUTES INTERNATIONAL pourcentage
UNION ALL 
select transaction_date, 'BVM_MIN_INT_P' usage_code, 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE, sum(time_used_volume)*100/SUM(DURATION) KPI, CURRENT_TIMESTAMP
from AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
where transaction_date ='###SLICE_VALUE###'
 and CALL_DESTINATION_CODE = 'INT'
 and service_code = 'VOI_VOX'
group by transaction_date