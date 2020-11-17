SELECT 
   msisdn,
   site_name,
   townname,
   administrative_region,
   commercial_region,
   last_location_day,
   operator_code,
   insert_date,
   event_date
FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
WHERE EVENT_DATE= "###SLICE_VALUE###"