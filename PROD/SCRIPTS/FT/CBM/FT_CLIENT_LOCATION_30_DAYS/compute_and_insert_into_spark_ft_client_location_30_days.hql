insert into mon.spark_ft_client_location_30_days 
select 
msisdn , 
region, 
ville, 
current_timestamp insert_date ,
'###SLICE_VALUE###' event_date 
from (
    select 
        fn_format_msisdn_to_9digits(MSISDN) MSISDN,
        ADMINISTRATIVE_REGION region ,
        TOWNNAME ville ,
        ROW_NUMBER() OVER (PARTITION BY fn_format_msisdn_to_9digits(MSISDN) ORDER BY SUM (NVL (DUREE_SORTANT, 0) + NVL (DUREE_ENTRANT, 0) + NVL (NBRE_SMS_SORTANT, 0)  + NVL (NBRE_SMS_ENTRANT, 0) ) DESC) AS Rang 
    from  mon.spark_ft_client_site_traffic_day 
    where event_date between date_sub('###SLICE_VALUE###',29) and  '###SLICE_VALUE###'  and  ADMINISTRATIVE_REGION is not null 
    group by fn_format_msisdn_to_9digits(MSISDN),TOWNNAME,ADMINISTRATIVE_REGION )a where Rang=1 and length(msisdn)=9