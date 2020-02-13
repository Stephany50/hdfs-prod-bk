INSERT INTO mon.spark_ft_data_conso_msisdn_month
SELECT
 nvl(a.msisdn, b.msisdn) msisdn                         
, nvl(a.acommercial_offer, b.acommercial_offer) commercial_offer               
, nvl(a.bytes_sent, b.bytes_sent) bytes_sent                     
, nvl(a.bytes_received, b.bytes_received) bytes_received                 
, nvl(a.mms_count, b.mms_count) mms_count                      
, nvl(a.main_rated_amount, b.main_rated_amount) main_rated_amount              
, nvl(a.promo_rated_amount, b.promo_rated_amount) promo_rated_amount             
, nvl(a.served_party_imsi, b.served_party_imsi) served_party_imsi              
, nvl(a.served_party_imei, b.served_party_imei) served_party_imei              
, nvl(a.bytes_used_in_bundle, b.bytes_used_in_bundle) bytes_used_in_bundle           
, nvl(a.bytes_used_out_bundle, b.bytes_used_out_bundle) bytes_used_out_bundle          
, nvl(a.bytes_used_in_bundle_roaming, b.bytes_used_in_bundle_roaming) bytes_used_in_bundle_roaming   
, nvl(a.bytes_used_out_bundle_roaming, b.bytes_used_out_bundle_roaming) bytes_used_out_bundle_roaming  
, nvl(a.bundle_mms_used_volume, b.bundle_mms_used_volume) bundle_mms_used_volume         
, nvl(a.main_rated_amount_roaming, b.main_rated_amount_roaming) main_rated_amount_roaming      
, nvl(a.promo_rated_amount_roaming, b.promo_rated_amount_roaming) promo_rated_amount_roaming     
, nvl(a.gos_debit_count, b.gos_debit_count) gos_debit_count                
, nvl(a.gos_session_count, b.gos_session_count) gos_session_count              
, nvl(a.gos_refund_count, b.gos_refund_count) gos_refund_count               
, nvl(a.gos_debit_amount, b.gos_debit_amount) gos_debit_amount               
, nvl(a.gos_session_amount, b.gos_session_amount) gos_session_amount             
, nvl(a.gos_refund_amount, b.gos_refund_amount) gos_refund_amount              
, nvl(a.bundle_bytes_remaining_volume, b.bundle_bytes_remaining_volume) bundle_bytes_remaining_volume  
, nvl(a.bundle_mms_remaining_volume, b.bundle_mms_remaining_volume) bundle_mms_remaining_volume    
, nvl(a.source_table, b.source_table) source_table                   
, nvl(a.operator_code, b.operator_code) operator_code                  
, nvl(a.active_days_count, b.active_days_count) active_days_count              
, nvl(a.first_active_day, b.first_active_day) first_active_day               
, nvl(a.last_active_day, b.last_active_day) last_active_day               
, IF(a.msisdn IS NULL OR b.msisdn IS NULL, 
  nvl(a.max_bytes_used, b.max_bytes_used),
  case when nvl(a.bytes_sent+a.bytes_received, 0) >= nvl(b.MAX_BYTES_USED, 0) then nvl(a.bytes_sent+a.bytes_received, 0) else nvl(b.MAX_BYTES_USED, 0) end
) max_bytes_used                 
, IF(a.msisdn IS NULL OR b.msisdn IS NULL,
  nvl(a.month_max_bytes_used, b.month_max_bytes_used),
  case when nvl(a.bytes_sent+a.bytes_received, 0) >= nvl(b.MAX_BYTES_USED, 0) then a.EVENT_MONTH else b.MONTH_MAX_BYTES_USED  end
) month_max_bytes_used           
, nvl(a.insert_date, b.insert_date) insert_date                    
, nvl(a.event_month, b.event_month) event_month 
FROM tmp.spark_tt_data_conso_msisdn_month A
FULL OUTER JOIN (
    select * from MOM.SPARK_FT_DATA_CONSO_MSISDN_MONTH
        where event_month = DATE_FORMAT(ADD_MONTHS(concat('###SLICE_VALUE###','-01'),-1) ,'yyyy-MM');
) B
ON a.msisdn = b.msisdn
