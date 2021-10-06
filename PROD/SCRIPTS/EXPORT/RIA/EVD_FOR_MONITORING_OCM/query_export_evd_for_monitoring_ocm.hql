SELECT 
 refill_id

 ,concat_ws(' ',regexp_replace(from_unixtime(unix_timestamp(refill_date ,'yyyy-MM-dd'), 'dd-MM-yyyy'),'-','.') ,'00:00:00') AS refill_date

 ,from_unixtime(unix_timestamp(refill_time,'HHmmss') + 3600, 'HH:mm:ss') AS refill_time

 ,receiver_msisdn

 ,receiver_profile

 ,sender_msisdn

 ,refill_mean

 ,refill_type

 ,CAST(refill_amount AS INT) refill_amount

 , CAST(refill_bonus AS INT) refill_bonus

 ,termination_ind

 ,concat_ws(' ',regexp_replace(from_unixtime(unix_timestamp(entry_date ,'yyyy-MM-dd'), 'dd-MM-yyyy'),'-','.') ,'00:00:00') AS entry_date

 ,sender_category

 ,CAST(sender_pre_bal AS BIGINT) AS sender_pre_bal

 ,CAST(sender_post_bal AS BIGINT) AS sender_post_bal

 ,receiver_pre_bal

 ,receiver_post_bal

from mon.SPARK_FT_REFILL

where refill_date = "###SLICE_VALUE###"
and termination_ind = 200