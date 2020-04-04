SELECT
UNIX_TIMESTAMP(end_date) DIV 300 * 300  time,
commercial_offer,
administrative_region,
contract_type,
subs_channel,
ORIGINAL_FILE_NAME,
subscription_service,
sum(subs_amount) amount,SUM(subs_total_count)  nbr_total,
sum(case when subs_amount <> 0 then subs_total_count ELSE 0 END)  nbr_fact,
sum(case when subs_amount = 0 then subs_total_count ELSE 0 END)  nbr_nonfact
FROM subs_activity
group by
UNIX_TIMESTAMP(end_date) DIV 300 * 300 ,
commercial_offer,
administrative_region,
contract_type,subs_channel,
subscription_service,
subscription_service_details,
ORIGINAL_FILE_NAME