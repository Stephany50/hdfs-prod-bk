select
    Sdate `date`,
    substr(start_time, 12, 2) hour,
    ret_msisdn msisdn_retailer,
    sub_msisdn msisdn_customer,
    offer_name bundle_name,
    channel,
    offer_type bundle_type,
    sum(RECHARGE_AMOUNT) recharge_amount,
    sum(RETAILER_COMMISSION) retailer_commission,
    count(*) transaction_count
from MON.SPARK_FT_VAS_RETAILLER_IRIS
where sdate = '###SLICE_VALUE###' and PRETUPS_STATUSCODE = '200'
group by Sdate, substr(start_time, 12, 2), ret_msisdn, sub_msisdn, offer_name, offer_type, CHANNEL
