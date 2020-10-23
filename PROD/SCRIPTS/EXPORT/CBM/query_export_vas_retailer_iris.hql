select
    Sdate `DATE`,
    substr(start_time, 12, 2) Hour,
    ret_msisdn MSISDN_retailer,
    sub_msisdn MSISDN_customer,
    offer_name Bundle_Name,
    CHANNEL,
    offer_type Bundle_Type,
    sum(RECHARGE_AMOUNT) RECHARGE_AMOUNT,
    sum(RETAILER_COMMISSION) RETAILER_COMMISSION,
    count(*) transaction_count
from MON.SPARK_FT_VAS_RETAILLER_IRIS
where sdate = '###SLICE_VALUE###' and PRETUPS_STATUSCODE = '200'
group by Sdate, substr(start_time, 12, 2), ret_msisdn, sub_msisdn, offer_name, offer_type, CHANNEL
