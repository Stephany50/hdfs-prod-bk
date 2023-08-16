create table tt.spark_cbm_data as
select
"YY PayGo" as bdle_name,
date,
"USSD" as subscription_channel,
souscriptions,
revenu,
"USSD" as Paiement
from 
(
    select 
        distinct period as date, 
        sum(MA_VOICE_ONNET + MA_VOICE_OFNET + MA_VOICE_INTER) as revenu,
        sum((case when MA_VOICE_ONNET + MA_VOICE_OFNET + MA_VOICE_INTER > 0 then 1 else 0 end)) as souscriptions
    FROM MON.SPARK_FT_CBM_CUST_INSIGTH_DAILY 
    WHERE PERIOD = "###SLICE_VALUE###"
    group by date
)

union

select
    "YY PayGo SMS" as bdle_name,
    date,
    "USSD" as subscription_channel,
    souscriptions,
    revenu,
    "USSD" as Paiement
from 
(
    select 
        distinct period as date, 
        sum(MA_SMS_ONNET + MA_SMS_OFNET + MA_SMS_INTER) as revenu,
        sum((case when MA_SMS_ONNET + MA_SMS_OFNET + MA_SMS_INTER > 0 then 1 else 0 end)) as souscriptions
    FROM MON.SPARK_FT_CBM_CUST_INSIGTH_DAILY 
    WHERE PERIOD = "###SLICE_VALUE###"
    group by date
)
union
select
    offer_name as bdle_name,
    date,
    "USSD" as subscription_channel,
    souscriptions,
    revenu,
    "USSD" as Paiement
from (
    select 
        date,
        offer_name,
        sum(recharge_amount) as revenu,
        sum(case when recharge_amount>0 then 1 else 0 end) as souscriptions
    from (
        select
        Sdate `date`,
        offer_name,
        sum(RECHARGE_AMOUNT) recharge_amount
        from MON.SPARK_FT_VAS_RETAILLER_IRIS
        where sdate = '###SLICE_VALUE###' and PRETUPS_STATUSCODE = '200'
        group by Sdate, substr(start_time, 12, 2), ret_msisdn, sub_msisdn, offer_name, offer_type, CHANNEL
    )
    group by date, offer_name
)

union

select
"ZZ Emergency Data" as bdle_name,
event_date as date,
"USSD" as subscription_channel,
souscriptions,
revenu,
"USSD" as Paiement
from (
    select event_date,
        sum(montant_loan) as revenu,
        sum(case when montant_loan > 0 then 1 else 0 end) as souscriptions
    from (
        SELECT
            SUM(CASE TRANSACTION_TYPE WHEN 'LOAN' THEN AMOUNT ELSE 0 END ) MONTANT_LOAN,
            TRANSACTION_DATE EVENT_DATE
        FROM MON.SPARK_FT_EMERGENCY_DATA
        WHERE TRANSACTION_DATE ='###SLICE_VALUE###'
        GROUP BY MSISDN,TRANSACTION_DATE,TRANSACTION_TYPE, CONTACT_CHANNEL, OFFER_PROFILE_CODE, OPERATOR_CODE,
        (CASE WHEN AMOUNT = 100 THEN '100' 
        WHEN AMOUNT = 200 THEN '200' 
        WHEN AMOUNT = 500 THEN '500' 
        ELSE 'AUTRES' 
        END)
    )
    group by event_date
)

union

select
"ZZ Emergency Voice" as bdle_name,
event_date as date,
"USSD" as subscription_channel,
souscriptions,
revenu,
"USSD" as Paiement
from (
    select event_date,
        sum(amount) as revenu,
        sum(case when amount > 0 then 1 else 0 end) as souscriptions
    from (
        SELECT
            SUM(CASE TRANSACTION_TYPE WHEN 'LOAN' THEN AMOUNT/100 ELSE 0 END ) amount,
            original_file_date EVENT_DATE
        FROM CDR.SPARK_IT_ZTE_LOAN_CDR
        WHERE original_file_date ='###SLICE_VALUE###'
        GROUP BY MSISDN,original_file_date,TRANSACTION_TYPE, CONTACT_CHANNEL, price_plan_code,
            (CASE WHEN AMOUNT/100 = 100 THEN '100' 
            WHEN AMOUNT/100 = 200 THEN '200' 
            WHEN AMOUNT/100 = 500 THEN '500' 
            ELSE 'AUTRES' 
            END)
    )
    group by event_date
)

union

select
    bdle_name,
    period date,
    subscription_channel,
    count(msisdn) as souscriptions,
    sum(bdle_cost) as revenu,
    (case when subscription_channel in ('32','111') or (upper(subscription_channel) like '%GOS%SDP%' and upper(subscription_channel) like '%MY%WAY%DIGITAL%') then "OM" else "USSD" end) as Paiement
from MON.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
WHERE BDLE_COST > 0 and period = '###SLICE_VALUE###'
group by date, bdle_name, subscription_channel, (case when subscription_channel in ('32','111') or (upper(subscription_channel) like '%GOS%SDP%' and upper(bdle_name) like '%MY%WAY%DIGITAL%') then "OM" else "USSD" end)
