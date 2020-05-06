insert into MON.SPARK_FT_CBM_MOU_ARPU
select
        NVL(NVL(NVL(NVL(NVL(A.msisdn,B.msisdn),C.msisdn),D.msisdn ),E.msisdn),F.msisdn) as msisdn,
        COMBO_DATA,
        combo_voix,
        VAS_AMT,
        VAS_NB,
        MOU_ONNET,
        MOU_OFNET,
        MOU_INTER,
        bdles_onet,
        bdles_ofnet,
        bdles_inter,
        bdles_data,
        bdles_roaming_voix,
        bdles_roaming_data,
        Parrain,
        MA_DATA as PAYG_DATA,
        nb_calls,
        INC_NB_CALLS,
        MA_VOICE_ONNET + MA_SMS_ONNET + bdles_onet as arpu_onet,
        MA_VOICE_OFNET + MA_SMS_OFNET + bdles_ofnet as arpu_ofnet,
        MA_VOICE_INTER + MA_SMS_INTER + bdles_inter as arpu_inter,
        MA_DATA + bdles_data as arpu_data,
        MA_VOICE_ONNET + MA_SMS_ONNET + bdles_onet+MA_VOICE_OFNET + MA_SMS_OFNET
        + bdles_ofnet+MA_VOICE_INTER + MA_SMS_INTER + bdles_inter as arpu_voix,
        MA_VAS + MA_GOS_SVA + MONTANT as arpu_VAS,
        MA_VOICE_ONNET + MA_SMS_ONNET + bdles_onet+MA_VOICE_OFNET + MA_SMS_OFNET
        + bdles_ofnet+MA_VOICE_INTER + MA_SMS_INTER + bdles_inter + MA_VAS + MA_GOS_SVA + MONTANT
        + MA_DATA + bdles_data as arpu,
        bdles_roaming_voix + MA_VOICE_ROAMING + MA_SMS_ROAMING as arpu_roaming_voix,
        bdles_roaming_voix + MA_VOICE_ROAMING + MA_SMS_ROAMING + bdles_roaming_data as arpu_roaming,
        mou_onnet + MOU_OFNET + mou_inter as MOU,
        MA_VOICE_ONNET + MA_SMS_ONNET + bdles_onet+MA_VOICE_OFNET + MA_SMS_OFNET
        + bdles_ofnet+MA_VOICE_INTER + MA_SMS_INTER + bdles_inter + MA_VAS + MA_GOS_SVA + MONTANT
        + MA_DATA + bdles_data + Parrain as arpu2,
        MA_VOICE_ONNET + MA_VOICE_OFNET + MA_VOICE_INTER + MA_VOICE_ROAMING + MA_VOICE_SVA as paygo_webi,
        MA_VOICE_ONNET + MA_SMS_ONNET + MA_VOICE_OFNET + MA_SMS_OFNET + MA_VOICE_INTER + MA_SMS_INTER as PAYG_VOIX,
        MA_SMS_ONNET + MA_SMS_OFNET + MA_SMS_INTER + MA_SMS_SVA + MA_SMS_ROAMING as sms_webi,
        REF_AMT,
        REF_NB,
        volume_data,
        volume_chat,
        volume_voip,
        volume_4G,
        volume_3G,
        volume_ott,
        ott_user,
        CURRENT_TIMESTAMP  AS INSERT_DATE ,
        '###SLICE_VALUE###' AS EVENT_DATE
from(
        SELECT
        '6'||SUBSTR(MSISDN, -8) AS MSISDN,
        sum(nvl(MOU_ONNET,0)) as MOU_ONNET ,
        sum(nvl(MOU_OFNET,0)) as MOU_OFNET,
        sum(nvl(MOU_INTER,0)) as MOU_INTER,
        sum(nvl(MA_VOICE_ONNET,0)) as MA_VOICE_ONNET,
        sum(nvl(MA_VOICE_OFNET,0)) as MA_VOICE_OFNET,
        sum(nvl(MA_VOICE_INTER,0)) as MA_VOICE_INTER,
        sum(nvl(MA_SMS_OFNET,0)) as MA_SMS_OFNET,
        sum(nvl(MA_SMS_INTER,0)) as MA_SMS_INTER,
        sum(nvl(MA_SMS_ONNET,0)) as MA_SMS_ONNET,
        sum(nvl(MA_DATA,0)) AS MA_DATA,
        sum(nvl(MA_VAS,0)) as MA_VAS,
        sum(nvl(MA_GOS_SVA,0)) as MA_GOS_SVA,
        sum(nvl(MA_VOICE_ROAMING,0)) as MA_VOICE_ROAMING,
        sum(nvl(MA_SMS_ROAMING,0)) as MA_SMS_ROAMING,
        sum(nvl(nb_calls,0)) as nb_calls,
        sum(nvl(INC_NB_CALLS,0)) as INC_NB_CALLS,
        sum(nvl(MA_SMS_SVA,0)) as MA_SMS_SVA,
        sum(nvl(MA_VOICE_SVA,0)) as MA_VOICE_SVA
        FROM
        mon.SPARK_FT_CBM_CUST_INSIGTH_DAILY
        WHERE PERIOD ='###SLICE_VALUE###'
        group by '6'||SUBSTR(MSISDN, -8)
) A
full join
(
    select msisdn,
    sum(bdle_cost*coeff_onnet/100) as bdles_onet,
    sum(bdle_cost*coeff_offnet/100) as bdles_ofnet,
    sum(bdle_cost*coeff_inter/100) as bdles_inter,
    sum(bdle_cost*coeff_data/100) as bdles_data,
    sum(BDLE_COST*coeff_roaming_voix/100) as bdles_roaming_voix,
    sum(BDLE_COST*coeff_roaming_data/100) as bdles_roaming_data,
    sum(case when coeff_data>=20 and coeff_onnet+coeff_offnet+coeff_inter>=20 then bdle_cost*coeff_data/100 end) as combo_data,
    sum(case when coeff_data>=20 and coeff_onnet+coeff_offnet+coeff_inter>=20
    then (bdle_cost*coeff_onnet+bdle_cost*coeff_offnet+bdle_cost*coeff_inter)/100 end) as combo_voix
    from
    (
       select MSISDN_customer as msisdn, upper(bundle_name) as BDLE_NAME,
       sum(RECHARGE_AMOUNT) as BDLE_COST, count(*) as NBER_PURCHASE
        from (
            select
            sub_msisdn MSISDN_customer,
            offer_name Bundle_Name,
            CHANNEL,
            offer_type Bundle_Type,
            sum(RECHARGE_AMOUNT) RECHARGE_AMOUNT ,
            sum(RETAILER_COMMISSION) RETAILER_COMMISSION,
            count(*) transaction_count
            FROM MON.SPARK_FT_VAS_RETAILLER_IRIS
            where sdate  ='###SLICE_VALUE###'
            and PRETUPS_STATUSCODE = '200'
        group by  ret_msisdn, sub_msisdn, offer_name, offer_type, CHANNEL
        ) T
        where bundle_type not in ('TOPUP')
        group by MSISDN_customer, upper(bundle_name)
union all
select
  MSISDN, upper(bdle_name) as bdle_name,
        sum(bdle_cost) as bdle_cost, sum(NBER_PURCHASE) as NBER_PURCHASE
from mon.SPARK_FT_CBM_BUNDLE_SUBS_DAILY  WHERE PERIOD ='###SLICE_VALUE###'
group by msisdn, upper(bdle_name)
) A left join
    (select
        bdle_name,
        CAST(prix as double) prix,
        CAST(coeff_onnet as int) coeff_onnet,
        CAST(coeff_offnet  as int) coeff_offnet,
        CAST(coeff_inter  as int) coeff_inter,
        CAST(coeff_data  as int) coeff_data,
        CAST(coeff_roaming_voix  as int) coeff_roaming_voix,
        CAST(coeff_roaming_data  as int) coeff_roaming_data,
        validite,
        type_forfait,
        destination,
        type_ocm,
        offre,
        offer,
        offer_1,
        offer_2
     from  DIM.DT_CBM_REF_SOUSCRIPTION_PRICE) B on A.bdle_name=B.bdle_name
    GROUP BY  msisdn
    )B on nvl(A.msisdn, B.msisdn)=B.msisdn

full join
(
SELECT msisdn,
sum(case when service in (9,13,28,29,33,37) and ACCT_RES_NAME='MAIN BALANCE' then (
case when MONTANT<0 then -1*MONTANT else MONTANT end ) else 0 end) as MONTANT,
sum(case when service in (15,26) and ACCT_RES_NAME='MAIN BALANCE' then (
case when MONTANT<0 then -1*MONTANT else MONTANT end ) else 0 end) as PARRAIN
FROM
(
    SELECT
        A.CREATE_DATE CREATE_DATE,
        A.MSISDN MSISDN,
        A.SERVICE SERVICE,
        C.ACCT_RES_NAME ACCT_RES_NAME,
    CASE WHEN C.ACCT_RES_RATING_UNIT = 'QM' THEN -A.CHARGE/100 ELSE -A.CHARGE END AS MONTANT
    FROM
    (
    SELECT
        CREATE_DATE,
        '6'||SUBSTR(ACC_NBR, -8)  AS MSISDN,
        CHANNEL_ID SERVICE,
        ACCT_RES_CODE,
        COUNT(1) NOMBRE_TRANSACTION,
        SUM(NVL(CHARGE,0)) CHARGE
    FROM CDR.SPARK_IT_ZTE_ADJUSTMENT
    WHERE CREATE_DATE ='###SLICE_VALUE###'

    GROUP BY CREATE_DATE,  '6'||SUBSTR(ACC_NBR, -8), CHANNEL_ID, ACCT_RES_CODE
    ) A
    LEFT JOIN
     (
    select *
    from DIM.SPARK_DT_ZTE_USAGE_TYPE
     ) B ON A.SERVICE = B.USAGE_CODE
    LEFT JOIN
     (
        SELECT DISTINCT
        ACCT_RES_ID,
        UPPER(ACCT_RES_NAME) ACCT_RES_NAME,
        ACCT_RES_RATING_TYPE,
        ACCT_RES_RATING_UNIT
        FROM DIM.SPARK_DT_BALANCE_TYPE_ITEM
     ) C ON A.ACCT_RES_CODE = C.ACCT_RES_ID
) T
    group by msisdn
    having  sum(case when service in (9,13,28,29,33,37) and ACCT_RES_NAME='MAIN BALANCE' then (case when MONTANT<0 then -1*MONTANT else MONTANT end ) else 0 end)> 0
    and
    sum(case when service in (15,26) and ACCT_RES_NAME='MAIN BALANCE' then (case when MONTANT<0 then -1*MONTANT else MONTANT end ) else 0 end)> 0

) C on  NVL(NVL(A.msisdn,B.msisdn),C.msisdn) =C.msisdn
full join
(
    select  '6'||SUBSTR(sub_msisdn, -8)  AS MSISDN,
    sum(RECHARGE_AMOUNT) as VAS_AMT, count(*) as vas_nb
    from MON.SPARK_FT_VAS_RETAILLER_IRIS where to_date(sdate) ='###SLICE_VALUE###'
    and offer_type not in ('TOPUP') and PRETUPS_STATUSCODE = '200'
    group by  '6'||SUBSTR(sub_msisdn, -8)
) D on  NVL(NVL(NVL(A.msisdn,B.msisdn),C.msisdn),D.msisdn) =D.msisdn
full join
(
    SELECT
    REFILL_DATE AS  PERIOD,
    RECEIVER_MSISDN AS MSISDN,
    SUM(REFILL_AMOUNT) AS REF_AMT,
    SUM(1) REF_NB,
    REFILL_MEAN AS TYPE
    FROM MON.SPARK_FT_REFILL
    WHERE REFILL_DATE =  '###SLICE_VALUE###'
    AND TERMINATION_IND='200'
    AND REFILL_MEAN = 'C2S'
    GROUP BY REFILL_DATE
    ,RECEIVER_MSISDN
    ,REFILL_MEAN
) E on NVL(NVL(NVL(NVL(A.msisdn,B.msisdn),C.msisdn),D.msisdn ),E.msisdn) =E.msisdn
full join
(
    select
    '6'||SUBSTR(MSISDN, -8) AS MSISDN,
    round(sum(nbytest)/(1024*1024),2) as volume_data,
    round(sum(case when appli_type= 'Chat' then NBYTEST else 0 end)/(1024*1024),2) as volume_chat,
    round(sum(case when appli_type= 'VoIP' then NBYTEST else 0 end)/(1024*1024),2) as volume_voip,
    round(sum(case when RADIO_ACCESS_TECHNO='LTE' then NBYTEST else 0 end)/(1024*1024),2) as  volume_4G,
    round(sum(case when RADIO_ACCESS_TECHNO='3G' then NBYTEST else 0 end)/(1024*1024),2) as  volume_3G,
    round(sum(case when RADIO_ACCESS_TECHNO='2G' then NBYTEST else 0 end)/(1024*1024),2) as  volume_2G,
    round(sum(case when appli_type= 'Chat' then NBYTEST else 0 end)/(1024*1024) * 0.75 +
    sum(case when appli_type= 'VoIP' then NBYTEST else 0 end)/(1024*1024) * 0.25,2) as volume_ott,
    case when
    sum(case when appli_type= 'Chat' then NBYTEST else 0 end)/(1024*1024) * 0.75 +
    sum(case when appli_type= 'VoIP' then NBYTEST else 0 end)/(1024*1024) * 0.25  >=4
    then 1 else 0 end as ott_user
    FROM MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
    WHERE TRANSACTION_DATE='###SLICE_VALUE###'
    group by  '6'||SUBSTR(MSISDN, -8)
)F on  NVL(NVL(NVL(NVL(NVL(A.msisdn,B.msisdn),C.msisdn),D.msisdn ),E.msisdn),F.msisdn)=F.msisdn
