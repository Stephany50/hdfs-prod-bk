INSERT INTO MON.SPARK_FT_CBM_ARPU_MOU
SELECT 
    A.msisdn MSISDN,
    bdles_onet + MA_VOICE_ONNET + bdles_ofnet + MA_VOICE_OFNET + bdles_inter + MA_VOICE_INTER + bdles_data + MA_DATA arpu,
    bdles_onet + MA_VOICE_ONNET + bdles_ofnet + MA_VOICE_OFNET + bdles_inter + MA_VOICE_INTER arpu_voix,
    bdles_onet + MA_VOICE_ONNET arpu_onet,
    bdles_ofnet + MA_VOICE_OFNET arpu_ofnet,
    bdles_inter + MA_VOICE_INTER arpu_inter,
    bdles_data + MA_DATA arpu_data,
    null arpu_VAS,
    bdles_roaming_voix + MA_VOICE_ROAMING arpu_roaming_voix, -- To be confirmed
    bdles_roaming_voix + MA_VOICE_ROAMING + bdles_roaming_data arpu_roaming,
    combo_data COMBO_DATA,
    combo_voix combo_voix,
    vas_amt VAS_AMT,
    vas_nb VAS_NB,
    MA_VOICE_ONNET + MA_VOICE_OFNET + MA_VOICE_INTER PAYG_VOIX,
    MA_VOICE_ONNET PAYG_VOIX_onnet,
    MA_VOICE_OFNET PAYG_VOIX_offnet,
    MA_VOICE_INTER PAYG_VOIX_inter,
    MA_VOICE_ROAMING PAYG_VOIX_roaming,
    MOU_ONNET MOU_ONNET,
    MOU_OFNET MOU_OFNET,
    MOU_INTER MOU_INTER,
    MOU_ONNET + MOU_OFNET + MOU_INTER MOU,
    bdles_onet + bdles_ofnet + bdles_inter bdles_voix,
    bdles_onet bdles_onet,
    bdles_ofnet bdles_ofnet,
    bdles_inter bdles_inter,
    bdles_data bdles_data,
    bdles_roaming_voix bdles_roaming_voix,
    bdles_roaming_data bdles_roaming_data,
    null Parrain,
    MA_DATA PAYG_DATA,
    nb_calls nb_calls,
    ref_amt REF_AMT,
    ref_nb REF_NB,
    INC_NB_CALLS INC_NB_CALLS,
    volume_data volume_data,
    volume_chat volume_chat,
    volume_voip volume_voip,
    volume_ott volume_ott,
    ott_user ott_user,
    (
        CASE WHEN ville ='DOUALA' THEN region = 'a. DOUALA'
             WHEN ville ='YAOUNDE' then region = 'b. YAOUNDE'
             WHEN region='LITTORAL' then region = 'c. LITTORAL'
             WHEN region = 'CENTRE' then region='d. CENTRE'
             WHEN region='EXTREME-NORD' then region='e. EXTREME-NORD'
             WHEN region='NORD' then region='f. NORD'
             WHEN region='ADAMAOUA' then region='g. ADAMAOUA'
             WHEN region='NORD-OUEST' then region='h. NORD-OUEST'
             WHEN region='SUD-OUEST' then region = 'i. SUD-OUEST'
             WHEN region = 'OUEST' then region='j. OUEST'
             WHEN region='SUD' then region='k. SUD'
            WHEN region='EST' then region='l. EST'
            ELSE null
            end
    ) REGION,
    ACTIVATION_DATE date_activation,
    NULL multi,
    NULL Segment_valeur,
    inserted_sources SOURCE,
    CURRENT_TIMESTAMP() INSERT_DATE,
    '###SLICE_VALUE###' EVENT_DATE
FROM
(
    SELECT
        concat_ws(':' , collect_set(source)) inserted_sources
    FROM
    (
        SELECT case when count(*)>1 then 'FT_CBM_CUST_INSIGTH_DAILY' else '' end source
        FROM mon.SPARK_FT_CBM_CUST_INSIGTH_DAILY
        WHERE PERIOD ='###SLICE_VALUE###'
        union
        SELECT case when count(*)>1 then 'FT_CBM_BUNDLE_SUBS_DAILY' else '' end source
        from mon.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
        WHERE PERIOD ='###SLICE_VALUE###'
        union
        SELECT case when count(*)>1 then 'FT_VAS_RETAILLER_IRIS' else '' end source
        FROM MON.SPARK_FT_VAS_RETAILLER_IRIS
        where sdate  ='###SLICE_VALUE###'
        union
        SELECT case when count(*)>1 then 'FT_REFILL' else '' end source
        FROM MON.SPARK_FT_REFILL
        WHERE REFILL_DATE =  '###SLICE_VALUE###'
        union
        SELECT case when count(*)>1 then 'FT_OTARIE_DATA_TRAFFIC_DAY' else '' end source
        FROM MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
        WHERE TRANSACTION_DATE='###SLICE_VALUE###'
    ) X
) Z,
(
    SELECT
        DISTINCT '6'||SUBSTR(MSISDN, -8) AS MSISDN
    FROM mon.SPARK_FT_CBM_CUST_INSIGTH_DAILY
    WHERE PERIOD ='###SLICE_VALUE###'
    UNION
    select
        distinct '6'||SUBSTR(sub_msisdn, -8) as MSISDN
    FROM MON.SPARK_FT_VAS_RETAILLER_IRIS
    where sdate  ='###SLICE_VALUE###'
        and PRETUPS_STATUSCODE = '200'
        and upper(offer_type) not in ('TOPUP')
    union
    select
        distinct '6'||SUBSTR(MSISDN, -8) as MSISDN
    from mon.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
    WHERE PERIOD ='###SLICE_VALUE###'
    UNION
    SELECT DISTINCT '6'||SUBSTR(RECEIVER_MSISDN, -8) AS MSISDN
    FROM MON.SPARK_FT_REFILL
    WHERE REFILL_DATE =  '###SLICE_VALUE###'
        AND TERMINATION_IND='200'
        AND REFILL_MEAN = 'C2S'
    UNION
    select DISTINCT MSISDN
    FROM MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
    WHERE TRANSACTION_DATE='###SLICE_VALUE###'
) A -- Distinct msisdns from both cust_insight_daily and bundle_subs_daily
LEFT JOIN
(
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
    FROM mon.SPARK_FT_CBM_CUST_INSIGTH_DAILY
    WHERE PERIOD ='###SLICE_VALUE###'
    group by '6'||SUBSTR(MSISDN, -8)
) B -- CBM_CUST_INSIGTH_DAILY
ON A.MSISDN=B.MSISDN
LEFT JOIN
(
    select
        '6'||SUBSTR(MSISDN, -8) AS MSISDN ,
        sum(nvl(bdle_cost*coeff_onnet/100, 0)) as bdles_onet,
        sum(nvl(bdle_cost*coeff_offnet/100, 0)) as bdles_ofnet,
        sum(nvl(bdle_cost*coeff_inter/100, 0)) as bdles_inter,
        sum(nvl(bdle_cost*coeff_data/100, 0)) as bdles_data,
        sum(nvl(BDLE_COST*coeff_roaming_voix/100, 0)) as bdles_roaming_voix,
        sum(nvl(BDLE_COST*coeff_roaming_data/100, 0)) as bdles_roaming_data,
        sum(case when coeff_data>=20 and coeff_data<=80 then nvl(bdle_cost*coeff_data/100, 0) end) as combo_data,
        sum(case when coeff_data>=20 and coeff_data<=80 then nvl((bdle_cost*coeff_onnet+bdle_cost*coeff_offnet+bdle_cost*coeff_inter)/100, 0) end) as combo_voix
    from
    (
        select
            '6'||SUBSTR(MSISDN_customer, -8) AS MSISDN , upper(bundle_name) as BDLE_NAME,
            sum(RECHARGE_AMOUNT) as BDLE_COST, count(*) as NBER_PURCHASE
        from
        (
            select
                '6'||SUBSTR(sub_msisdn, -8) AS MSISDN_customer,
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
            '6'||SUBSTR(MSISDN, -8) AS MSISDN,
            upper(bdle_name) as bdle_name,
            sum(bdle_cost) as bdle_cost,
            sum(NBER_PURCHASE) as NBER_PURCHASE
        from mon.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
        WHERE PERIOD ='###SLICE_VALUE###'
        group by msisdn, upper(bdle_name)
    ) A left join
    (
        select
            *
        from  DIM.DT_CBM_REF_SOUSCRIPTION_PRICE
    ) B
    on UPPER(A.bdle_name)=UPPER(B.bdle_name)
    GROUP BY  msisdn
) C -- CBM_BUNDLE_SUBS_DAILY UNION VAS_RETAILER_IRIE
ON A.MSISDN=C.MSISDN
LEFT JOIN
(
    SELECT
        '6'||SUBSTR(ACCESS_KEY, -8) MSISDN,
        MAX(ACTIVATION_DATE) ACTIVATION_DATE
    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
    WHERE EVENT_DATE='###SLICE_VALUE###'
    GROUP BY '6'||SUBSTR(ACCESS_KEY, -8)
) D -- Contract Snapshot
ON A.MSISDN=D.MSISDN
LEFT JOIN
(
    SELECT
        msisdn,
        max(upper(administrative_region)) REGION,
        max(upper(townname)) VILLE
    FROM mon.spark_ft_client_last_site_day
    WHERE event_date='###SLICE_VALUE###'
    group by msisdn
) E -- Client_last_site_day
ON A.MSISDN=E.MSISDN
LEFT JOIN
(
    select
        '6'||SUBSTR(sub_msisdn, -8) MSISDN,
        sum(RECHARGE_AMOUNT) VAS_AMT,
         count(*)  vas_nb
    from MON.SPARK_FT_VAS_RETAILLER_IRIS where to_date(sdate) ='###SLICE_VALUE###'
        and offer_type not in ('TOPUP') and PRETUPS_STATUSCODE = '200'
    group by  '6'||SUBSTR(sub_msisdn, -8)
) F -- VAS_RETAILLER_IRIS
ON A.msisdn=F.msisdn
LEFT JOIN
(
    SELECT
        REFILL_DATE AS  PERIOD,
        '6'||SUBSTR(RECEIVER_MSISDN, -8) AS MSISDN,
        SUM(REFILL_AMOUNT) AS REF_AMT,
        SUM(1) REF_NB,
        REFILL_MEAN AS TYPE
    FROM MON.SPARK_FT_REFILL
    WHERE REFILL_DATE =  '###SLICE_VALUE###'
        AND TERMINATION_IND='200'
        AND REFILL_MEAN = 'C2S'
    GROUP BY REFILL_DATE, '6'||SUBSTR(RECEIVER_MSISDN, -8), REFILL_MEAN
) G
ON A.msisdn=G.msisdn
LEFT JOIN
(
    select
        '6'||SUBSTR(MSISDN, -8) AS  MSISDN,
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
                sum(case when appli_type= 'VoIP' then NBYTEST else 0 end)/(1024*1024) * 0.25  >= 4
            then 1
            else 0
            end as ott_user
    FROM MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
    WHERE TRANSACTION_DATE='###SLICE_VALUE###'
    group by  '6'||SUBSTR(MSISDN, -8)
) H -- OTARIE_DATA_TRAFFIC_DAY
on A.msisdn=H.msisdn
