INSERT INTO MON.SPARK_FT_CBM_ARPU_MOU
SELECT
    A.msisdn MSISDN,
    nvl(bdles_onet, 0) + nvl(MA_VOICE_ONNET, 0) + nvl(MA_SMS_ONNET, 0) + nvl(bdles_ofnet, 0) + nvl(MA_VOICE_OFNET, 0) + nvl(MA_SMS_OFNET, 0) + nvl(bdles_inter, 0) + nvl(MA_VOICE_INTER, 0) + nvl(MA_SMS_INTER, 0) + nvl(bdles_data, 0) + nvl(MA_DATA, 0) + NVL(MA_VAS, 0) + NVL(MA_GOS_SVA, 0) arpu,
    nvl(bdles_onet, 0) + nvl(MA_VOICE_ONNET, 0) + nvl(MA_SMS_ONNET, 0) + nvl(bdles_ofnet, 0) + nvl(MA_VOICE_OFNET, 0) + nvl(MA_SMS_OFNET, 0) + nvl(bdles_inter, 0) + nvl(MA_VOICE_INTER, 0) + nvl(MA_SMS_INTER, 0) arpu_voix,
    nvl(bdles_onet, 0) + nvl(MA_VOICE_ONNET, 0) + nvl(MA_SMS_ONNET, 0) arpu_onet,
    nvl(bdles_ofnet, 0) + nvl(MA_VOICE_OFNET, 0) + nvl(MA_SMS_OFNET, 0)  arpu_ofnet,
    nvl(bdles_inter, 0) + nvl(MA_VOICE_INTER, 0) + nvl(MA_SMS_INTER, 0) arpu_inter,
    nvl(bdles_data, 0) + nvl(MA_DATA, 0) arpu_data,
    null arpu_VAS,
    nvl(bdles_roaming_voix, 0) + nvl(MA_VOICE_ROAMING, 0) + nvl(MA_SMS_ROAMING, 0) arpu_roaming_voix, -- To be confirmed
    nvl(bdles_roaming_voix, 0) + nvl(MA_VOICE_ROAMING, 0) + nvl(MA_SMS_ROAMING, 0) + nvl(bdles_roaming_data, 0) arpu_roaming,
    nvl(combo_data, 0) COMBO_DATA,
    nvl(combo_voix, 0) combo_voix,
    nvl(vas_amt, 0) VAS_AMT,
    nvl(vas_nb, 0) VAS_NB,
    nvl(MA_VOICE_ONNET, 0) + nvl(MA_SMS_ONNET, 0) + nvl(MA_VOICE_OFNET, 0) + nvl(MA_SMS_OFNET, 0) + nvl(MA_VOICE_INTER, 0) + nvl(MA_SMS_INTER, 0) PAYG_VOIX,
    nvl(MA_VOICE_ONNET, 0) PAYG_VOIX_onnet,
    nvl(MA_VOICE_OFNET, 0) PAYG_VOIX_offnet,
    nvl(MA_VOICE_INTER, 0) PAYG_VOIX_inter,
    nvl(MA_VOICE_ROAMING, 0) PAYG_VOIX_roaming,
    nvl(MOU_ONNET, 0) MOU_ONNET,
    nvl(MOU_OFNET, 0) MOU_OFNET,
    nvl(MOU_INTER, 0) MOU_INTER,
    nvl(MOU_ONNET, 0) + nvl(MOU_OFNET, 0) + nvl(MOU_INTER, 0) MOU,
    nvl(MA_VOICE_ONNET, 0) + nvl(MA_VOICE_OFNET, 0) + nvl(MA_VOICE_INTER, 0) + nvl(MA_VOICE_ROAMING, 0) + nvl(MA_VOICE_SVA, 0) PAYGO_WEBI,
    nvl(MA_SMS_ONNET, 0) + nvl(MA_SMS_OFNET, 0) + nvl(MA_SMS_INTER, 0) + nvl(MA_SMS_ROAMING, 0) + nvl(MA_SMS_SVA, 0) SMS_WEBI,
    NULL ARPU2,
    nvl(bdles_onet, 0) + nvl(bdles_ofnet, 0) + nvl(bdles_inter, 0) bdles_voix,
    nvl(bdles_onet, 0) bdles_onet,
    nvl(bdles_ofnet, 0) bdles_ofnet,
    nvl(bdles_inter, 0) bdles_inter,
    nvl(bdles_data, 0) bdles_data,
    nvl(bdles_roaming_voix, 0) bdles_roaming_voix,
    nvl(bdles_roaming_data, 0) bdles_roaming_data,
    null Parrain,
    nvl(MA_DATA, 0) PAYG_DATA,
    nvl(nb_calls, 0) nb_calls,
    ref_amt REF_AMT,
    ref_nb REF_NB,
    INC_NB_CALLS INC_NB_CALLS,
    nvl(volume_data, 0) volume_data,
    nvl(volume_chat, 0) volume_chat,
    nvl(volume_voip, 0) volume_voip,
    nvl(volume_ott, 0) volume_ott,
    ott_user ott_user,
    (
        CASE WHEN ville ='DOUALA' THEN 'a. DOUALA'
             WHEN ville ='YAOUNDE' then 'b. YAOUNDE'
             WHEN region='LITTORAL' then 'c. LITTORAL'
             WHEN region = 'CENTRE' then 'd. CENTRE'
             WHEN region='EXTREME-NORD' then 'e. EXTREME-NORD'
             WHEN region='NORD' then 'f. NORD'
             WHEN region='ADAMAOUA' then 'g. ADAMAOUA'
             WHEN region='NORD-OUEST' then 'h. NORD-OUEST'
             WHEN region='SUD-OUEST' then 'i. SUD-OUEST'
             WHEN region = 'OUEST' then 'j. OUEST'
             WHEN region='SUD' then 'k. SUD'
            WHEN region='EST' then 'l. EST'
            ELSE null
            end
    ) REGION,
    ACTIVATION_DATE date_activation,
    NULL multi,
    NULL Segment_valeur,
    inserted_sources SOURCE,
    CURRENT_TIMESTAMP() INSERT_DATE,
    nvl(BYTES_DATA, 0) BYTES_DATA,
    nvl(FOU_SMS, 0) FOU_SMS,
    VOLUME_4G,
    VOLUME_3G,
    VOLUME_2G,
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
    WHERE PERIOD ='###SLICE_VALUE###' and bdle_name != ' '
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
        sum(nvl(MA_VOICE_SVA,0)) as MA_VOICE_SVA,

        sum(nvl(BYTES_DATA, 0)) as BYTES_DATA,
        sum(nvl(FOU_SMS, 0)) as FOU_SMS
    FROM mon.SPARK_FT_CBM_CUST_INSIGTH_DAILY
    WHERE PERIOD ='###SLICE_VALUE###'
    group by '6'||SUBSTR(MSISDN, -8)
) B -- CBM_CUST_INSIGTH_DAILY  OK
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
        sum(case when coeff_data>=20 and coeff_data<=80 then (nvl(bdle_cost*coeff_onnet, 0)+nvl(bdle_cost*coeff_offnet, 0)+nvl(bdle_cost*coeff_inter, 0))/100 end) as combo_voix
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
                -- and upper(offer_type) not in ('TOPUP')
            group by  ret_msisdn, sub_msisdn, offer_name, offer_type, CHANNEL
        ) T
        where upper(bundle_type) not in ('TOPUP')
        group by MSISDN_customer, upper(bundle_name)
        union all
        select
            '6'||SUBSTR(MSISDN, -8) AS MSISDN,
            upper(bdle_name) as bdle_name,
            sum(nvl(bdle_cost, 0)) as bdle_cost,
            sum(nvl(NBER_PURCHASE, 0)) as NBER_PURCHASE
        from mon.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
        WHERE PERIOD ='###SLICE_VALUE###' and bdle_name != ' '
        group by msisdn, upper(bdle_name)
    ) A left join
    (
        select
            UPPER(TRIM(BDLE_NAME)) BDLE_NAME, nvl(max(coeff_onnet), 0) coeff_onnet, nvl(max(coeff_offnet), 0) coeff_offnet, nvl(max(coeff_inter), 0) coeff_inter, nvl(max(coeff_data), 0) coeff_data, nvl(max(coeff_roaming_data), 0) coeff_roaming_data, nvl(max(coeff_roaming_voix), 0) coeff_roaming_voix
        from  DIM.SPARK_DT_CBM_REF_SOUSCRIPTION_PRICE
        GROUP BY UPPER(TRIM(BDLE_NAME))
    ) B
    on UPPER(trim(A.bdle_name))=UPPER(trim(B.bdle_name))
    GROUP BY  msisdn
) C -- CBM_BUNDLE_SUBS_DAILY UNION VAS_RETAILER_IRIS
ON A.MSISDN=C.MSISDN
LEFT JOIN
(
    SELECT
        '6'||SUBSTR(ACCESS_KEY, -8) MSISDN,
        MAX(ACTIVATION_DATE) ACTIVATION_DATE
    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
    WHERE EVENT_DATE='###SLICE_VALUE###'
    GROUP BY '6'||SUBSTR(ACCESS_KEY, -8)
) D -- Contract Snapshot OK
ON A.MSISDN=D.MSISDN
LEFT JOIN
(
    select
        a1.msisdn msisdn,
        NVL(max(upper(b1.administrative_region)), max(upper(a1.administrative_region))) REGION,
        NVL(max(upper(b1.townname)), max(upper(a1.townname))) VILLE
    from (
            select
                *
            from mon.spark_ft_client_last_site_day
            where event_date in (
                select
                    max (event_date)
                from  mon.spark_ft_client_last_site_day
                where event_date between
                date_sub('###SLICE_VALUE###',3) and '###SLICE_VALUE###'
                )
        ) a1
    left join (
        select
            *
        from mon.spark_ft_client_site_traffic_day
        where event_date in (
            select max (event_date)
            from mon.spark_ft_client_site_traffic_day
            where event_date between date_sub('###SLICE_VALUE###',3) and '###SLICE_VALUE###' )
    ) b1 on a1.msisdn = b1.msisdn
    group by a1.msisdn
) E -- Client_last_site_day and Client_site_traffic_day
ON A.MSISDN=E.MSISDN
-- LEFT JOIN
-- (
--     SELECT
--         msisdn,
--         max(upper(administrative_region)) REGION,
--         max(upper(townname)) VILLE
--     FROM mon.spark_ft_client_last_site_day
--     WHERE event_date='###SLICE_VALUE###'
--     group by msisdn
-- ) E -- Client_last_site_day OK
-- ON A.MSISDN=E.MSISDN
LEFT JOIN
(
    select
        '6'||SUBSTR(sub_msisdn, -8) MSISDN,
        sum(RECHARGE_AMOUNT) VAS_AMT,
         count(*)  vas_nb
    from MON.SPARK_FT_VAS_RETAILLER_IRIS where to_date(sdate) ='###SLICE_VALUE###'
        and offer_type not in ('TOPUP') and PRETUPS_STATUSCODE = '200'
    group by  '6'||SUBSTR(sub_msisdn, -8)
) F -- VAS_RETAILLER_IRIS OK
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
) G -- FT_REFILL OK
ON A.msisdn=G.msisdn
LEFT JOIN
(
    select
        '6'||SUBSTR(MSISDN, -8) AS  MSISDN,
        round(sum(nbytest)/(1024*1024),2) as volume_data,
        round(sum(case when appli_type= 'Chat' then NBYTEST else 0 end)/(1024*1024),2) as volume_chat,
        round(sum(case when appli_type= 'VoIP' then NBYTEST else 0 end)/(1024*1024),2) as volume_voip,
        round(sum(case when RADIO_ACCESS_TECHNO='LTE' then NBYTEST else 0 end)/(1024*1024),2) as  volume_4G,
        round(sum(case when RADIO_ACCESS_TECHNO IN ("3G","HSPA") then NBYTEST else 0 end)/(1024*1024),2) as  volume_3G,
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
) H -- OTARIE_DATA_TRAFFIC_DAY OK
on A.msisdn=H.msisdn