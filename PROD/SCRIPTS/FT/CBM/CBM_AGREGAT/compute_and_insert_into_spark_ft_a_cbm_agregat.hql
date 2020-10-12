INSERT INTO AGG.SPARK_FT_A_CBM_AGREGAT
select
A.SITE_NAME,
A.MULTI,
A.OPERATEUR,
A.TENURE,
A.SEGMENT,
sum(MOU_ONNET) as MOU_ONNET,
sum(MOU_OFNET) as MOU_OFNET,
sum(MOU_INTER) as MOU_INTER,
sum(MA_VOICE_ONNET) as MA_VOICE_ONNET,
sum(MA_VOICE_OFNET) as MA_VOICE_OFNET,
sum(MA_VOICE_INTER) as MA_VOICE_INTER,
sum(MA_SMS_OFNET) as MA_SMS_OFNET,
sum(MA_SMS_INTER) as MA_SMS_INTER,
sum(MA_SMS_ONNET) as MA_SMS_ONNET,
sum(MA_DATA) as MA_DATA,
sum(MA_VAS) as MA_VAS,
sum(MA_GOS_SVA) as MA_GOS_SVA,
sum(MA_VOICE_ROAMING) as MA_VOICE_ROAMING,
sum(MA_SMS_ROAMING) as MA_SMS_ROAMING,
sum(MA_SMS_SVA) as MA_SMS_SVA,
sum(MA_VOICE_SVA) as MA_VOICE_SVA,
sum(USER_VOICE) as USER_VOICE,
NVL(SUBS.BDLE_NAME, RETAIL.BDLE_NAME) BDLE_NAME,
sum(BDLE_COST_RETAIL) as BDLE_COST_RETAIL,
sum(BDLE_COST_SUBS) as BDLE_COST_SUBS,
sum(VOLUME_DATA) as VOLUME_DATA,
sum(USER_DATA1) as USER_DATA1,
sum(USER_DATA2) as USER_DATA2,
sum(MONTANT) as MONTANT_VAS,
NULL DESTINATION_TYPE,
NULL DESTINATION,
NULL TRAFFIC_DATA,
NULL RATED_SMS_TOTAL_COUNT,
NULL IN_DURATION,
NULL RATED_DURATION,
NULL LOC_SITE_NAME,
sum(ACTIF_90) as ACTIF_90,
sum(INACTIF_90) as INACTIF_90,
sum(GROSS_ADD) as GROSS_ADD,
CURRENT_TIMESTAMP() INSERT_DATE,
A.ZONE ZONE,
A.EVENT_DATE EVENT_DATE

FROM
(
    select EVENT_DATE, SITE_NAME,  MULTI,  OPERATEUR,  TENURE,  SEGMENT,  ZONE
    from
    (
        SELECT
            PERIOD EVENT_DATE,
            SITE_NAME,
            MULTI,
            OPERATEUR,
            TENURE,
            SEGMENT,
            ZONE,
            sum(nvl(MOU_ONNET,0)) MOU_ONNET ,
            sum(nvl(MOU_OFNET,0)) MOU_OFNET,
            sum(nvl(MOU_INTER,0)) MOU_INTER,
            sum(nvl(MA_VOICE_ONNET,0)) MA_VOICE_ONNET,
            sum(nvl(MA_VOICE_OFNET,0)) MA_VOICE_OFNET,
            sum(nvl(MA_VOICE_INTER,0)) MA_VOICE_INTER,
            sum(nvl(MA_SMS_OFNET,0)) MA_SMS_OFNET,
            sum(nvl(MA_SMS_INTER,0)) MA_SMS_INTER,
            sum(nvl(MA_SMS_ONNET,0)) MA_SMS_ONNET,
            sum(nvl(MA_DATA,0)) MA_DATA,
            sum(nvl(MA_VAS,0)) MA_VAS,
            sum(nvl(MA_GOS_SVA,0)) MA_GOS_SVA,
            sum(nvl(MA_VOICE_ROAMING,0)) MA_VOICE_ROAMING,
            sum(nvl(MA_SMS_ROAMING,0)) MA_SMS_ROAMING,
            sum(nvl(MA_SMS_SVA,0)) MA_SMS_SVA,
            sum(nvl(MA_VOICE_SVA,0)) MA_VOICE_SVA,
            sum(is_user_voice) user_voice
        FROM
        (
            select
                *,
                case when (MOU_ONNET+MOU_OFNET+MOU_INTER) > 0 then 1 else 0 end as is_user_voice
                from mon.SPARK_FT_CBM_CUST_INSIGTH_DAILY
            WHERE PERIOD = '###SLICE_VALUE###'
        ) AA
        LEFT JOIN
        (
            SELECT
                EVENT_DATE,
                msisdn,
                site_name
            FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE='###SLICE_VALUE###'
        ) BB on AA.msisdn=BB.msisdn and AA.PERIOD=BB.EVENT_DATE

        left join

        ( SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT, zone FROM dim.dt_ref_cbm ) CC on AA.msisdn=CC.msisdn

        group by AA.period, BB.site_name, multi, operateur, tenure, segment, zone
    ) CI
    union
    select EVENT_DATE, SITE_NAME,  MULTI,  OPERATEUR,  TENURE,  SEGMENT,  ZONE from
    (
        SELECT period EVENT_DATE, site_name, bdle_name, multi, operateur, tenure, segment, zone,sum(nvl(BDLE_COST,0)) as bdle_cost_subs
        FROM
        (select * from mon.spark_FT_CBM_BUNDLE_SUBS_DAILY
        WHERE PERIOD = '###SLICE_VALUE###') AAA

        left join

        (SELECT EVENT_DATE, msisdn, site_name FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE
        ='###SLICE_VALUE###') BBB on AAA.msisdn=BBB.msisdn and AAA.PERIOD=BBB.EVENT_DATE

        left join

        (SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT, zone FROM dim.dt_ref_cbm) CCC on AAA.msisdn=CCC.msisdn

        group by AAA.period, BBB.site_name, bdle_name, multi, operateur, tenure, segment, zone
    ) SUBS
    union
    select EVENT_DATE, SITE_NAME,  MULTI,  OPERATEUR,  TENURE,  SEGMENT,  ZONE from
    (
        SELECT period EVENT_DATE, site_name, bdle_name, multi, operateur, tenure, segment, zone,sum(nvl(BDLE_COST,0)) as bdle_cost_retail

        FROM
        (select Sdate as period, sub_msisdn as msisdn, offer_name as bdle_name,
        sum(RECHARGE_AMOUNT) as BDLE_COST
        from MON.SPARK_FT_VAS_RETAILLER_IRIS
        where upper(offer_type) not in ('TOPUP') and sdate = '###SLICE_VALUE###' and PRETUPS_STATUSCODE = '200'
        group by Sdate, sub_msisdn, offer_name) A

        left join

        (SELECT EVENT_DATE, msisdn, site_name FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE ='###SLICE_VALUE###') B on A.msisdn=B.msisdn and A.PERIOD=B.EVENT_DATE

        left join

        (SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT, ZONE FROM dim.dt_ref_cbm) C on A.msisdn=C.msisdn

        group by A.period, B.site_name, bdle_name, multi, operateur, tenure, segment, zone
    ) RETAIL
    union
    select EVENT_DATE, SITE_NAME,  MULTI,  OPERATEUR,  TENURE,  SEGMENT,  ZONE from
    (
        SELECT  period EVENT_DATE, site_name, multi, operateur, tenure, segment,zone,
        sum(nbytest)/(1024*1024) as volume_data,
        sum(is_user_data1) as user_data1,
        sum(is_user_data2) as user_data2
        FROM
        (select TRANSACTION_DATE as period,
                msisdn, nbytest, case when nbytest/(1024*1024)>1  then 1 else 0 end as is_user_data1,
                case when nbytest/(1024*1024)>5  then 1 else 0 end as is_user_data2  from MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
                WHERE TRANSACTION_DATE = '###SLICE_VALUE###') A
        left join

        (SELECT EVENT_DATE, msisdn, site_name FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE
        ='###SLICE_VALUE###') B on A.msisdn=B.msisdn and A.PERIOD=B.EVENT_DATE

        left join

        (SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT, zone FROM dim.dt_ref_cbm) C on A.msisdn=C.msisdn

        group by A.period, B.site_name, multi, operateur, tenure, segment, zone
    ) traffic_data
    union
    select EVENT_DATE, SITE_NAME,  MULTI,  OPERATEUR,  TENURE,  SEGMENT,  ZONE from
    (
        SELECT CREATE_DATE EVENT_DATE, SITE_NAME, MULTI, OPERATEUR, TENURE, SEGMENT, zone,sum(MONTANT) as MONTANT
        FROM
        (
        SELECT
            A.CREATE_DATE,
            A.MSISDN,
            A.SERVICE,
            CASE WHEN C.ACCT_RES_RATING_UNIT = 'QM' THEN -A.CHARGE/100 ELSE -A.CHARGE END AS MONTANT

        FROM
        (
            SELECT
                CREATE_DATE,
                IF(SUBSTR(TRIM(ACC_NBR),1,3)=237,SUBSTR(TRIM(ACC_NBR),4,9),TRIM(ACC_NBR)) MSISDN,
                CHANNEL_ID SERVICE,
                ACCT_RES_CODE,
                COUNT(1) NOMBRE_TRANSACTION,
                SUM(NVL(CHARGE,0)) CHARGE
            FROM CDR.spark_IT_ZTE_ADJUSTMENT
            WHERE CREATE_DATE = '###SLICE_VALUE###'
            GROUP BY CREATE_DATE, IF(SUBSTR(TRIM(ACC_NBR),1,3)=237,SUBSTR(TRIM(ACC_NBR),4,9),TRIM(ACC_NBR)), CHANNEL_ID, ACCT_RES_CODE
        ) A
        LEFT JOIN DIM.spark_DT_ZTE_USAGE_TYPE B ON A.SERVICE = B.USAGE_CODE
        LEFT JOIN
        (
            SELECT DISTINCT
                ACCT_RES_ID,
                UPPER(ACCT_RES_NAME) ACCT_RES_NAME,
                ACCT_RES_RATING_TYPE,
                ACCT_RES_RATING_UNIT
            FROM DIM.spark_DT_BALANCE_TYPE_ITEM
        ) C ON A.ACCT_RES_CODE = C.ACCT_RES_ID) FI

        LEFT JOIN
        (SELECT EVENT_DATE, msisdn, site_name FROM mon.spark_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE ='###SLICE_VALUE###') EN on FI.msisdn=EN.msisdn and FI.CREATE_DATE=EN.EVENT_DATE

        left join

        (SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT, zone FROM dim.dt_ref_cbm) ES on FI.msisdn=ES.msisdn

        WHERE SERVICE IN (9,13,28,29,33)

        group by FI.CREATE_DATE, EN.SITE_NAME, multi, operateur, tenure, segment, zone
    ) vas
    union
    select EVENT_DATE, SITE_NAME,  MULTI,  OPERATEUR,  TENURE,  SEGMENT,  ZONE from
    (
        select EVENT_DATE, SITE_NAME,  MULTI,  OPERATEUR,  TENURE,  SEGMENT,  ZONE,
        count(distinct case when GRP_GP_STATUS='ACTIF' then A.msisdn else null end) as actif_90,
        count(distinct case when DEACTIVATION_DATE=date_sub(EVENT_DATE,89) then A.msisdn else null end) as inactif_90,
        count(distinct case when ACTIVATION_DATE=EVENT_DATE then A.msisdn else null end) as gross_add
        from (select EVENT_DATE, msisdn, GRP_GP_STATUS, DEACTIVATION_DATE, ACTIVATION_DATE ,
                    LOC_SITE_NAME as SITE_NAME from mon.spark_ft_marketing_datamart
        where EVENT_DATE ='###SLICE_VALUE###') A
        left join (SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT, zone FROM dim.dt_ref_cbm) B on A.msisdn=B.msisdn
        group by A.EVENT_DATE, A.SITE_NAME, multi, operateur, tenure, segment, zone
    )parc
) A
LEFT JOIN
(
    SELECT
        PERIOD EVENT_DATE,
        SITE_NAME,
        MULTI,
        OPERATEUR,
        TENURE,
        SEGMENT,
        ZONE,
        sum(nvl(MOU_ONNET,0)) MOU_ONNET ,
        sum(nvl(MOU_OFNET,0)) MOU_OFNET,
        sum(nvl(MOU_INTER,0)) MOU_INTER,
        sum(nvl(MA_VOICE_ONNET,0)) MA_VOICE_ONNET,
        sum(nvl(MA_VOICE_OFNET,0)) MA_VOICE_OFNET,
        sum(nvl(MA_VOICE_INTER,0)) MA_VOICE_INTER,
        sum(nvl(MA_SMS_OFNET,0)) MA_SMS_OFNET,
        sum(nvl(MA_SMS_INTER,0)) MA_SMS_INTER,
        sum(nvl(MA_SMS_ONNET,0)) MA_SMS_ONNET,
        sum(nvl(MA_DATA,0)) MA_DATA,
        sum(nvl(MA_VAS,0)) MA_VAS,
        sum(nvl(MA_GOS_SVA,0)) MA_GOS_SVA,
        sum(nvl(MA_VOICE_ROAMING,0)) MA_VOICE_ROAMING,
        sum(nvl(MA_SMS_ROAMING,0)) MA_SMS_ROAMING,
        sum(nvl(MA_SMS_SVA,0)) MA_SMS_SVA,
        sum(nvl(MA_VOICE_SVA,0)) MA_VOICE_SVA,
        sum(is_user_voice) user_voice
    FROM
    (
        select
            *,
            case when (MOU_ONNET+MOU_OFNET+MOU_INTER) > 0 then 1 else 0 end as is_user_voice
            from mon.SPARK_FT_CBM_CUST_INSIGTH_DAILY
        WHERE PERIOD = '###SLICE_VALUE###'
    ) AA
    LEFT JOIN
    (
        SELECT
            EVENT_DATE,
            msisdn,
            site_name
        FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY
        WHERE EVENT_DATE='###SLICE_VALUE###'
    ) BB on AA.msisdn=BB.msisdn and AA.PERIOD=BB.EVENT_DATE

    left join

    ( SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT, zone FROM dim.dt_ref_cbm ) CC on AA.msisdn=CC.msisdn

    group by AA.period, BB.site_name, multi, operateur, tenure, segment, zone
)  CI
ON
    A.EVENT_DATE=CI.EVENT_DATE AND
    A.SITE_NAME=CI.SITE_NAME AND
    A.MULTI=CI.MULTI AND
    A.OPERATEUR=CI.OPERATEUR AND
    A.TENURE=CI.TENURE AND
    A.SEGMENT=CI.SEGMENT AND
    A.ZONE=CI.ZONE
LEFT JOIN
(
    SELECT period EVENT_DATE, site_name, bdle_name, multi, operateur, tenure, segment, zone,sum(nvl(BDLE_COST,0)) as bdle_cost_subs
    FROM
    (select * from mon.spark_FT_CBM_BUNDLE_SUBS_DAILY
    WHERE PERIOD = '###SLICE_VALUE###') AAA

    left join

    (SELECT EVENT_DATE, msisdn, site_name FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE
    ='###SLICE_VALUE###') BBB on AAA.msisdn=BBB.msisdn and AAA.PERIOD=BBB.EVENT_DATE

    left join

    (SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT, zone FROM dim.dt_ref_cbm) CCC on AAA.msisdn=CCC.msisdn

    group by AAA.period, BBB.site_name, bdle_name, multi, operateur, tenure, segment, zone
) SUBS
ON
    A.EVENT_DATE=SUBS.EVENT_DATE AND
    A.SITE_NAME=SUBS.SITE_NAME AND
    A.MULTI=SUBS.MULTI AND
    A.OPERATEUR=SUBS.OPERATEUR AND
    A.TENURE=SUBS.TENURE AND
    A.SEGMENT=SUBS.SEGMENT AND
    A.ZONE=SUBS.ZONE
LEFT JOIN
(
    SELECT period EVENT_DATE, site_name, bdle_name, multi, operateur, tenure, segment, zone,sum(nvl(BDLE_COST,0)) as bdle_cost_retail

    FROM
    (select Sdate as period, sub_msisdn as msisdn, offer_name as bdle_name,
    sum(RECHARGE_AMOUNT) as BDLE_COST
    from MON.SPARK_FT_VAS_RETAILLER_IRIS
    where upper(offer_type) not in ('TOPUP') and sdate = '###SLICE_VALUE###' and PRETUPS_STATUSCODE = '200'
    group by Sdate, sub_msisdn, offer_name) A

    left join

    (SELECT EVENT_DATE, msisdn, site_name FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE ='###SLICE_VALUE###') B on A.msisdn=B.msisdn and A.PERIOD=B.EVENT_DATE

    left join

    (SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT, ZONE FROM dim.dt_ref_cbm) C on A.msisdn=C.msisdn

    group by A.period, B.site_name, bdle_name, multi, operateur, tenure, segment, zone
) RETAIL
ON
    A.EVENT_DATE=RETAIL.EVENT_DATE AND
    A.SITE_NAME=RETAIL.SITE_NAME AND
    A.MULTI=RETAIL.MULTI AND
    A.OPERATEUR=RETAIL.OPERATEUR AND
    A.TENURE=RETAIL.TENURE AND
    A.SEGMENT=RETAIL.SEGMENT AND
    A.ZONE=RETAIL.ZONE
LEFT JOIN
(
    SELECT  period EVENT_DATE, site_name, multi, operateur, tenure, segment,zone,
    sum(nbytest)/(1024*1024) as volume_data,
    sum(is_user_data1) as user_data1,
    sum(is_user_data2) as user_data2
    FROM
    (select TRANSACTION_DATE as period,
            msisdn, nbytest, case when nbytest/(1024*1024)>1  then 1 else 0 end as is_user_data1,
            case when nbytest/(1024*1024)>5  then 1 else 0 end as is_user_data2  from MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
            WHERE TRANSACTION_DATE = '###SLICE_VALUE###') A
    left join

    (SELECT EVENT_DATE, msisdn, site_name FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE
    ='###SLICE_VALUE###') B on A.msisdn=B.msisdn and A.PERIOD=B.EVENT_DATE

    left join

    (SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT, zone FROM dim.dt_ref_cbm) C on A.msisdn=C.msisdn

    group by A.period, B.site_name, multi, operateur, tenure, segment, zone
) TRAFFIC_DATA
ON
    A.EVENT_DATE=TRAFFIC_DATA.EVENT_DATE AND
    A.SITE_NAME=TRAFFIC_DATA.SITE_NAME AND
    A.MULTI=TRAFFIC_DATA.MULTI AND
    A.OPERATEUR=TRAFFIC_DATA.OPERATEUR AND
    A.TENURE=TRAFFIC_DATA.TENURE AND
    A.SEGMENT=TRAFFIC_DATA.SEGMENT AND
    A.ZONE=TRAFFIC_DATA.ZONE
LEFT JOIN
(
    SELECT CREATE_DATE EVENT_DATE, SITE_NAME, MULTI, OPERATEUR, TENURE, SEGMENT, zone,sum(MONTANT) as MONTANT
    FROM
    (
    SELECT
        A.CREATE_DATE,
        A.MSISDN,
        A.SERVICE,
        CASE WHEN C.ACCT_RES_RATING_UNIT = 'QM' THEN -A.CHARGE/100 ELSE -A.CHARGE END AS MONTANT

    FROM
    (
        SELECT
            CREATE_DATE,
            IF(SUBSTR(TRIM(ACC_NBR),1,3)=237,SUBSTR(TRIM(ACC_NBR),4,9),TRIM(ACC_NBR)) MSISDN,
            CHANNEL_ID SERVICE,
            ACCT_RES_CODE,
            COUNT(1) NOMBRE_TRANSACTION,
            SUM(NVL(CHARGE,0)) CHARGE
        FROM CDR.spark_IT_ZTE_ADJUSTMENT
        WHERE CREATE_DATE = '###SLICE_VALUE###'
        GROUP BY CREATE_DATE, IF(SUBSTR(TRIM(ACC_NBR),1,3)=237,SUBSTR(TRIM(ACC_NBR),4,9),TRIM(ACC_NBR)), CHANNEL_ID, ACCT_RES_CODE
    ) A
    LEFT JOIN DIM.spark_DT_ZTE_USAGE_TYPE B ON A.SERVICE = B.USAGE_CODE
    LEFT JOIN
    (
        SELECT DISTINCT
            ACCT_RES_ID,
            UPPER(ACCT_RES_NAME) ACCT_RES_NAME,
            ACCT_RES_RATING_TYPE,
            ACCT_RES_RATING_UNIT
        FROM DIM.spark_DT_BALANCE_TYPE_ITEM
    ) C ON A.ACCT_RES_CODE = C.ACCT_RES_ID) FI

    LEFT JOIN
    (SELECT EVENT_DATE, msisdn, site_name FROM mon.spark_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE ='###SLICE_VALUE###') EN on FI.msisdn=EN.msisdn and FI.CREATE_DATE=EN.EVENT_DATE

    left join

    (SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT, zone FROM dim.dt_ref_cbm) ES on FI.msisdn=ES.msisdn

    WHERE SERVICE IN (9,13,28,29,33)

    group by FI.CREATE_DATE, EN.SITE_NAME, multi, operateur, tenure, segment, zone
) VAS
ON
    A.EVENT_DATE=VAS.EVENT_DATE AND
    A.SITE_NAME=VAS.SITE_NAME AND
    A.MULTI=VAS.MULTI AND
    A.OPERATEUR=VAS.OPERATEUR AND
    A.TENURE=VAS.TENURE AND
    A.SEGMENT=VAS.SEGMENT AND
    A.ZONE=VAS.ZONE
LEFT JOIN
(
    select EVENT_DATE, SITE_NAME,  MULTI,  OPERATEUR,  TENURE,  SEGMENT,  ZONE,
    count(distinct case when GRP_GP_STATUS='ACTIF' then A.msisdn else null end) as actif_90,
    count(distinct case when DEACTIVATION_DATE=date_sub(EVENT_DATE,89) then A.msisdn else null end) as inactif_90,
    count(distinct case when ACTIVATION_DATE=EVENT_DATE then A.msisdn else null end) as gross_add
    from (select EVENT_DATE, msisdn, GRP_GP_STATUS, DEACTIVATION_DATE, ACTIVATION_DATE ,
                LOC_SITE_NAME as SITE_NAME from mon.spark_ft_marketing_datamart
    where EVENT_DATE ='###SLICE_VALUE###') A
    left join (SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT, zone FROM dim.dt_ref_cbm) B on A.msisdn=B.msisdn
    group by A.EVENT_DATE, A.SITE_NAME, multi, operateur, tenure, segment, zone
)PARC
ON
    A.EVENT_DATE=PARC.EVENT_DATE AND
    A.SITE_NAME=PARC.SITE_NAME AND
    A.MULTI=PARC.MULTI AND
    A.OPERATEUR=PARC.OPERATEUR AND
    A.TENURE=PARC.TENURE AND
    A.SEGMENT=PARC.SEGMENT AND
    A.ZONE=PARC.ZONE
GROUP BY A.EVENT_DATE, A.SITE_NAME,  A.MULTI,  A.OPERATEUR,  A.TENURE,  A.SEGMENT,  A.ZONE, SUBS.BDLE_NAME, RETAIL.BDLE_NAME