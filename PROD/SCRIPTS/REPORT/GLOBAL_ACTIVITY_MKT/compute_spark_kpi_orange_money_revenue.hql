create or replace PACKAGE BODY     PCK_LOAD_GLOBAL_ACTIVITY_MKT IS
            /*
            <VERSION>1.0.0</VERSION>
            <FILENAME>MON.PCK_LOAD_GLOBAL_ACTIVITY_MKT.pkg</FILENAME>
            <AUTHOR>Felix BOGNOU felix.bognou@orange.cm</AUTHOR>
            <SUMMARY>none</SUMMARY>
            <COPYRIGHT>Orange Cameroun, 2015</COPYRIGHT>
            <OVERVIEW>
                Ce Package alimente les agregats FT_GLOBAL_ACTIVITY_DAILY_MKT, FT_REFILL_SLICE_MKT qui contient l'activité glogale des abonnés Orange Cameroun pour des besoins du dashbord MKT
            </OVERVIEW>
            <DEPENDENCIES>
            ALL_SOURCE data dictionary view
            </DEPENDENCIES>
            <EXCEPTIONS>None</EXCEPTIONS>
            Modification History
            Date By Modification
            ---------- --------- -------------------------------
            <MODIFICATIONS>
            13/01/2015 Creation date
            </MODIFICATIONS>
            */
PROCEDURE MAIN IS
BEGIN
--Appel des procedures
    P_INS_GSM_TRAFFIC_REVENUE;
    COMMIT;
    P_INS_GSM_LOC_TRAFFIC_REVENUE;
    COMMIT ;
    P_INS_SUBSCRIPTION_REVENUE;
    COMMIT;
    P_INS_GPRS_REVENUE;
    COMMIT;
    P_INS_GPRS_LOC_REVENUE;
    COMMIT;
    P_INS_UNIQUE_USER;
    COMMIT;
    P_INS_REFILL_TRAFFIC;
    COMMIT;
    P_INS_SUBCRIPTION_LOCATION;
    COMMIT;
    P_INS_USER_LOCATION;
    COMMIT ;
    P_INS_CUSTOMER_BASE;
    COMMIT;
    P_INS_ORANGE_MONEY_REVENUE;
    COMMIT;
END; -- MAIN

----------------------------------------------------------------------------------------------------------------

PROCEDURE P_INS_GSM_TRAFFIC_REVENUE (p_gsm_revenu_number_day_from IN NUMBER DEFAULT 25,p_gsm_revenu_number_day_bef IN NUMBER DEFAULT 1) IS

  gsm_revenu_number_day_from number:=p_gsm_revenu_number_day_from; --Nombre de jours à partir duquel il faut checker (J-n)
  gsm_revenu_number_day_bef number:=p_gsm_revenu_number_day_bef; --Nombre de jours à jusqu'à quand il faut checker (Sysdate-n)

  BEGIN
  --Insertion du revenue MAIN
INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
                        SELECT
                        TRANSACTION_DATE
                        ,(CASE
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_INT' THEN 'REVENUE_VOICE_INTERNATIONAL'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'REVENUE_SMS_CAMTEL'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'REVENUE_SMS_ONNET'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_SVA' THEN 'REVENUE_SMS_VAS'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_ROAM_MO' THEN 'REVENUE_SMS_ROAMING'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MT' THEN 'REVENUE_VOICE_ROAMING'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'REVENUE_SMS_ONNET'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'REVENUE_VOICE_NEXTTEL'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'REVENUE_VOICE_ONNET'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'REVENUE_VOICE_CAMTEL'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_INT' THEN 'REVENUE_SMS_INTERNATIONAL'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'REVENUE_SMS_MTN'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_SVA' THEN 'REVENUE_VOICE_VAS'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_CCSVA' THEN 'REVENUE_VOICE_VAS'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'REVENUE_SMS_NEXTTEL'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MO' THEN 'REVENUE_VOICE_ROAMING'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'REVENUE_VOICE_MTN'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'REVENUE_VOICE_ONNET'
                        ELSE DECODE(SERVICE_CODE, 'VOI_VOX', 'REVENUE_VOICE_OTHER', 'REVENUE_SMS_OTHER') END
                        ) DESTINATION_CODE
                        ,OFFER_PROFILE_CODE PROFILE_CODE
                        ,'MAIN' SUB_ACCOUNT
                        ,'HIT' MEASUREMENT_UNIT
                        , 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE
                        ,OPERATOR_CODE
                        ,SUM(MAIN_RATED_AMOUNT) TOTAL_AMOUNT
                        ,SUM(MAIN_RATED_AMOUNT) RATED_AMOUNT
                        ,SYSDATE INSERT_DATE
                        ,NULL REGION_ID
                        FROM FT_GSM_TRAFFIC_REVENUE_DAILY
                        WHERE TRANSACTION_DATE IN ( SELECT datecode
                                     FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                                                 WHERE datecode BETWEEN TRUNC(SYSDATE-gsm_revenu_number_day_from) AND TRUNC(SYSDATE-gsm_revenu_number_day_bef)
                                               MINUS
                                          SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                                WHERE SOURCE_TABLE='FT_GSM_TRAFFIC_REVENUE_DAILY' AND SUB_ACCOUNT='MAIN' AND TRANSACTION_DATE BETWEEN trunc(sysdate-gsm_revenu_number_day_from) and trunc(sysdate-gsm_revenu_number_day_bef))
                                                            )
                        GROUP BY
                        TRANSACTION_DATE
                        ,(CASE
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_INT' THEN 'REVENUE_VOICE_INTERNATIONAL'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'REVENUE_SMS_CAMTEL'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'REVENUE_SMS_ONNET'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_SVA' THEN 'REVENUE_SMS_VAS'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_ROAM_MO' THEN 'REVENUE_SMS_ROAMING'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MT' THEN 'REVENUE_VOICE_ROAMING'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'REVENUE_SMS_ONNET'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'REVENUE_VOICE_NEXTTEL'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'REVENUE_VOICE_ONNET'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'REVENUE_VOICE_CAMTEL'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_INT' THEN 'REVENUE_SMS_INTERNATIONAL'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'REVENUE_SMS_MTN'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_SVA' THEN 'REVENUE_VOICE_VAS'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_CCSVA' THEN 'REVENUE_VOICE_VAS'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'REVENUE_SMS_NEXTTEL'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MO' THEN 'REVENUE_VOICE_ROAMING'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'REVENUE_VOICE_MTN'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'REVENUE_VOICE_ONNET'
                        ELSE DECODE(SERVICE_CODE, 'VOI_VOX', 'REVENUE_VOICE_OTHER', 'REVENUE_SMS_OTHER') END
                        )
                        ,OFFER_PROFILE_CODE
                        ,OPERATOR_CODE
                        ;
COMMIT;

--Insertion du revenue PROMO
INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
                        SELECT
                        TRANSACTION_DATE
                        ,(CASE
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_INT' THEN 'REVENUE_VOICE_INTERNATIONAL'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'REVENUE_SMS_CAMTEL'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'REVENUE_SMS_ONNET'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_SVA' THEN 'REVENUE_SMS_VAS'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_ROAM_MO' THEN 'REVENUE_SMS_ROAMING'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MT' THEN 'REVENUE_VOICE_ROAMING'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'REVENUE_SMS_ONNET'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'REVENUE_VOICE_NEXTTEL'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'REVENUE_VOICE_ONNET'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'REVENUE_VOICE_CAMTEL'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_INT' THEN 'REVENUE_SMS_INTERNATIONAL'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'REVENUE_SMS_MTN'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_SVA' THEN 'REVENUE_VOICE_VAS'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_CCSVA' THEN 'REVENUE_VOICE_VAS'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'REVENUE_SMS_NEXTTEL'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MO' THEN 'REVENUE_VOICE_ROAMING'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'REVENUE_VOICE_MTN'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'REVENUE_VOICE_ONNET'
                        ELSE DECODE(SERVICE_CODE, 'VOI_VOX', 'REVENUE_VOICE_OTHER', 'REVENUE_SMS_OTHER') END
                        ) DESTINATION_CODE
                        ,OFFER_PROFILE_CODE PROFILE_CODE
                        ,'PROMO' SUB_ACCOUNT
                        ,'HIT' MEASUREMENT_UNIT
                        , 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE
                        ,OPERATOR_CODE
                        ,SUM(PROMO_RATED_AMOUNT) TOTAL_AMOUNT
                        ,SUM(PROMO_RATED_AMOUNT) RATED_AMOUNT
                        ,SYSDATE INSERT_DATE
                        ,NULL REGION_ID
                        FROM FT_GSM_TRAFFIC_REVENUE_DAILY
                        WHERE TRANSACTION_DATE IN ( SELECT datecode
                                     FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                                                 WHERE datecode BETWEEN TRUNC(SYSDATE-gsm_revenu_number_day_from) AND TRUNC(SYSDATE-gsm_revenu_number_day_bef)
                                               MINUS
                                          SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                                WHERE SOURCE_TABLE='FT_GSM_TRAFFIC_REVENUE_DAILY' AND SUB_ACCOUNT='PROMO' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-gsm_revenu_number_day_from) AND TRUNC(SYSDATE-gsm_revenu_number_day_bef))
                                                            )
                        GROUP BY
                        TRANSACTION_DATE
                        ,(CASE
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_INT' THEN 'REVENUE_VOICE_INTERNATIONAL'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'REVENUE_SMS_CAMTEL'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'REVENUE_SMS_ONNET'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_SVA' THEN 'REVENUE_SMS_VAS'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_ROAM_MO' THEN 'REVENUE_SMS_ROAMING'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MT' THEN 'REVENUE_VOICE_ROAMING'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'REVENUE_SMS_ONNET'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'REVENUE_VOICE_NEXTTEL'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'REVENUE_VOICE_ONNET'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'REVENUE_VOICE_CAMTEL'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_INT' THEN 'REVENUE_SMS_INTERNATIONAL'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'REVENUE_SMS_MTN'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_SVA' THEN 'REVENUE_VOICE_VAS'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_CCSVA' THEN 'REVENUE_VOICE_VAS'
                        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'REVENUE_SMS_NEXTTEL'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MO' THEN 'REVENUE_VOICE_ROAMING'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'REVENUE_VOICE_MTN'
                        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'REVENUE_VOICE_ONNET'
                        ELSE DECODE(SERVICE_CODE, 'VOI_VOX', 'REVENUE_VOICE_OTHER', 'REVENUE_SMS_OTHER') END
                        )
                        ,OFFER_PROFILE_CODE
                        ,OPERATOR_CODE
                        ;
COMMIT;

--Insertion des usages Voix en Minute et SMS en Nombre

INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
SELECT TRANSACTION_DATE
,(CASE
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_INT' THEN 'USAGE_VOICE_INTERNATIONAL'
WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'USAGE_SMS_CAMTEL'
WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'USAGE_SMS_ONNET'
WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_SVA' THEN 'USAGE_SMS_VAS'
WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_ROAM_MO' THEN 'USAGE_SMS_ROAMING'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MT' THEN 'USAGE_VOICE_ROAMING'
WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'USAGE_SMS_ONNET'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'USAGE_VOICE_NEXTTEL'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'USAGE_VOICE_ONNET'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'USAGE_VOICE_CAMTEL'
WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_INT' THEN 'USAGE_SMS_INTERNATIONAL'
WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'USAGE_SMS_MTN'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_SVA' THEN 'USAGE_VOICE_VAS'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_CCSVA' THEN 'USAGE_VOICE_VAS'
WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'USAGE_SMS_NEXTTEL'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MO' THEN 'USAGE_VOICE_ROAMING'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'USAGE_VOICE_MTN'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'USAGE_VOICE_ONNET'
ELSE DECODE(SERVICE_CODE, 'VOI_VOX', 'USAGE_VOICE_OTHER', 'USAGE_SMS_OTHER') END
) DESTINATION_CODE
,OFFER_PROFILE_CODE PROFILE_CODE
,'UNKNOWN' SUB_ACCOUNT
,(CASE WHEN SERVICE_CODE='NVX_SMS' THEN 'HIT'
       WHEN SERVICE_CODE='VOI_VOX' THEN 'DURATION' ELSE 'UNKOWN' END
  ) MEASUREMENT_UNIT
, 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE
,OPERATOR_CODE
,SUM(CASE WHEN SERVICE_CODE='NVX_SMS' THEN TOTAL_COUNT
          WHEN SERVICE_CODE='VOI_VOX' THEN DURATION/60 ELSE 0 END) TOTAL_AMOUNT
,SUM(CASE WHEN SERVICE_CODE='NVX_SMS' THEN RATED_TOTAL_COUNT
          WHEN SERVICE_CODE='VOI_VOX' THEN RATED_DURATION/60 ELSE 0 END) RATED_AMOUNT
,SYSDATE INSERT_DATE
,NULL REGION_ID
FROM FT_GSM_TRAFFIC_REVENUE_DAILY
WHERE TRANSACTION_DATE  IN ( SELECT datecode
                                     FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                                                 WHERE datecode BETWEEN TRUNC(SYSDATE-gsm_revenu_number_day_from) AND TRUNC(SYSDATE-gsm_revenu_number_day_bef)
                                               MINUS
                                          SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                                WHERE SOURCE_TABLE='FT_GSM_TRAFFIC_REVENUE_DAILY' AND SUB_ACCOUNT='UNKNOWN' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-gsm_revenu_number_day_from) AND TRUNC(SYSDATE-gsm_revenu_number_day_bef))
                                                            )
GROUP BY
TRANSACTION_DATE
,(CASE
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_INT' THEN 'USAGE_VOICE_INTERNATIONAL'
WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'USAGE_SMS_CAMTEL'
WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'USAGE_SMS_ONNET'
WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_SVA' THEN 'USAGE_SMS_VAS'
WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_ROAM_MO' THEN 'USAGE_SMS_ROAMING'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MT' THEN 'USAGE_VOICE_ROAMING'
WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'USAGE_SMS_ONNET'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'USAGE_VOICE_NEXTTEL'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'USAGE_VOICE_ONNET'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'USAGE_VOICE_CAMTEL'
WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_INT' THEN 'USAGE_SMS_INTERNATIONAL'
WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'USAGE_SMS_MTN'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_SVA' THEN 'USAGE_VOICE_VAS'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_CCSVA' THEN 'USAGE_VOICE_VAS'
WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'USAGE_SMS_NEXTTEL'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MO' THEN 'USAGE_VOICE_ROAMING'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'USAGE_VOICE_MTN'
WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'USAGE_VOICE_ONNET'
ELSE DECODE(SERVICE_CODE, 'VOI_VOX', 'USAGE_VOICE_OTHER', 'USAGE_SMS_OTHER') END
)
,OFFER_PROFILE_CODE
,(CASE WHEN SERVICE_CODE='NVX_SMS' THEN 'HIT'
       WHEN SERVICE_CODE='VOI_VOX' THEN 'DURATION' ELSE 'UNKOWN' END
  )
,OPERATOR_CODE
;
COMMIT;

END; -- P_INS_GSM_TRAFFIC_REVENUE



PROCEDURE P_INS_GSM_LOC_TRAFFIC_REVENUE (p_loc_revenu_number_day_from IN NUMBER DEFAULT 25,p_loc_revenu_number_day_bef IN NUMBER DEFAULT 1) IS

  loc_revenu_number_day_from number:=p_loc_revenu_number_day_from; --Nombre de jours à partir duquel il faut checker (J-n)
  loc_revenu_number_day_bef number:=p_loc_revenu_number_day_bef; --Nombre de jours à jusqu'à quand il faut checker (Sysdate-n)

/*
  n_is_empty NUMBER;
  BEGIN


SELECT COUNT(*) INTO n_is_empty FROM MON.VW_SDT_LAC_INFO;

-- si le calcule pour slice peut être effectué sans pb
IF n_is_empty <> 0 THEN
    --Insertion du revenu MAIN par region
    INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
    SELECT
    TRANSACTION_DATE
    , DESTINATION_CODE
    , PROFILE_CODE
    , SUB_ACCOUNT
    , MEASUREMENT_UNIT
    , SOURCE_TABLE
    , OPERATOR_CODE
    , TOTAL_AMOUNT
    , RATED_AMOUNT
    , SYSDATE INSERT_DATE
    , REGION_ID
    FROM
                    (SELECT
                            TRANSACTION_DATE
                            ,(CASE
                            WHEN SERVICE_CODE='VOI_VOX' THEN 'REVENUE_LOCATION_VOICE'
                            WHEN SERVICE_CODE='NVX_SMS' THEN 'REVENUE_LOCATION_SMS'
                            ELSE 'REVENUE_LOCATION_OTHER' END
                            ) DESTINATION_CODE
                            ,OFFER_PROFILE_CODE PROFILE_CODE
                            ,'MAIN' SUB_ACCOUNT
                            ,'HIT' MEASUREMENT_UNIT
                            , 'FT_GSM_LOCATION_REVENUE_DAILY' SOURCE_TABLE
                            ,OPERATOR_CODE
                            ,SUM(MAIN_RATED_AMOUNT) TOTAL_AMOUNT
                            ,SUM(MAIN_RATED_AMOUNT) RATED_AMOUNT
    --                        ,SYSDATE INSERT_DATE
                            ,vs.ADMINISTRATIVE_REGION
                            FROM FT_GSM_LOCATION_REVENUE_DAILY a, VW_SDT_LAC_INFO vs
                            WHERE TRANSACTION_DATE   IN ( SELECT datecode
                                         FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                                                     WHERE datecode BETWEEN TRUNC(SYSDATE-loc_revenu_number_day_from) AND TRUNC(SYSDATE-loc_revenu_number_day_bef)
                                                   MINUS
                                              SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                                    WHERE SOURCE_TABLE='FT_GSM_LOCATION_REVENUE_DAILY' AND SUB_ACCOUNT='MAIN' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-loc_revenu_number_day_from) AND TRUNC(SYSDATE-loc_revenu_number_day_bef))
                                                                )
                            AND LPAD(fn_hex2deci(UPPER(NSL_LAC)),5,0) = vs.LAC(+)
                            GROUP BY
                            TRANSACTION_DATE
                            ,(CASE
                            WHEN SERVICE_CODE='VOI_VOX' THEN 'REVENUE_LOCATION_VOICE'
                            WHEN SERVICE_CODE='NVX_SMS' THEN 'REVENUE_LOCATION_SMS'
                            ELSE 'REVENUE_LOCATION_OTHER' END
                            )
                            ,OFFER_PROFILE_CODE
                            ,OPERATOR_CODE
                            ,vs.ADMINISTRATIVE_REGION
                            ) f, DIM.DT_REGIONS_MKT r
    WHERE TRIM(COALESCE(f.ADMINISTRATIVE_REGION, 'INCONNU')) = r.ADMINISTRATIVE_REGION
    ;
    COMMIT ;

    --Insertion du revenu PROMO par region
    INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
    SELECT
    TRANSACTION_DATE
    , DESTINATION_CODE
    , PROFILE_CODE
    , SUB_ACCOUNT
    , MEASUREMENT_UNIT
    , SOURCE_TABLE
    , OPERATOR_CODE
    , TOTAL_AMOUNT
    , RATED_AMOUNT
    , SYSDATE INSERT_DATE
    , REGION_ID
    FROM
                    (SELECT
                            TRANSACTION_DATE
                            ,(CASE
                            WHEN SERVICE_CODE='VOI_VOX' THEN 'REVENUE_LOCATION_VOICE'
                            WHEN SERVICE_CODE='NVX_SMS' THEN 'REVENUE_LOCATION_SMS'
                            ELSE 'REVENUE_LOCATION_OTHER' END
                            ) DESTINATION_CODE
                            ,OFFER_PROFILE_CODE PROFILE_CODE
                            ,'PROMO' SUB_ACCOUNT
                            ,'HIT' MEASUREMENT_UNIT
                            , 'FT_GSM_LOCATION_REVENUE_DAILY' SOURCE_TABLE
                            ,OPERATOR_CODE
                            ,SUM(PROMO_RATED_AMOUNT) TOTAL_AMOUNT
                            ,SUM(PROMO_RATED_AMOUNT) RATED_AMOUNT
    --                        ,SYSDATE INSERT_DATE
                            ,vs.ADMINISTRATIVE_REGION
                            FROM FT_GSM_LOCATION_REVENUE_DAILY a, VW_SDT_LAC_INFO vs
                            WHERE TRANSACTION_DATE     IN ( SELECT datecode
                                         FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                                                     WHERE datecode BETWEEN TRUNC(SYSDATE-loc_revenu_number_day_from) AND TRUNC(SYSDATE-loc_revenu_number_day_bef)
                                                   MINUS
                                              SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                                    WHERE SOURCE_TABLE='FT_GSM_LOCATION_REVENUE_DAILY' AND SUB_ACCOUNT='PROMO' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-loc_revenu_number_day_from) AND TRUNC(SYSDATE-loc_revenu_number_day_bef))
                                                                )
                            AND LPAD(fn_hex2deci(UPPER(NSL_LAC)),5,0) = vs.LAC(+)
                            GROUP BY
                            TRANSACTION_DATE
                            ,(CASE
                            WHEN SERVICE_CODE='VOI_VOX' THEN 'REVENUE_LOCATION_VOICE'
                            WHEN SERVICE_CODE='NVX_SMS' THEN 'REVENUE_LOCATION_SMS'
                            ELSE 'REVENUE_LOCATION_OTHER' END
                            )
                            ,OFFER_PROFILE_CODE
                            ,OPERATOR_CODE
                            ,vs.ADMINISTRATIVE_REGION
                            ) f, DIM.DT_REGIONS_MKT r
    WHERE TRIM(COALESCE(f.ADMINISTRATIVE_REGION, 'INCONNU')) = r.ADMINISTRATIVE_REGION
    ;
    COMMIT ;

    --Insertion des usages Voix en Minute par localisation

    INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
    SELECT
    TRANSACTION_DATE
    , DESTINATION_CODE
    , PROFILE_CODE
    , SUB_ACCOUNT
    , MEASUREMENT_UNIT
    , SOURCE_TABLE
    , OPERATOR_CODE
    , TOTAL_AMOUNT
    , RATED_AMOUNT
    , SYSDATE INSERT_DATE
    , REGION_ID
    FROM
                    (SELECT
                            TRANSACTION_DATE
                            ,(CASE
                            WHEN SERVICE_CODE='VOI_VOX' THEN 'USAGE_LOCATION_VOICE'
                            WHEN SERVICE_CODE='NVX_SMS' THEN 'USAGE_LOCATION_SMS'
                            ELSE 'USAGE_LOCATION_OTHER' END
                            )  DESTINATION_CODE
                            ,OFFER_PROFILE_CODE PROFILE_CODE
                            ,'UNKNOWN' SUB_ACCOUNT
                            ,(CASE WHEN SERVICE_CODE='NVX_SMS' THEN 'HIT'
                              WHEN SERVICE_CODE='VOI_VOX' THEN 'DURATION' ELSE 'UNKOWN' END
                                 )  MEASUREMENT_UNIT
                            , 'FT_GSM_LOCATION_REVENUE_DAILY' SOURCE_TABLE
                            ,OPERATOR_CODE
                            ,SUM(CASE WHEN SERVICE_CODE='NVX_SMS' THEN TOTAL_COUNT
                                                    WHEN SERVICE_CODE='VOI_VOX' THEN DURATION/60 ELSE 0 END) TOTAL_AMOUNT
                            ,SUM(CASE WHEN SERVICE_CODE='NVX_SMS' THEN RATED_TOTAL_COUNT
                                                     WHEN SERVICE_CODE='VOI_VOX' THEN RATED_DURATION/60 ELSE 0 END) RATED_AMOUNT
                            ,vs.ADMINISTRATIVE_REGION
                            FROM FT_GSM_LOCATION_REVENUE_DAILY a, VW_SDT_LAC_INFO vs
                            WHERE SERVICE_CODE='VOI_VOX'  --Se limiter à l'usage voix pous l'instant
                            AND TRANSACTION_DATE   IN ( SELECT datecode
                                         FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                                                     WHERE datecode BETWEEN TRUNC(SYSDATE-loc_revenu_number_day_from) AND TRUNC(SYSDATE-loc_revenu_number_day_bef)
                                                   MINUS
                                              SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                                    WHERE SOURCE_TABLE='FT_GSM_LOCATION_REVENUE_DAILY' AND SUB_ACCOUNT='UNKNOWN' AND DESTINATION_CODE='USAGE_LOCATION_VOICE'  AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-loc_revenu_number_day_from) AND TRUNC(SYSDATE-loc_revenu_number_day_bef))
                                                                )
                            AND LPAD(fn_hex2deci(UPPER(NSL_LAC)),5,0) = vs.LAC(+)
                            GROUP BY
                            TRANSACTION_DATE
                            ,(CASE
                            WHEN SERVICE_CODE='VOI_VOX' THEN 'USAGE_LOCATION_VOICE'
                            WHEN SERVICE_CODE='NVX_SMS' THEN 'USAGE_LOCATION_SMS'
                            ELSE 'USAGE_LOCATION_OTHER' END
                            )
                            ,OFFER_PROFILE_CODE
                            ,(CASE WHEN SERVICE_CODE='NVX_SMS' THEN 'HIT'
                              WHEN SERVICE_CODE='VOI_VOX' THEN 'DURATION' ELSE 'UNKOWN' END
                                 )
                            ,OPERATOR_CODE
                            ,vs.ADMINISTRATIVE_REGION
                            ) f, DIM.DT_REGIONS_MKT r
    WHERE TRIM(COALESCE(f.ADMINISTRATIVE_REGION, 'INCONNU')) = r.ADMINISTRATIVE_REGION
    ;
    COMMIT;


END IF ;


END;
*/
  d_start_date DATE;
  d_end_date DATE;
  s_val NUMBER;
  i_val NUMBER;
  s_slice_value VARCHAR2(15);
  n_is_empty INT;
BEGIN
    d_start_date := TRUNC(SYSDATE - loc_revenu_number_day_from);
    d_end_date := TRUNC(SYSDATE - loc_revenu_number_day_bef);

    WHILE d_start_date <= d_end_date LOOP
        s_slice_value := TO_CHAR(d_start_date, 'yyyymmdd');
        -- Insert code here
        SELECT COUNT(*) INTO s_val FROM MON.FT_GLOBAL_ACTIVITY_DAILY_MKT
        WHERE SOURCE_TABLE='FT_GSM_LOCATION_REVENUE_DAILY'  AND TRANSACTION_DATE = d_start_date AND ROWNUM <= 10;

        SELECT COUNT(*) INTO n_is_empty FROM MON.VW_SDT_LAC_INFO;

        SELECT mon.FN_VALIDATE_DAY2DAY_EXIST ('MON.FT_GSM_LOCATION_REVENUE_DAILY', 'TRANSACTION_DATE'
                        , s_slice_value, s_slice_value, 10, '') INTO i_val FROM DUAL;

        IF s_val = 0 AND i_val = 1 AND n_is_empty <> 0 THEN

            INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
            SELECT
            TRANSACTION_DATE
            , DESTINATION_CODE
            , PROFILE_CODE
            , SUB_ACCOUNT
            , MEASUREMENT_UNIT
            , SOURCE_TABLE
            , OPERATOR_CODE
            , TOTAL_AMOUNT
            , RATED_AMOUNT
            , SYSDATE INSERT_DATE
            , REGION_ID
            FROM
            (SELECT
                TRANSACTION_DATE
                ,(CASE
                WHEN SERVICE_CODE='VOI_VOX' THEN 'REVENUE_LOCATION_VOICE'
                WHEN SERVICE_CODE='NVX_SMS' THEN 'REVENUE_LOCATION_SMS'
                ELSE 'REVENUE_LOCATION_OTHER' END
                ) DESTINATION_CODE
                ,OFFER_PROFILE_CODE PROFILE_CODE
                ,'MAIN' SUB_ACCOUNT
                ,'HIT' MEASUREMENT_UNIT
                , 'FT_GSM_LOCATION_REVENUE_DAILY' SOURCE_TABLE
                ,OPERATOR_CODE
                ,SUM(MAIN_RATED_AMOUNT) TOTAL_AMOUNT
                ,SUM(MAIN_RATED_AMOUNT) RATED_AMOUNT
        --                        ,SYSDATE INSERT_DATE
                ,vs.ADMINISTRATIVE_REGION
            FROM FT_GSM_LOCATION_REVENUE_DAILY a, VW_SDT_LAC_INFO vs
            WHERE TRANSACTION_DATE = d_start_date
                AND LPAD(fn_hex2deci(UPPER(NSL_LAC)),5,0) = vs.LAC(+)
                GROUP BY
                TRANSACTION_DATE
                ,(CASE
                WHEN SERVICE_CODE='VOI_VOX' THEN 'REVENUE_LOCATION_VOICE'
                WHEN SERVICE_CODE='NVX_SMS' THEN 'REVENUE_LOCATION_SMS'
                ELSE 'REVENUE_LOCATION_OTHER' END
                )
                ,OFFER_PROFILE_CODE
                ,OPERATOR_CODE
                ,vs.ADMINISTRATIVE_REGION
                ) f, DIM.DT_REGIONS_MKT r
            WHERE TRIM(COALESCE(f.ADMINISTRATIVE_REGION, 'INCONNU')) = r.ADMINISTRATIVE_REGION;

            --Insertion du revenu PROMO par region
            INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
            SELECT
            TRANSACTION_DATE
            , DESTINATION_CODE
            , PROFILE_CODE
            , SUB_ACCOUNT
            , MEASUREMENT_UNIT
            , SOURCE_TABLE
            , OPERATOR_CODE
            , TOTAL_AMOUNT
            , RATED_AMOUNT
            , SYSDATE INSERT_DATE
            , REGION_ID
            FROM
            (SELECT
                TRANSACTION_DATE
                ,(CASE
                WHEN SERVICE_CODE='VOI_VOX' THEN 'REVENUE_LOCATION_VOICE'
                WHEN SERVICE_CODE='NVX_SMS' THEN 'REVENUE_LOCATION_SMS'
                ELSE 'REVENUE_LOCATION_OTHER' END
                ) DESTINATION_CODE
                ,OFFER_PROFILE_CODE PROFILE_CODE
                ,'PROMO' SUB_ACCOUNT
                ,'HIT' MEASUREMENT_UNIT
                , 'FT_GSM_LOCATION_REVENUE_DAILY' SOURCE_TABLE
                ,OPERATOR_CODE
                ,SUM(PROMO_RATED_AMOUNT) TOTAL_AMOUNT
                ,SUM(PROMO_RATED_AMOUNT) RATED_AMOUNT
        --                        ,SYSDATE INSERT_DATE
                ,vs.ADMINISTRATIVE_REGION
            FROM FT_GSM_LOCATION_REVENUE_DAILY a, VW_SDT_LAC_INFO vs
            WHERE TRANSACTION_DATE = d_start_date
                AND LPAD(fn_hex2deci(UPPER(NSL_LAC)),5,0) = vs.LAC(+)
                GROUP BY
                TRANSACTION_DATE
                ,(CASE
                WHEN SERVICE_CODE='VOI_VOX' THEN 'REVENUE_LOCATION_VOICE'
                WHEN SERVICE_CODE='NVX_SMS' THEN 'REVENUE_LOCATION_SMS'
                ELSE 'REVENUE_LOCATION_OTHER' END
                )
                ,OFFER_PROFILE_CODE
                ,OPERATOR_CODE
                ,vs.ADMINISTRATIVE_REGION
                ) f, DIM.DT_REGIONS_MKT r
            WHERE TRIM(COALESCE(f.ADMINISTRATIVE_REGION, 'INCONNU')) = r.ADMINISTRATIVE_REGION;

            --Insertion des usages Voix en Minute par localisation

            INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
            SELECT
            TRANSACTION_DATE
            , DESTINATION_CODE
            , PROFILE_CODE
            , SUB_ACCOUNT
            , MEASUREMENT_UNIT
            , SOURCE_TABLE
            , OPERATOR_CODE
            , TOTAL_AMOUNT
            , RATED_AMOUNT
            , SYSDATE INSERT_DATE
            , REGION_ID
            FROM
            (SELECT
                    TRANSACTION_DATE
                    ,(CASE
                    WHEN SERVICE_CODE='VOI_VOX' THEN 'USAGE_LOCATION_VOICE'
                    WHEN SERVICE_CODE='NVX_SMS' THEN 'USAGE_LOCATION_SMS'
                    ELSE 'USAGE_LOCATION_OTHER' END
                    )  DESTINATION_CODE
                    ,OFFER_PROFILE_CODE PROFILE_CODE
                    ,'UNKNOWN' SUB_ACCOUNT
                    ,(CASE WHEN SERVICE_CODE='NVX_SMS' THEN 'HIT'
                      WHEN SERVICE_CODE='VOI_VOX' THEN 'DURATION' ELSE 'UNKOWN' END
                         )  MEASUREMENT_UNIT
                    , 'FT_GSM_LOCATION_REVENUE_DAILY' SOURCE_TABLE
                    ,OPERATOR_CODE
                    ,SUM(CASE WHEN SERVICE_CODE='NVX_SMS' THEN TOTAL_COUNT
                                            WHEN SERVICE_CODE='VOI_VOX' THEN DURATION/60 ELSE 0 END) TOTAL_AMOUNT
                    ,SUM(CASE WHEN SERVICE_CODE='NVX_SMS' THEN RATED_TOTAL_COUNT
                                             WHEN SERVICE_CODE='VOI_VOX' THEN RATED_DURATION/60 ELSE 0 END) RATED_AMOUNT
                    ,vs.ADMINISTRATIVE_REGION
                FROM FT_GSM_LOCATION_REVENUE_DAILY a, VW_SDT_LAC_INFO vs
                WHERE SERVICE_CODE='VOI_VOX'  --Se limiter à l'usage voix pous l'instant
                    AND TRANSACTION_DATE = d_start_date
                    AND LPAD(fn_hex2deci(UPPER(NSL_LAC)),5,0) = vs.LAC(+)
                    GROUP BY
                    TRANSACTION_DATE
                    ,(CASE
                    WHEN SERVICE_CODE='VOI_VOX' THEN 'USAGE_LOCATION_VOICE'
                    WHEN SERVICE_CODE='NVX_SMS' THEN 'USAGE_LOCATION_SMS'
                    ELSE 'USAGE_LOCATION_OTHER' END
                    )
                    ,OFFER_PROFILE_CODE
                    ,(CASE WHEN SERVICE_CODE='NVX_SMS' THEN 'HIT'
                      WHEN SERVICE_CODE='VOI_VOX' THEN 'DURATION' ELSE 'UNKOWN' END
                         )
                    ,OPERATOR_CODE
                    ,vs.ADMINISTRATIVE_REGION
                    ) f, DIM.DT_REGIONS_MKT r
                WHERE TRIM(COALESCE(f.ADMINISTRATIVE_REGION, 'INCONNU')) = r.ADMINISTRATIVE_REGION;

            COMMIT;

        END IF;
        --
        d_start_date := d_start_date + 1;
    END LOOP;
END;  -- P_INS_GSM_LOC_TRAFFIC_REVENUE

----------------------------------------------------------------------------------------------------------------

PROCEDURE P_INS_SUBSCRIPTION_REVENUE (p_subs_number_day_from IN NUMBER DEFAULT 25,p_subs_number_day_bef IN NUMBER DEFAULT 1)  IS

  subs_number_day_from number:=p_subs_number_day_from;
  subs_number_day_bef number:=p_subs_number_day_bef;


  BEGIN
      --Insertion du Revenu des souscriptions voix (Orange Bundle)
      -- @Maj : par dimitri.happi@orange.cm : Ajout du forfaits : 'IPP SASSAYE JEUNESSE' et 'IPP FAMILYTALK FOR SUNDAY TALK'

INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
                SELECT
                TRANSACTION_DATE
                ,'REVENUE_VOICE_BUNDLE' DESTINATION_CODE
                ,COMMERCIAL_OFFER PROFILE_CODE
                ,'MAIN' SUB_ACCOUNT
                ,'HIT' MEASUREMENT_UNIT
                , 'FT_A_SUBSCRIPTION' SOURCE_TABLE
                ,OPERATOR_CODE
                ,SUM(SUBS_AMOUNT) TOTAL_AMOUNT
                ,SUM(SUBS_AMOUNT) RATED_AMOUNT
                ,SYSDATE INSERT_DATE
                ,NULL REGION_ID
                FROM FT_A_SUBSCRIPTION
                WHERE TRANSACTION_DATE  IN ( SELECT datecode
                             FROM (SELECT DISTINCT datecode from DIM.DT_DATES
                                         WHERE datecode BETWEEN TRUNC(SYSDATE-subs_number_day_from) and TRUNC(SYSDATE-subs_number_day_bef)
                                       MINUS
                                  SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                        WHERE SOURCE_TABLE='FT_A_SUBSCRIPTION' AND DESTINATION_CODE='REVENUE_VOICE_BUNDLE' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-subs_number_day_from) AND TRUNC(SYSDATE-subs_number_day_bef))
                                                    )
                AND (
                        UPPER(SUBS_BENEFIT_NAME)='PREPAID INDIVIDUAL BUNDLE MONEY'
                     OR UPPER(SUBS_BENEFIT_NAME) LIKE 'FORFAITS ORANGE%'
                     OR UPPER(SUBS_BENEFIT_NAME) LIKE 'FORFAIT ORANGE%'
                     OR UPPER(SUBS_BENEFIT_NAME) = 'IPP SASSAYE JEUNESSE'
                     OR UPPER(SUBS_BENEFIT_NAME) = 'IPP FAMILYTALK FOR SUNDAY TALK'
                     OR UPPER(SUBS_BENEFIT_NAME) LIKE 'IPP MAXI BONUS%'
                     OR UPPER(SUBS_BENEFIT_NAME) LIKE 'IPP ORANGE BONUS ALLNET%'
                     OR UPPER(SUBS_BENEFIT_NAME) LIKE 'IPP ORANGE BONUS INTER%'
                     OR UPPER(SUBS_BENEFIT_NAME) LIKE 'IPP ORANGE BONUS ONNET%'
                     OR UPPER(SUBS_BENEFIT_NAME) LIKE 'IPP ORANGE BONUS SUPER%'
                     OR UPPER(SUBS_BENEFIT_NAME) = 'IPP ORANGE BONUS CLASSIQUE'
                     OR UPPER(SUBS_BENEFIT_NAME) LIKE 'IPP GALAXY%'
                     OR UPPER(SUBS_BENEFIT_NAME) = 'INTERNETCM MOBILE ANNEE'
                     OR UPPER(SUBS_BENEFIT_NAME) = 'IPP FLYBOX CONFORT'
                     OR UPPER(SUBS_BENEFIT_NAME) = 'IPP INTERNATIONAL HAJJ JOUR'
                     OR UPPER(SUBS_BENEFIT_NAME) = 'IPP ORANGE BONUS NEW RATING_0'
                     OR UPPER(SUBS_BENEFIT_NAME) = 'IPP PREPAID MULTI SIM TRAP'
                     )
                GROUP BY TRANSACTION_DATE
                ,COMMERCIAL_OFFER
                ,OPERATOR_CODE
                ;
COMMIT;

      --Insertion du Nombre des souscriptions voix (Usage Orange Bundle)
-- @MAJ  par dimitri.happi@orange.cm: changement de la table de base : de FT_A_SUBSCRIPTION vers FT_CONSO_MSISDN_DAY (BUNLE_TEL)
INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
                SELECT
                EVENT_DATE TRANSACTION_DATE
                ,'USAGE_VOICE_BUNDLE' DESTINATION_CODE
                ,FORMULE PROFILE_CODE
                ,'UNKNOWN' SUB_ACCOUNT
                ,'HIT' MEASUREMENT_UNIT
                , 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE
                ,OPERATOR_CODE
                ,SUM(BUNDLE_TEL_DURATION) / 60 TOTAL_AMOUNT
                ,SUM(BUNDLE_TEL_DURATION) / 60 RATED_AMOUNT
                ,SYSDATE INSERT_DATE
                ,NULL REGION_ID
                    FROM MON.FT_CONSO_MSISDN_DAY
                WHERE EVENT_DATE IN ( SELECT datecode
                             FROM (SELECT DISTINCT datecode from DIM.DT_DATES
                                         WHERE datecode BETWEEN TRUNC(SYSDATE-subs_number_day_from) and TRUNC(SYSDATE-subs_number_day_bef)
                                       MINUS
                                  SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                        WHERE SOURCE_TABLE='FT_CONSO_MSISDN_DAY' AND DESTINATION_CODE='USAGE_VOICE_BUNDLE' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-subs_number_day_from) AND TRUNC(SYSDATE-subs_number_day_bef))
                                                    )
                GROUP BY EVENT_DATE
                ,FORMULE
                ,OPERATOR_CODE
                ;
/*
INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
                SELECT
                TRANSACTION_DATE
                ,'USAGE_VOICE_BUNDLE' DESTINATION_CODE
                ,COMMERCIAL_OFFER PROFILE_CODE
                ,'UNKNOWN' SUB_ACCOUNT
                ,'HIT' MEASUREMENT_UNIT
                , 'FT_A_SUBSCRIPTION' SOURCE_TABLE
                ,OPERATOR_CODE
                ,SUM(SUBS_TOTAL_COUNT) TOTAL_AMOUNT
                ,SUM(SUBS_TOTAL_COUNT) RATED_AMOUNT
                ,SYSDATE INSERT_DATE
                ,NULL REGION_ID
                FROM FT_A_SUBSCRIPTION
                WHERE TRANSACTION_DATE  IN ( SELECT datecode
                             FROM (SELECT DISTINCT datecode from DIM.DT_DATES
                                         WHERE datecode BETWEEN TRUNC(SYSDATE-subs_number_day_from) and TRUNC(SYSDATE-subs_number_day_bef)
                                       MINUS
                                  SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                        WHERE SOURCE_TABLE='FT_A_SUBSCRIPTION' AND DESTINATION_CODE='USAGE_VOICE_BUNDLE' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-subs_number_day_from) AND TRUNC(SYSDATE-subs_number_day_bef))
                                                    )
                AND (UPPER(SUBS_BENEFIT_NAME)='PREPAID INDIVIDUAL BUNDLE MONEY'   OR UPPER(SUBS_BENEFIT_NAME) LIKE 'FORFAITS ORANGE%' OR UPPER(SUBS_BENEFIT_NAME) LIKE 'FORFAIT ORANGE%')
                GROUP BY TRANSACTION_DATE
                ,COMMERCIAL_OFFER
                ,OPERATOR_CODE
                ;
                */
COMMIT;


--Insertion du Revenu des souscriptions SMS (Orange Bundle)
INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
                SELECT
                TRANSACTION_DATE
                ,'REVENUE_SMS_BUNDLE' DESTINATION_CODE
                ,COMMERCIAL_OFFER PROFILE_CODE
                ,'MAIN' SUB_ACCOUNT
                ,'HIT' MEASUREMENT_UNIT
                , 'FT_A_SUBSCRIPTION' SOURCE_TABLE
                ,OPERATOR_CODE
                ,SUM(SUBS_AMOUNT) TOTAL_AMOUNT
                ,SUM(SUBS_AMOUNT) RATED_AMOUNT
                ,SYSDATE INSERT_DATE
                ,NULL REGION_ID
                FROM FT_A_SUBSCRIPTION
                WHERE TRANSACTION_DATE IN ( SELECT datecode
                             FROM (SELECT DISTINCT datecode from DIM.DT_DATES
                                         WHERE datecode BETWEEN TRUNC(SYSDATE-subs_number_day_from) and TRUNC(SYSDATE-subs_number_day_bef)
                                       MINUS
                                  SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                        WHERE SOURCE_TABLE='FT_A_SUBSCRIPTION' AND DESTINATION_CODE='REVENUE_SMS_BUNDLE' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-subs_number_day_from) AND TRUNC(SYSDATE-subs_number_day_bef))
                                                    )
                AND UPPER(SUBS_BENEFIT_NAME) LIKE '%SMS%'
                GROUP BY TRANSACTION_DATE
                ,COMMERCIAL_OFFER
                ,OPERATOR_CODE
                ;
COMMIT;

--Insertion du Nombre de souscriptions SMS (Orange Bundle SMS)
-- @MAJ  par dimitri.happi@orange.cm: changement de la table de base : de FT_A_SUBSCRIPTION vers FT_CONSO_MSISDN_DAY (BUNLE_SMS)
INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
                SELECT
                EVENT_DATE TRANSACTION_DATE
                ,'USAGE_SMS_BUNDLE' DESTINATION_CODE
                ,FORMULE PROFILE_CODE
                ,'UNKNOWN' SUB_ACCOUNT
                ,'HIT' MEASUREMENT_UNIT
                , 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE
                ,OPERATOR_CODE
                ,SUM(BUNDLE_SMS_COUNT) TOTAL_AMOUNT
                ,SUM(BUNDLE_SMS_COUNT) RATED_AMOUNT
                ,SYSDATE INSERT_DATE
                ,NULL REGION_ID
                    FROM MON.FT_CONSO_MSISDN_DAY
                WHERE EVENT_DATE IN ( SELECT datecode
                             FROM (SELECT DISTINCT datecode from DIM.DT_DATES
                                         WHERE datecode BETWEEN TRUNC(SYSDATE-subs_number_day_from) and TRUNC(SYSDATE-subs_number_day_bef)
                                       MINUS
                                  SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                        WHERE SOURCE_TABLE='FT_CONSO_MSISDN_DAY' AND DESTINATION_CODE='USAGE_SMS_BUNDLE' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-subs_number_day_from) AND TRUNC(SYSDATE-subs_number_day_bef))
                                                    )
                GROUP BY EVENT_DATE
                ,FORMULE
                ,OPERATOR_CODE
                ;
COMMIT;

--Insertion du Revenu des souscriptions Data (2G Bundle)

INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
                    SELECT
                    TRANSACTION_DATE
                    ,'REVENUE_2G_BUNDLE' DESTINATION_CODE
                    ,COMMERCIAL_OFFER PROFILE_CODE
                    ,'MAIN' SUB_ACCOUNT
                    ,'HIT' MEASUREMENT_UNIT
                    ,'FT_A_SUBSCRIPTION' SOURCE_TABLE
                    ,OPERATOR_CODE
                    ,SUM(SUBS_AMOUNT) TOTAL_AMOUNT
                    ,SUM(SUBS_AMOUNT) RATED_AMOUNT
                    ,SYSDATE INSERT_DATE
                    ,NULL REGION_ID
                    FROM FT_A_SUBSCRIPTION
                    WHERE TRANSACTION_DATE  IN ( SELECT datecode
                                 FROM (SELECT DISTINCT datecode from DIM.DT_DATES
                                             WHERE datecode BETWEEN TRUNC(SYSDATE-subs_number_day_from) and TRUNC(SYSDATE-subs_number_day_bef)
                                           MINUS
                                      SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                            WHERE SOURCE_TABLE='FT_A_SUBSCRIPTION' AND DESTINATION_CODE='REVENUE_2G_BUNDLE' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-subs_number_day_from) AND TRUNC(SYSDATE-subs_number_day_bef))
                                                        )
                    AND  (NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'DATACM%' OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%' OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'ORANGECM%')
                    GROUP BY TRANSACTION_DATE
                    ,COMMERCIAL_OFFER
                    ,OPERATOR_CODE
                    ;
  COMMIT;

--Insertion du Revenu des souscriptions Data (3G Bundle)

INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
                    SELECT
                    TRANSACTION_DATE
                    ,'REVENUE_3G_BUNDLE' DESTINATION_CODE
                    ,COMMERCIAL_OFFER PROFILE_CODE
                    ,'MAIN' SUB_ACCOUNT
                    ,'HIT' MEASUREMENT_UNIT
                    ,'FT_A_SUBSCRIPTION' SOURCE_TABLE
                    ,OPERATOR_CODE
                    ,SUM(SUBS_AMOUNT) TOTAL_AMOUNT
                    ,SUM(SUBS_AMOUNT) RATED_AMOUNT
                    ,SYSDATE INSERT_DATE
                    ,NULL REGION_ID
                    FROM FT_A_SUBSCRIPTION
                    WHERE TRANSACTION_DATE  IN ( SELECT datecode
                                 FROM (SELECT DISTINCT datecode from DIM.DT_DATES
                                             WHERE datecode BETWEEN TRUNC(SYSDATE-subs_number_day_from) and TRUNC(SYSDATE-subs_number_day_bef)
                                           MINUS
                                      SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                            WHERE SOURCE_TABLE='FT_A_SUBSCRIPTION' AND DESTINATION_CODE='REVENUE_3G_BUNDLE' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-subs_number_day_from) AND TRUNC(SYSDATE-subs_number_day_bef))
                                                        )
                    AND  (NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%' OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%DATA%' OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%3G%' OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%')
                    GROUP BY TRANSACTION_DATE
                    ,COMMERCIAL_OFFER
                    ,OPERATOR_CODE
                    ;
  COMMIT;

END; -- P_INS_SUBSCRIPTION_REVENUE

----------------------------------------------------------------------------------------------------------------

PROCEDURE P_INS_SUBCRIPTION_LOCATION (p_number_day_from IN NUMBER DEFAULT 25,p_number_day_bef IN NUMBER DEFAULT 1)  IS

    /* ===>Source FT_A_GPRS_ACTIVITY
                .Revenu du tafic GPRS (Pay as you Go pour les prepayés uniquement)
     */

  number_day_from number:=p_number_day_from;
  number_day_bef number:=p_number_day_bef;
  d_start_date DATE;
  d_end_date DATE;
  s_val NUMBER;
  i_val NUMBER;
  s_slice_value VARCHAR2(15);
BEGIN
    d_start_date := TRUNC(SYSDATE - number_day_from);
    d_end_date := TRUNC(SYSDATE - number_day_bef);

    WHILE d_start_date <= d_end_date LOOP
        s_slice_value := TO_CHAR(d_start_date, 'yyyymmdd');
        -- Insert code here
        SELECT COUNT(*) INTO s_val FROM MON.FT_GLOBAL_ACTIVITY_DAILY_MKT
        WHERE SOURCE_TABLE='FT_SUBSCRIPTION_SITE_DAY'
            AND DESTINATION_CODE = 'REVENUE_LOCATION_SUBSCRIPTION'  AND TRANSACTION_DATE = d_start_date AND ROWNUM <= 10;

        SELECT mon.FN_VALIDATE_DAY2DAY_EXIST ('MON.FT_SUBSCRIPTION_SITE_DAY', 'EVENT_DATE'
                        , s_slice_value, s_slice_value, 10, '') INTO i_val FROM DUAL;

        IF s_val = 0 AND i_val = 1 THEN

            INSERT INTO MON.FT_GLOBAL_ACTIVITY_DAILY_MKT
            SELECT
                EVENT_DATE TRANSACTION_DATE,
                'REVENUE_LOCATION_SUBSCRIPTION' DESTINATION_CODE,
                COMMERCIAL_OFFER PROFILE_CODE,
                'MAIN' SUB_ACCOUNT,
                'HIT' MEASUREMENT_UNIT,
                'FT_SUBSCRIPTION_SITE_DAY' SOURCE_TABLE,
                OPERATOR_CODE,
                SUM(RATED_AMOUNT) TOTAL_AMOUNT,
                SUM(RATED_AMOUNT) RATED_AMOUNT,
                SYSDATE INSERT_DATE,
                r.REGION_ID
            FROM MON.FT_SUBSCRIPTION_SITE_DAY ul, DIM.DT_REGIONS_MKT r
            WHERE ul.EVENT_DATE  = d_start_date
                AND TRIM(COALESCE(ul.ADMINISTRATIVE_REGION, 'INCONNU')) = r.ADMINISTRATIVE_REGION
            GROUP BY EVENT_DATE,
                COMMERCIAL_OFFER,
                OPERATOR_CODE,
                r.REGION_ID;

             COMMIT;

        END IF;
        --
        d_start_date := d_start_date + 1;
    END LOOP;
END  ; --P_INS_SUBCRIPTION_LOCATION
----------------------------------------------------------------------------------------------------------------


PROCEDURE P_INS_GPRS_REVENUE (p_gprs_number_day_from IN NUMBER DEFAULT 25,p_gprs_number_day_bef IN NUMBER DEFAULT 1)  IS

    /* ===>Source FT_A_GPRS_ACTIVITY
                .Revenu du tafic GPRS (Pay as you Go pour les prepayés uniquement)
     */

  gprs_number_day_from number:=p_gprs_number_day_from;
  gprs_number_day_bef number:=p_gprs_number_day_bef;

BEGIN
-- Revenu GPRS PAYGO MAIN
INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
SELECT
DATECODE
,'REVENUE_2G_PAYGO' DESTINATION_CODE
,COMMERCIAL_OFFER PROFILE_CODE
,'MAIN' SUB_ACCOUNT
,'HIT' MEASUREMENT_UNIT
,'FT_A_GPRS_ACTIVITY' SOURCE_TABLE
,OPERATOR_CODE
,SUM(MAIN_COST) TOTAL_AMOUNT
,SUM(MAIN_COST) RATED_AMOUNT
,SYSDATE INSERT_DATE
,NULL REGION_ID
FROM FT_A_GPRS_ACTIVITY
WHERE DATECODE  IN  ( SELECT datecode
             FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                         WHERE datecode BETWEEN TRUNC(SYSDATE-gprs_number_day_from) and TRUNC(SYSDATE-gprs_number_day_bef)
                       MINUS
                  SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                        WHERE SOURCE_TABLE='FT_A_GPRS_ACTIVITY' AND SUB_ACCOUNT='MAIN' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-gprs_number_day_from) AND TRUNC(SYSDATE-gprs_number_day_bef))
                                    )
AND MAIN_COST>0
GROUP BY DATECODE
,COMMERCIAL_OFFER
,OPERATOR_CODE
;
COMMIT;
-- Revenu GPRS PAYGO PROMO

INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
SELECT
DATECODE
,'REVENUE_2G_PAYGO' DESTINATION_CODE
,COMMERCIAL_OFFER PROFILE_CODE
,'PROMO' SUB_ACCOUNT
,'HIT' MEASUREMENT_UNIT
,'FT_A_GPRS_ACTIVITY' SOURCE_TABLE
,OPERATOR_CODE
,SUM(PROMO_COST) TOTAL_AMOUNT
,SUM(PROMO_COST) RATED_AMOUNT
,SYSDATE INSERT_DATE
,NULL REGION_ID
FROM FT_A_GPRS_ACTIVITY
WHERE DATECODE IN  ( SELECT datecode
             FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                         WHERE datecode BETWEEN TRUNC(SYSDATE-gprs_number_day_from) and TRUNC(SYSDATE-gprs_number_day_bef)
                       MINUS
                  SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                        WHERE SOURCE_TABLE='FT_A_GPRS_ACTIVITY' AND SUB_ACCOUNT='PROMO' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-gprs_number_day_from) AND TRUNC(SYSDATE-gprs_number_day_bef))
                                    )
AND PROMO_COST>0
GROUP BY DATECODE
,COMMERCIAL_OFFER
,OPERATOR_CODE
;
COMMIT;

-- Volume echangé GPRS PAYGO (Usage en Go)

INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
SELECT
DATECODE
,'USAGE_2G_PAYGO' DESTINATION_CODE
,COMMERCIAL_OFFER PROFILE_CODE
,'UNKNOWN' SUB_ACCOUNT
,'HIT' MEASUREMENT_UNIT
,'FT_A_GPRS_ACTIVITY' SOURCE_TABLE
,OPERATOR_CODE
,SUM(BILLED_UNIT/(1024*1024*1024)) TOTAL_AMOUNT
,SUM(BILLED_UNIT/(1024*1024*1024)) RATED_AMOUNT
,SYSDATE INSERT_DATE
,NULL REGION_ID
FROM FT_A_GPRS_ACTIVITY
WHERE DATECODE IN  ( SELECT datecode
             FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                         WHERE datecode BETWEEN TRUNC(SYSDATE-gprs_number_day_from) and TRUNC(SYSDATE-gprs_number_day_bef)
                       MINUS
                  SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                        WHERE SOURCE_TABLE='FT_A_GPRS_ACTIVITY' AND DESTINATION_CODE='USAGE_2G_PAYGO' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-gprs_number_day_from) AND TRUNC(SYSDATE-gprs_number_day_bef))
                                    )
GROUP BY DATECODE
,COMMERCIAL_OFFER
,OPERATOR_CODE
;
COMMIT;

-- Volume echangé GPRS Bundle (Usage en Go)

INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
SELECT
DATECODE
,'USAGE_2G_BUNDLE' DESTINATION_CODE
,COMMERCIAL_OFFER PROFILE_CODE
,'UNKNOWN' SUB_ACCOUNT
,'HIT' MEASUREMENT_UNIT
,'FT_A_GPRS_ACTIVITY' SOURCE_TABLE
,OPERATOR_CODE
,SUM(BUCKET_VALUE/(1024*1024*1024)) TOTAL_AMOUNT
,SUM(BUCKET_VALUE/(1024*1024*1024)) RATED_AMOUNT
,SYSDATE INSERT_DATE
,NULL REGION_ID
FROM FT_A_GPRS_ACTIVITY
WHERE DATECODE IN  ( SELECT datecode
             FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                         WHERE datecode BETWEEN TRUNC(SYSDATE-gprs_number_day_from) and TRUNC(SYSDATE-gprs_number_day_bef)
                       MINUS
                  SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                        WHERE SOURCE_TABLE='FT_A_GPRS_ACTIVITY' AND DESTINATION_CODE='USAGE_2G_BUNDLE' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-gprs_number_day_from) AND TRUNC(SYSDATE-gprs_number_day_bef))
                                    )
GROUP BY DATECODE
,COMMERCIAL_OFFER
,OPERATOR_CODE
;
COMMIT;
END  ; --P_INS_GPRS_REVENUE

-------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE P_INS_GPRS_LOC_REVENUE (p_gprs_loc_number_day_from IN NUMBER DEFAULT 25,p_gprs_loc_number_day_bef IN NUMBER DEFAULT 1)  IS

    /* ===>Source  FT_A_GPRS_LOCATION
                .Revenu du trafic GPRS par localisation (Pay as you Go pour les prepayés uniquement)
     */

  gprs_loc_number_day_from number:=p_gprs_loc_number_day_from;
  gprs_loc_number_day_bef number:=p_gprs_loc_number_day_bef;
/*
  n_is_empty NUMBER;
BEGIN


SELECT COUNT(*) INTO n_is_empty FROM MON.VW_SDT_LAC_INFO;

-- si le calcule pour slice peut être effectué sans pb
IF n_is_empty <> 0 THEN
    -- Revenu GPRS PAYGO par localisation MAIN
    INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
    SELECT
                     TRANSACTION_DATE
                    , DESTINATION_CODE
                    , PROFILE_CODE
                    , SUB_ACCOUNT
                    , MEASUREMENT_UNIT
                    , SOURCE_TABLE
                    , OPERATOR_CODE
                    , TOTAL_AMOUNT
                    , RATED_AMOUNT
                    , SYSDATE INSERT_DATE
                    , REGION_ID
    FROM (
    SELECT
    SESSION_DATE TRANSACTION_DATE
    ,'REVENUE_2G_PAYGO_LOC' DESTINATION_CODE
    ,SERVED_PARTY_OFFER PROFILE_CODE
    ,'MAIN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    ,'FT_A_GPRS_LOCATION' SOURCE_TABLE
    ,OPERATOR_CODE
    ,SUM(MAIN_COST) TOTAL_AMOUNT
    ,SUM(MAIN_COST) RATED_AMOUNT
    ,SYSDATE INSERT_DATE
    , vs.ADMINISTRATIVE_REGION
    FROM FT_A_GPRS_LOCATION a, VW_SDT_LAC_INFO vs
    WHERE SESSION_DATE  IN  ( SELECT datecode
                 FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                             WHERE datecode BETWEEN TRUNC(SYSDATE-gprs_loc_number_day_from) and TRUNC(SYSDATE-gprs_loc_number_day_bef)
                           MINUS
                      SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                            WHERE SOURCE_TABLE='FT_A_GPRS_LOCATION' AND SUB_ACCOUNT='MAIN' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-gprs_loc_number_day_from) AND TRUNC(SYSDATE-gprs_loc_number_day_bef))
                                        )
    AND MAIN_COST>0
     AND LOCATION_LAC= vs.LAC(+)
    GROUP BY SESSION_DATE
    ,SERVED_PARTY_OFFER
    ,OPERATOR_CODE
    ,vs.ADMINISTRATIVE_REGION) f, DIM.DT_REGIONS_MKT r
    WHERE TRIM(COALESCE(f.ADMINISTRATIVE_REGION, 'INCONNU')) = r.ADMINISTRATIVE_REGION
    ;
    COMMIT;
    -- Revenu GPRS PAYGO PROMO par localisation

    INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
    SELECT
    TRANSACTION_DATE
    , DESTINATION_CODE
    , PROFILE_CODE
    , SUB_ACCOUNT
    , MEASUREMENT_UNIT
    , SOURCE_TABLE
    , OPERATOR_CODE
    , TOTAL_AMOUNT
    , RATED_AMOUNT
    , SYSDATE INSERT_DATE
    , REGION_ID
    FROM (SELECT
                        SESSION_DATE TRANSACTION_DATE
                        ,'REVENUE_2G_PAYGO_LOC' DESTINATION_CODE
                        ,SERVED_PARTY_OFFER PROFILE_CODE
                        ,'PROMO' SUB_ACCOUNT
                        ,'HIT' MEASUREMENT_UNIT
                        ,'FT_A_GPRS_LOCATION' SOURCE_TABLE
                        ,OPERATOR_CODE
                        ,SUM(PROMO_COST) TOTAL_AMOUNT
                        ,SUM(PROMO_COST) RATED_AMOUNT
                        ,SYSDATE INSERT_DATE
                        ,vs.ADMINISTRATIVE_REGION
    FROM FT_A_GPRS_LOCATION a, VW_SDT_LAC_INFO vs
    WHERE SESSION_DATE IN  ( SELECT datecode
                 FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                             WHERE datecode BETWEEN TRUNC(SYSDATE-gprs_loc_number_day_from) and TRUNC(SYSDATE-gprs_loc_number_day_bef)
                           MINUS
                      SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                            WHERE SOURCE_TABLE='FT_A_GPRS_LOCATION' AND SUB_ACCOUNT='PROMO' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-gprs_loc_number_day_from) AND TRUNC(SYSDATE-gprs_loc_number_day_bef))
                                        )
     AND LOCATION_LAC = vs.LAC(+)
    AND PROMO_COST>0
    GROUP BY SESSION_DATE
    ,SERVED_PARTY_OFFER
    ,OPERATOR_CODE
    ,vs.ADMINISTRATIVE_REGION ) f, DIM.DT_REGIONS_MKT r
    WHERE TRIM(COALESCE(f.ADMINISTRATIVE_REGION, 'INCONNU')) = r.ADMINISTRATIVE_REGION
    ;
    COMMIT;
END IF ;

END  ;
*/
  d_start_date DATE;
  d_end_date DATE;
  s_val NUMBER;
  i_val NUMBER;
  s_slice_value VARCHAR2(15);
  n_is_empty INT;
BEGIN
    d_start_date := TRUNC(SYSDATE - gprs_loc_number_day_from);
    d_end_date := TRUNC(SYSDATE - gprs_loc_number_day_bef);

    WHILE d_start_date <= d_end_date LOOP
        s_slice_value := TO_CHAR(d_start_date, 'yyyymmdd');
        -- Insert code here
        SELECT COUNT(*) INTO s_val FROM MON.FT_GLOBAL_ACTIVITY_DAILY_MKT
        WHERE SOURCE_TABLE='FT_A_GPRS_LOCATION'  AND TRANSACTION_DATE = d_start_date AND ROWNUM <= 10;

        SELECT COUNT(*) INTO n_is_empty FROM MON.VW_SDT_LAC_INFO;

        SELECT mon.FN_VALIDATE_DAY2DAY_EXIST ('MON.FT_A_GPRS_LOCATION', 'SESSION_DATE'
                        , s_slice_value, s_slice_value, 10, '') INTO i_val FROM DUAL;

        IF s_val = 0 AND i_val = 1 AND n_is_empty <> 0 THEN

            -- Revenu GPRS PAYGO par localisation MAIN
            INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
            SELECT
                 TRANSACTION_DATE
                , DESTINATION_CODE
                , PROFILE_CODE
                , SUB_ACCOUNT
                , MEASUREMENT_UNIT
                , SOURCE_TABLE
                , OPERATOR_CODE
                , TOTAL_AMOUNT
                , RATED_AMOUNT
                , SYSDATE INSERT_DATE
                , REGION_ID
            FROM (
                SELECT
                SESSION_DATE TRANSACTION_DATE
                ,'REVENUE_2G_PAYGO_LOC' DESTINATION_CODE
                ,SERVED_PARTY_OFFER PROFILE_CODE
                ,'MAIN' SUB_ACCOUNT
                ,'HIT' MEASUREMENT_UNIT
                ,'FT_A_GPRS_LOCATION' SOURCE_TABLE
                ,OPERATOR_CODE
                ,SUM(MAIN_COST) TOTAL_AMOUNT
                ,SUM(MAIN_COST) RATED_AMOUNT
                ,SYSDATE INSERT_DATE
                , vs.ADMINISTRATIVE_REGION
                FROM FT_A_GPRS_LOCATION a, VW_SDT_LAC_INFO vs
                WHERE SESSION_DATE  = d_start_date
                AND MAIN_COST>0
                 AND LOCATION_LAC= vs.LAC(+)
                GROUP BY SESSION_DATE
                ,SERVED_PARTY_OFFER
                ,OPERATOR_CODE
                ,vs.ADMINISTRATIVE_REGION) f, DIM.DT_REGIONS_MKT r
            WHERE TRIM(COALESCE(f.ADMINISTRATIVE_REGION, 'INCONNU')) = r.ADMINISTRATIVE_REGION;

            -- Revenu GPRS PAYGO PROMO par localisation
            INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
            SELECT
            TRANSACTION_DATE
            , DESTINATION_CODE
            , PROFILE_CODE
            , SUB_ACCOUNT
            , MEASUREMENT_UNIT
            , SOURCE_TABLE
            , OPERATOR_CODE
            , TOTAL_AMOUNT
            , RATED_AMOUNT
            , SYSDATE INSERT_DATE
            , REGION_ID
            FROM (SELECT
                    SESSION_DATE TRANSACTION_DATE
                    ,'REVENUE_2G_PAYGO_LOC' DESTINATION_CODE
                    ,SERVED_PARTY_OFFER PROFILE_CODE
                    ,'PROMO' SUB_ACCOUNT
                    ,'HIT' MEASUREMENT_UNIT
                    ,'FT_A_GPRS_LOCATION' SOURCE_TABLE
                    ,OPERATOR_CODE
                    ,SUM(PROMO_COST) TOTAL_AMOUNT
                    ,SUM(PROMO_COST) RATED_AMOUNT
                    ,SYSDATE INSERT_DATE
                    ,vs.ADMINISTRATIVE_REGION
            FROM FT_A_GPRS_LOCATION a, VW_SDT_LAC_INFO vs
            WHERE SESSION_DATE   = d_start_date
             AND LOCATION_LAC = vs.LAC(+)
            AND PROMO_COST>0
            GROUP BY SESSION_DATE
            ,SERVED_PARTY_OFFER
            ,OPERATOR_CODE
            ,vs.ADMINISTRATIVE_REGION ) f, DIM.DT_REGIONS_MKT r
            WHERE TRIM(COALESCE(f.ADMINISTRATIVE_REGION, 'INCONNU')) = r.ADMINISTRATIVE_REGION;

            COMMIT;

        END IF;
        --
        d_start_date := d_start_date + 1;
    END LOOP;
END;
--P_INS_GPRS_LOC_REVENUE


--------------------------------------------------------------------------------------------------------------------------------------

PROCEDURE P_INS_UNIQUE_USER (p_user_day_number_day_from IN NUMBER DEFAULT 25,p_user_day_number_day_bef IN NUMBER DEFAULT 1) IS

user_day_number_day_from number:=p_user_day_number_day_from;
user_day_number_day_bef number:=p_user_day_number_day_bef;

   /* ===>Source FT_USERS_DAY et FT_USERS_DATA_DAY
                Insertion des utilisateurs Unique
     */

    BEGIN
       -- Insertion des Users uniques  Voix et SMS
INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
SELECT
    EVENT_DATE TRANSACTION_DATE,
    (CASE
        WHEN DESTINATION='ONNET' AND SERVICE='NVX_SMS' THEN 'USER_SMS_ONNET'
        WHEN DESTINATION='BUNDLE' AND SERVICE='NVX_SMS' THEN 'USER_SMS_BUNDLE'
        WHEN DESTINATION='MTN' AND SERVICE='NVX_SMS' THEN 'USER_SMS_MTN'
        WHEN DESTINATION='NEXTTEL' AND SERVICE='NVX_SMS' THEN 'USER_SMS_NEXTTEL'
        WHEN DESTINATION='CAMTEL' AND SERVICE='NVX_SMS' THEN 'USER_SMS_CAMTEL'
        WHEN DESTINATION='INTERNATIONAL' AND SERVICE='NVX_SMS' THEN 'USER_SMS_INTERNATIONAL'
        WHEN DESTINATION='ROAM' AND SERVICE='NVX_SMS' THEN 'USER_SMS_ROAM'
        WHEN DESTINATION='INROAM' AND SERVICE='NVX_SMS' THEN 'USER_SMS_INROAM'
        WHEN DESTINATION='ONNET' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_ONNET'
        WHEN DESTINATION='BUNDLE' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_BUNDLE'
        WHEN DESTINATION='MTN' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_MTN'
        WHEN DESTINATION='NEXTTEL' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_NEXTTEL'
        WHEN DESTINATION='CAMTEL' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_CAMTEL'
        WHEN DESTINATION='INTERNATIONAL' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_INTERNATIONAL'
        WHEN DESTINATION='ROAM' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_ROAM'
        WHEN DESTINATION='INROAM' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_INROAM'
    END) DESTINATION_CODE,
    FORMULE PROFILE_CODE,
    'UNKNOWN' SUB_ACCOUNT,
    'HIT' MEASUREMENT_UNIT,
    'FT_USERS_DAY' SOURCE_TABLE,
     OPERATOR_CODE,
    SUM(USERS_COUNT) TOTAL_AMOUNT,
    SUM(USERS_COUNT) RATED_AMOUNT,
    SYSDATE INSERT_DATE ,
    NULL REGION_ID
FROM
(
    SELECT
        ud.EVENT_DATE,
        ud.FORMULE,
        TRIM(ud.SERVICE) SERVICE,
        TRIM(d.DESTINATION) DESTINATION,
        OPERATOR_CODE,
        (CASE
            WHEN d.DESTINATION='ONNET' THEN ud.ONNET
            WHEN d.DESTINATION='BUNDLE' THEN ud.BUNDLE
            WHEN d.DESTINATION='MTN' THEN ud.MTN
            WHEN d.DESTINATION='NEXTTEL' THEN ud.NEXTTEL
            WHEN d.DESTINATION='CAMTEL' THEN ud.CAMTEL
            WHEN d.DESTINATION='INTERNATIONAL' THEN ud.INTERNATIONAL
            WHEN d.DESTINATION='ROAM' THEN ud.ROAM
            WHEN d.DESTINATION='SET' THEN ud."SET"
            WHEN d.DESTINATION='INROAM' THEN ud.INROAM
        END) USERS_COUNT
    FROM MON.FT_USERS_DAY ud
    CROSS JOIN
    (
        SELECT 'ONNET' DESTINATION  FROM DUAL UNION ALL
        SELECT 'BUNDLE' DESTINATION  FROM DUAL UNION ALL
        SELECT 'MTN' DESTINATION FROM DUAL UNION ALL
        SELECT 'NEXTTEL' DESTINATION FROM DUAL UNION ALL
        SELECT 'CAMTEL' DESTINATION FROM DUAL UNION ALL
        SELECT 'INTERNATIONAL' DESTINATION FROM DUAL UNION ALL
        SELECT 'ROAM' DESTINATION FROM DUAL UNION ALL
        SELECT 'SET' DESTINATION FROM DUAL UNION ALL
        SELECT 'INROAM' DESTINATION FROM DUAL
    ) d
    WHERE EVENT_DATE IN  ( SELECT datecode
             FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                         WHERE datecode BETWEEN TRUNC(SYSDATE-user_day_number_day_from) and TRUNC(SYSDATE-user_day_number_day_bef)
                       MINUS
                  SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                        WHERE SOURCE_TABLE='FT_USERS_DAY'  AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-user_day_number_day_from) AND TRUNC(SYSDATE-user_day_number_day_bef))
                                    )
        AND TRIM(SERVICE) <> 'ALL'
)
WHERE DESTINATION <> 'SET'
GROUP BY
    EVENT_DATE,
    OPERATOR_CODE,
    (CASE
        WHEN DESTINATION='ONNET' AND SERVICE='NVX_SMS' THEN 'USER_SMS_ONNET'
        WHEN DESTINATION='BUNDLE' AND SERVICE='NVX_SMS' THEN 'USER_SMS_BUNDLE'
        WHEN DESTINATION='MTN' AND SERVICE='NVX_SMS' THEN 'USER_SMS_MTN'
        WHEN DESTINATION='NEXTTEL' AND SERVICE='NVX_SMS' THEN 'USER_SMS_NEXTTEL'
        WHEN DESTINATION='CAMTEL' AND SERVICE='NVX_SMS' THEN 'USER_SMS_CAMTEL'
        WHEN DESTINATION='INTERNATIONAL' AND SERVICE='NVX_SMS' THEN 'USER_SMS_INTERNATIONAL'
        WHEN DESTINATION='ROAM' AND SERVICE='NVX_SMS' THEN 'USER_SMS_ROAM'
        WHEN DESTINATION='INROAM' AND SERVICE='NVX_SMS' THEN 'USER_SMS_INROAM'
        WHEN DESTINATION='ONNET' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_ONNET'
        WHEN DESTINATION='BUNDLE' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_BUNDLE'
        WHEN DESTINATION='MTN' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_MTN'
        WHEN DESTINATION='NEXTTEL' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_NEXTTEL'
        WHEN DESTINATION='CAMTEL' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_CAMTEL'
        WHEN DESTINATION='INTERNATIONAL' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_INTERNATIONAL'
        WHEN DESTINATION='ROAM' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_ROAM'
        WHEN DESTINATION='INROAM' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_INROAM'
    END) ,
    FORMULE
;
    COMMIT;

    -- Insertion des Users uniques  DATA
    INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
    SELECT
        EVENT_DATE TRANSACTION_DATE,
        DESTINATION_CODE,
        PROFILE_CODE,
        'UNKNOWN' SUB_ACCOUNT,
        'UNKNOWN' MEASUREMENT_UNIT,
        'FT_USERS_DATA_DAY' SOURCE_TABLE,
         OPERATOR_CODE,
        SUM(TOTAL_AMOUNT) TOTAL_AMOUNT,
        SUM(RATED_AMOUNT) RATED_AMOUNT,
        SYSDATE INSERT_DATE,
        NULL AS REGION_ID
    FROM
    (
        SELECT
            EVENT_DATE,
            CASE
                WHEN SERVICE_DATA = '2G_PAYGO' THEN 'USER_2G_PAYGO'
                WHEN SERVICE_DATA = '2G_BUNDLE' THEN 'USER_2G_BUNDLE'
            END DESTINATION_CODE,
            COMMERCIAL_OFFER PROFILE_CODE,
             OPERATOR_CODE,
            CASE
                WHEN SERVICE_DATA = '2G_PAYGO' THEN OUT_BUNDLE_COUNT
                WHEN SERVICE_DATA = '2G_BUNDLE' THEN IN_BUNDLE_COUNT
            END TOTAL_AMOUNT,
            CASE
                WHEN SERVICE_DATA = '2G_PAYGO' THEN OUT_BUNDLE_COUNT
                WHEN SERVICE_DATA = '2G_BUNDLE' THEN IN_BUNDLE_COUNT
            END RATED_AMOUNT,
            SYSDATE INSERT_DATE
        FROM
        (
            SELECT '2G_PAYGO' SERVICE_DATA FROM DUAL UNION
            SELECT '2G_BUNDLE' SERVICE_DATA FROM DUAL
        ) a
        CROSS JOIN FT_USERS_DATA_DAY b
        WHERE EVENT_DATE IN ( SELECT datecode
                             FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                                         WHERE datecode BETWEEN TRUNC(SYSDATE-user_day_number_day_from) and TRUNC(SYSDATE-user_day_number_day_bef)
                                       MINUS
                                  SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                        WHERE SOURCE_TABLE='FT_USERS_DATA_DAY'  AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-user_day_number_day_from) AND TRUNC(SYSDATE-user_day_number_day_bef))
                            )
    )
    GROUP BY EVENT_DATE, DESTINATION_CODE, PROFILE_CODE, OPERATOR_CODE;

    COMMIT;

END; -- P_INS_UNIQUE_USER

-------------------------------------------------------------------------------------------------------------------------------------

PROCEDURE P_INS_USER_LOCATION (p_user_loc_number_day_from IN NUMBER DEFAULT 25,p_user_loc_number_day_bef IN NUMBER DEFAULT 1) IS

user_loc_number_day_from number:=p_user_loc_number_day_from;
user_loc_number_day_bef number:=p_user_loc_number_day_bef;

   /* ===>Source FT_USERS_REGION_LOCATION
                Insertion des utilisateurs Unique
     */

    BEGIN
-- Insertion des utilisateurs unique par localisation
INSERT INTO FT_GLOBAL_ACTIVITY_DAILY_MKT
SELECT
    EVENT_DATE TRANSACTION_DATE,
    'USER_LOCATION' DESTINATION_CODE,
    FORMULE PROFILE_CODE,
    'UNKNOWN' SUB_ACCOUNT,
    'HIT' MEASUREMENT_UNIT,
    'FT_USERS_REGION_LOCATION' SOURCE_TABLE,
     OPERATOR_CODE,
    SUM(USERS_COUNT) TOTAL_AMOUNT,
    SUM(USERS_COUNT) RATED_AMOUNT,
    SYSDATE INSERT_DATE,
    r.REGION_ID
FROM FT_USERS_REGION_LOCATION ul, DIM.DT_REGIONS_MKT r
WHERE ul.EVENT_DATE  IN ( SELECT datecode
                                     FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                                                 WHERE datecode BETWEEN TRUNC(SYSDATE-user_loc_number_day_from) AND TRUNC(SYSDATE-user_loc_number_day_bef)
                                               MINUS
                                          SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                                WHERE SOURCE_TABLE='FT_USERS_REGION_LOCATION'  AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-user_loc_number_day_from) AND TRUNC(SYSDATE-user_loc_number_day_bef))
                                                            )
    AND TRIM(COALESCE(ul.ADMINISTRATIVE_REGION, 'INCONNU')) = r.ADMINISTRATIVE_REGION
GROUP BY EVENT_DATE,
    FORMULE,
    OPERATOR_CODE,
    r.REGION_ID;

    COMMIT;
END; -- P_INS_USER_LOCATION
----------------------------------------------------------------------------------------------------------------

PROCEDURE P_INS_REFILL_TRAFFIC (p_refill_number_day_from IN NUMBER DEFAULT 25,p_refill_number_day_bef IN NUMBER DEFAULT 1)  IS

refill_number_day_from number:=p_refill_number_day_from;
refill_number_day_bef number:=p_refill_number_day_bef;

  BEGIN
-- Insertion des recharges C2S
  --Insersion en volume
INSERT INTO FT_REFILL_SLICE_MKT
                    SELECT     REFILL_DATE TRANSACTION_DATE
                    ,'VOLUME' KPI
                    ,REFILL_MEAN
                    ,b.SLICE_ID
                    ,RECEIVER_PROFILE PROFILE_CODE
                    ,'FT_REFILL' SOURCE_TABLE
                    ,RECEIVER_OPERATOR_CODE OPERATOR_CODE
                    ,SUM(1) TOTAL_AMOUNT
                    ,SYSDATE INSERT_DATE
                    FROM FT_REFILL a,DIM.DT_REFILL_SLICES_MKT b
                    WHERE  REFILL_MEAN IN ('C2S')
                    AND (REFILL_AMOUNT BETWEEN SLICE_INF_VALUE AND SLICE_SUP_VALUE)
                    AND TERMINATION_IND='200'
                    AND REFILL_DATE  IN ( SELECT datecode
             FROM (SELECT DISTINCT datecode FROM dim.DT_DATES
                         WHERE datecode BETWEEN TRUNC(SYSDATE-refill_number_day_from) AND TRUNC(SYSDATE-refill_number_day_bef)
                       MINUS
                  SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_REFILL_SLICE_MKT
                        WHERE SOURCE_TABLE='FT_REFILL' AND REFILL_MEAN IN ('C2S') AND KPI='VOLUME' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-refill_number_day_from) and TRUNC(SYSDATE-refill_number_day_bef))
                                    )
                    GROUP BY
                    REFILL_DATE
                    ,REFILL_MEAN
                    ,b.SLICE_ID
                    ,RECEIVER_PROFILE
                    ,RECEIVER_OPERATOR_CODE
                    ;
COMMIT;

  --Insersion en Valeur

INSERT INTO FT_REFILL_SLICE_MKT
                    SELECT     REFILL_DATE TRANSACTION_DATE
                    ,'VALEUR' KPI
                    ,REFILL_MEAN
                    ,b.SLICE_ID
                    ,RECEIVER_PROFILE PROFILE_CODE
                    ,'FT_REFILL' SOURCE_TABLE
                    ,RECEIVER_OPERATOR_CODE OPERATOR_CODE
                    ,SUM(REFILL_AMOUNT) TOTAL_AMOUNT
                    ,SYSDATE INSERT_DATE
                    FROM FT_REFILL a,DIM.DT_REFILL_SLICES_MKT b
                    WHERE  REFILL_MEAN IN ('C2S')
                    AND (REFILL_AMOUNT BETWEEN SLICE_INF_VALUE AND SLICE_SUP_VALUE)
                    AND TERMINATION_IND='200'
                    AND REFILL_DATE IN ( SELECT datecode
                                 FROM (SELECT DISTINCT datecode FROM dim.DT_DATES
                                             WHERE datecode BETWEEN TRUNC(SYSDATE-refill_number_day_from) AND TRUNC(SYSDATE-refill_number_day_bef)
                                           MINUS
                                      SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_REFILL_SLICE_MKT
                                            WHERE SOURCE_TABLE='FT_REFILL' AND REFILL_MEAN IN ('C2S') AND KPI='VALEUR' AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-refill_number_day_from) and TRUNC(SYSDATE-refill_number_day_bef))
                                                        )
                    GROUP BY
                    REFILL_DATE
                    ,REFILL_MEAN
                    ,b.SLICE_ID
                    ,RECEIVER_PROFILE
                    ,RECEIVER_OPERATOR_CODE
                    ;
COMMIT;
-----
-- Insertion des recharges CAG
  --Insersion en volume
INSERT INTO FT_REFILL_SLICE_MKT
                    SELECT     REFILL_DATE TRANSACTION_DATE
                    ,'VOLUME' KPI
                    ,REFILL_MEAN
                    ,b.SLICE_ID
                    ,RECEIVER_PROFILE PROFILE_CODE
                    ,'FT_REFILL' SOURCE_TABLE
                    ,RECEIVER_OPERATOR_CODE OPERATOR_CODE
                    ,SUM(1) TOTAL_AMOUNT
                    ,SYSDATE INSERT_DATE
                    FROM FT_REFILL a,DIM.DT_REFILL_SLICES_MKT b
                    WHERE  REFILL_MEAN IN ('SCRATCH')
                    AND (REFILL_AMOUNT BETWEEN SLICE_INF_VALUE AND SLICE_SUP_VALUE)
                    AND TERMINATION_IND='200'
                    AND REFILL_DATE  IN ( SELECT datecode
             FROM (SELECT DISTINCT datecode FROM dim.DT_DATES
                         WHERE datecode BETWEEN TRUNC(SYSDATE-refill_number_day_from) AND TRUNC(SYSDATE-refill_number_day_bef)
                       MINUS
                  SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_REFILL_SLICE_MKT
                        WHERE SOURCE_TABLE='FT_REFILL' AND KPI='VOLUME' AND REFILL_MEAN IN ('SCRATCH')  AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-refill_number_day_from) and TRUNC(SYSDATE-refill_number_day_bef))
                                    )
                    GROUP BY
                    REFILL_DATE
                    ,REFILL_MEAN
                    ,b.SLICE_ID
                    ,RECEIVER_PROFILE
                    ,RECEIVER_OPERATOR_CODE
                    ;
COMMIT;

  --Insersion en Valeur

INSERT INTO FT_REFILL_SLICE_MKT
                    SELECT     REFILL_DATE TRANSACTION_DATE
                    ,'VALEUR' KPI
                    ,REFILL_MEAN
                    ,b.SLICE_ID
                    ,RECEIVER_PROFILE PROFILE_CODE
                    ,'FT_REFILL' SOURCE_TABLE
                    ,RECEIVER_OPERATOR_CODE OPERATOR_CODE
                    ,SUM(REFILL_AMOUNT) TOTAL_AMOUNT
                    ,SYSDATE INSERT_DATE
                    FROM FT_REFILL a,DIM.DT_REFILL_SLICES_MKT b
                    WHERE  REFILL_MEAN IN ('SCRATCH')
                    AND (REFILL_AMOUNT BETWEEN SLICE_INF_VALUE AND SLICE_SUP_VALUE)
                    AND TERMINATION_IND='200'
                    AND REFILL_DATE IN ( SELECT datecode
                                 FROM (SELECT DISTINCT datecode FROM dim.DT_DATES
                                             WHERE datecode BETWEEN TRUNC(SYSDATE-refill_number_day_from) AND TRUNC(SYSDATE-refill_number_day_bef)
                                           MINUS
                                      SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_REFILL_SLICE_MKT
                                            WHERE SOURCE_TABLE='FT_REFILL' AND KPI='VALEUR' AND REFILL_MEAN IN ('SCRATCH') AND TRANSACTION_DATE BETWEEN TRUNC(SYSDATE-refill_number_day_from) and TRUNC(SYSDATE-refill_number_day_bef))
                                                        )
                    GROUP BY
                    REFILL_DATE
                    ,REFILL_MEAN
                    ,b.SLICE_ID
                    ,RECEIVER_PROFILE
                    ,RECEIVER_OPERATOR_CODE
                    ;
COMMIT;

 END; -- P_INS_REFILL_TRAFFIC
  ----------------------------------------------------------------------------------------------------------------

PROCEDURE P_INS_CUSTOMER_BASE (p_cust_base_number_day_from IN NUMBER DEFAULT 25,p_cust_base_number_day_bef IN NUMBER DEFAULT 1)  IS

cust_base_number_day_from number:=p_cust_base_number_day_from;
cust_base_number_day_bef number:=p_cust_base_number_day_bef;

  BEGIN
-- Insertion des recharges
  --Insersion en volume
insert into FT_GLOBAL_ACTIVITY_DAILY_MKT
select event_date-1 TRANSACTION_DATE,'USER_GROUP' DESTINATION_CODE, PROFILE PROFILE_CODE,'UNKNOWN' SUB_ACCOUNT,'HIT' MEASUREMENT_UNIT
,'FT_GROUP_SUBSCRIBER_SUMMARY' SOURCE_TABLE,NVL(b.OPERATOR_CODE,'OCM')OPERATOR_CODE ,sum(EFFECTIF) TOTAL_AMOUNT,sum(EFFECTIF) RATED_AMOUNT
,sysdate INSERT_DATE,'' REGION_ID from MON.FT_GROUP_SUBSCRIBER_SUMMARY  a,DIM.DT_OFFER_PROFILES b
where event_date in ( SELECT datecode
                                FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                                                 WHERE datecode BETWEEN TRUNC(SYSDATE-cust_base_number_day_from) AND TRUNC(SYSDATE-cust_base_number_day_bef)
                                               MINUS
                                          SELECT DISTINCT TRANSACTION_DATE+1 datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                                WHERE SOURCE_TABLE='FT_GROUP_SUBSCRIBER_SUMMARY' AND TRANSACTION_DATE BETWEEN trunc(sysdate-cust_base_number_day_from-2) and trunc(sysdate-cust_base_number_day_bef+1))
                                                            )
and statut = 'ACTIF' and CUST_BILLCYCLE in ( 'PURE PREPAID','HYBRID')  and upper(a.PROFILE) = b.PROFILE_CODE(+)
group by EVENT_DATE,profile,NVL(b.OPERATOR_CODE,'OCM');
COMMIT;
---------------------------------------------
-- Regroupement des 5 INSERT
---------------------------------------------
insert into FT_GLOBAL_ACTIVITY_DAILY_MKT
SELECT x.EVENT_DATE TRANSACTION_DATE,
    CASE
        WHEN x.CUSTOMER_BASE = 'DAILYBASE' THEN 'USER_DAILY_ACTIVE'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSWINBACK' THEN 'USER_30DAYS_WINBACK'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSBASE' THEN 'USER_30DAYS_GROUP'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSLOST' THEN 'USER_30DAYS_LOST'
        WHEN x.CUSTOMER_BASE = 'CHURN' THEN 'USER_CHURN'
    END DESTINATION_CODE,
     x.formule PROFILE_CODE, 'UNKNOWN' SUB_ACCOUNT, 'HIT' MEASUREMENT_UNIT,'FT_ACCOUNT_ACTIVITY' SOURCE_TABLE, NVL(b.OPERATOR_CODE,'OCM') OPERATOR_CODE ,sum(x.AMOUNT) TOTAL_AMOUNT, sum(x.AMOUNT) RATED_AMOUNT, sysdate INSERT_DATE, '' REGION_ID
FROM
(
    SELECT a.EVENT_DATE,
           a.formule,
           b.CUSTOMER_BASE,
           CASE
                WHEN b.CUSTOMER_BASE = 'DAILYBASE' THEN DAILYBASE
                WHEN b.CUSTOMER_BASE = 'ALL30DAYSWINBACK' THEN ALL30DAYSWINBACK
                WHEN b.CUSTOMER_BASE = 'ALL30DAYSBASE' THEN ALL30DAYSBASE
                WHEN b.CUSTOMER_BASE = 'ALL30DAYSLOST' THEN ALL30DAYSLOST
                WHEN b.CUSTOMER_BASE = 'CHURN' THEN CHURN
           END AMOUNT
    FROM MON.FT_GROUP_USER_BASE a
    CROSS JOIN
    (
        SELECT 'DAILYBASE' AS CUSTOMER_BASE FROM DUAL UNION ALL
        SELECT 'ALL30DAYSWINBACK' AS CUSTOMER_BASE FROM DUAL UNION ALL
        SELECT 'ALL30DAYSBASE' AS CUSTOMER_BASE FROM DUAL UNION ALL
        SELECT 'ALL30DAYSLOST' AS CUSTOMER_BASE FROM DUAL UNION ALL
        SELECT 'CHURN' AS CUSTOMER_BASE FROM DUAL
    ) b
) x, DIM.DT_OFFER_PROFILES b
where EVENT_DATE in ( SELECT datecode
                                FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                                                 WHERE datecode BETWEEN TRUNC(SYSDATE-cust_base_number_day_from) AND TRUNC(SYSDATE-cust_base_number_day_bef)
                                               MINUS
                                          SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                                WHERE SOURCE_TABLE='FT_ACCOUNT_ACTIVITY' AND TRANSACTION_DATE BETWEEN trunc(sysdate-cust_base_number_day_from) and trunc(sysdate-cust_base_number_day_bef))
                                                            )
and upper(x.formule) = b.PROFILE_CODE(+)
group by x.EVENT_DATE,
    CASE
        WHEN x.CUSTOMER_BASE = 'DAILYBASE' THEN 'USER_DAILY_ACTIVE'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSWINBACK' THEN 'USER_30DAYS_WINBACK'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSBASE' THEN 'USER_30DAYS_GROUP'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSLOST' THEN 'USER_30DAYS_LOST'
        WHEN x.CUSTOMER_BASE = 'CHURN' THEN 'USER_CHURN'
    END, x.formule, NVL(b.OPERATOR_CODE,'OCM');
------------------------------------------
COMMIT;

insert into FT_GLOBAL_ACTIVITY_DAILY_MKT
select datecode TRANSACTION_DATE,'USER_GROSS_ADD' DESTINATION_CODE, COMMERCIAL_OFFER PROFILE_CODE,'UNKNOWN' SUB_ACCOUNT,'HIT' MEASUREMENT_UNIT
,'FT_A_SUBSCRIBER_SUMMARY' SOURCE_TABLE,NVL(OPERATOR_CODE,'OCM')OPERATOR_CODE ,sum(TOTAL_ACTIVATION) TOTAL_AMOUNT,sum(TOTAL_ACTIVATION) RATED_AMOUNT
,sysdate INSERT_DATE,'' REGION_ID
from FT_A_SUBSCRIBER_SUMMARY a,DIM.DT_OFFER_PROFILES b
where datecode in ( SELECT datecode
                                FROM (SELECT DISTINCT datecode FROM DIM.DT_DATES
                                                 WHERE datecode BETWEEN TRUNC(SYSDATE-cust_base_number_day_from) AND TRUNC(SYSDATE-cust_base_number_day_bef)
                                               MINUS
                                          SELECT DISTINCT TRANSACTION_DATE datecode FROM FT_GLOBAL_ACTIVITY_DAILY_MKT
                                                WHERE SOURCE_TABLE='FT_A_SUBSCRIBER_SUMMARY' AND TRANSACTION_DATE BETWEEN trunc(sysdate-cust_base_number_day_from) and trunc(sysdate-cust_base_number_day_bef))
                                                            )
and upper(a.COMMERCIAL_OFFER) = b.PROFILE_CODE(+)
and NETWORK_DOMAIN = 'GSM'
group by datecode,COMMERCIAL_OFFER,NVL(OPERATOR_CODE,'OCM');


-----------------------
COMMIT;

 END; -- P_INS_CUSTOMER_BASE

---------------------------------------------------------------------------------------------------------------------
PROCEDURE P_INS_ORANGE_MONEY_REVENUE (p_number_day_from IN NUMBER DEFAULT 25,p_number_day_bef IN NUMBER DEFAULT 1)  IS


  number_day_from number:=p_number_day_from;
  number_day_bef number:=p_number_day_bef;
  d_start_date DATE;
  d_next_day DATE;
  d_end_date DATE;
  s_val NUMBER;
  i_val NUMBER;
  i_value NUMBER;
  s_slice_value VARCHAR2(15);
BEGIN
    d_start_date := TRUNC(SYSDATE - p_number_day_from);
    d_end_date := TRUNC(SYSDATE - p_number_day_bef);

    WHILE d_start_date <= d_end_date LOOP
        d_next_day := d_start_date + 1;
        s_slice_value := TO_CHAR(d_start_date, 'yyyymmdd');
        -- Insert code here
        SELECT COUNT(*) INTO s_val FROM MON.FT_GLOBAL_ACTIVITY_DAILY_MKT
        WHERE SOURCE_TABLE='FT_OM_TRANSACTION_USERS'
            AND DESTINATION_CODE LIKE 'REVENUE_OMNY_SVC_%' AND TRANSACTION_DATE = d_start_date AND ROWNUM <= 10;

        SELECT mon.FN_VALIDATE_DAY2DAY_EXIST ('TANGO_MON.FT_OM_TRANSACTION_USERS', 'EVENT_DATE'
                        , s_slice_value, s_slice_value, 10, '') INTO i_val FROM DUAL;

        SELECT mon.FN_VALIDATE_DAY2DAY_EXIST ('MON.FT_CONTRACT_SNAPSHOT', 'EVENT_DATE'
                        , TO_CHAR(d_next_day, 'yyyymmdd'), TO_CHAR(d_next_day, 'yyyymmdd'), 10, '') INTO i_value FROM DUAL;

        IF s_val = 0 AND i_val = 1 AND i_value = 1 THEN

            INSERT INTO MON.FT_GLOBAL_ACTIVITY_DAILY_MKT
            SELECT  EVENT_DATE TRANSACTION_DATE,
                    'REVENUE_OMNY_SVC_' || SERVICE_TYPE DESTINATION_CODE,
                    PROFILE PROFILE_CODE,
                    'MAIN' SUB_ACCOUNT,
                    'HIT' MEASUREMENT_UNIT,
                    'FT_OM_TRANSACTION_USERS' SOURCE_TABLE,
                    'OCM' AS OPERATOR_CODE,
                    SUM(SERVICE_CHARGE_AMOUNT) TOTAL_AMOUNT,
                    SUM(SERVICE_CHARGE_AMOUNT) RATED_AMOUNT,
                    SYSDATE INSERT_DATE,
                    NULL AS REGION_ID
            FROM
            (
                SELECT EVENT_DATE, SENDER_MSISDN, SERVICE_TYPE, SERVICE_CHARGE_AMOUNT
                FROM TANGO_MON.FT_OM_TRANSACTION_USERS
                WHERE EVENT_DATE = d_start_date AND SERVICE_CHARGE_AMOUNT > 0
            )
            LEFT JOIN
            (
                SELECT ACCESS_KEY, PROFILE
                FROM MON.FT_CONTRACT_SNAPSHOT
                WHERE EVENT_DATE = d_next_day
            )
             ON SENDER_MSISDN = ACCESS_KEY
            GROUP BY EVENT_DATE, PROFILE, SERVICE_TYPE;

             COMMIT;

        END IF;
        --
        d_start_date := d_start_date + 1;
    END LOOP;
END  ; --P_INS_ORANGE_MONEY_REVENUE

END PCK_LOAD_GLOBAL_ACTIVITY_MKT;REVENUE_2G_BUNDLE