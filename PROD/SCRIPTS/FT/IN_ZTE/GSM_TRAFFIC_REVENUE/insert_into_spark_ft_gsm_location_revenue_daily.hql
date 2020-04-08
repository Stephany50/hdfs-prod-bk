insert into MON.SPARK_FT_GSM_LOCATION_REVENUE_DAILY 
select
    COMMERCIAL_PROFILE OFFER_PROFILE_CODE
    ,(
        CASE
        WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
        WHEN SERVICE_CODE = 'TEL' THEN 'VOI_VOX'
        WHEN SERVICE_CODE = 'USS' THEN 'NVX_USS'
        WHEN SERVICE_CODE = 'GPR' THEN 'NVX_DAT_GPR'
        WHEN SERVICE_CODE = 'DFX' THEN 'NVX_DFX'
        WHEN SERVICE_CODE = 'DAT' THEN 'NVX_DAT'
        WHEN SERVICE_CODE = 'VDT' THEN 'NVX_VDT'
        WHEN SERVICE_CODE = 'WEB' THEN 'NVX_WEB'
        WHEN UPPER(SERVICE_CODE) IN ('SMSMO','SMSRMG') THEN 'NVX_SMS'
        WHEN UPPER(SERVICE_CODE) IN ('OC','OCFWD','OCRMG','TCRMG') THEN 'VOI_VOX'
        WHEN UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' THEN 'VOI_VOX'
        WHEN UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN 'VOI_VOX'
        ELSE 'AUT'
        END
    ) SERVICE_CODE
    ,LOCATION_MCC NSL_MCC
    ,LOCATION_MNC NSL_MNC
    ,LOCATION_CI2 NSL_CI
    ,LOCATION_LAC2 NSL_LAC
    ,NETWORK_EVENT_TYPE SUB_SERVICE
    ,(
        CASE WHEN a.NETWORK_ELEMENT_ID IS NOT NULL THEN NETWORK_ELEMENT_ID
        WHEN UPPER(NETWORK_EVENT_TYPE) IN ('SMSMO', 'OC','OCFWD','OCRMG','TCRMG','SMSRMG') THEN 'IN_TRAFFIC'
        WHEN UPPER(NETWORK_EVENT_TYPE) IN ('TOPUP','REFILL VIA MENU') THEN 'IN_REFILL'
        WHEN UPPER(NETWORK_EVENT_TYPE) IN ('DIRECT-DEBIT','DIRECT-CREDIT','DEPOSIT') THEN 'IN_DEPOSIT'
        WHEN SERVICE_CODE = 'TEL' THEN 'IN_TRAFFIC'
        WHEN SERVICE_CODE = 'SMS' THEN 'IN_TRAFFIC'
        ELSE 'IN_SELFCARE'
        END 
    ) SERVED_SERVICE
    ,(
        CASE WHEN a.Call_Destination_Code IN ('ONNET','ONNETFREE','OCM_D') THEN 'OUT_NAT_MOB_OCM'
        WHEN a.Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
        WHEN a.Call_Destination_Code IN ('NEXTTEL') THEN 'OUT_NAT_MOB_NEX'
        WHEN a.Call_Destination_Code IN ('CAM_D','CAM') THEN 'OUT_NAT_MOB_CAM'
        WHEN a.Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
        WHEN a.Call_Destination_Code = 'VAS' THEN 'OUT_SVA'
        WHEN a.Call_Destination_Code = 'EMERG' THEN 'OUT_CCSVA'
        WHEN a.Call_Destination_Code = 'OCRMG' THEN 'OUT_ROAM_MO'
        WHEN a.Call_Destination_Code = 'INT' THEN 'OUT_INT'
        WHEN a.Call_Destination_Code = 'MVNO' THEN 'OUT_NAT_MOB_MVO'
        ELSE Call_Destination_Code END 
    ) DESTINATION
    ,OTHER_PARTY_ZONE
    ,SPECIFIC_TARIFF_INDICATOR 
    , SUM(1) Total_Count
    , SUM(CASE WHEN Main_Rated_Amount + Promo_Rated_Amount > 0 THEN 1 ELSE 0 END) Rated_Total_Count
    , SUM(CALL_PROCESS_TOTAL_DURATION) Duration
    , SUM(CASE WHEN a.Main_Rated_Amount + a.Promo_Rated_Amount > 0 THEN CALL_PROCESS_TOTAL_DURATION ELSE 0 END) Rated_Duration
    , SUM(a.Main_Rated_Amount) Main_Rated_Amount
    , SUM(a.Promo_Rated_Amount) Promo_Rated_Amount
    , SUM(a.Main_Rated_Amount + a.Promo_Rated_Amount) Rated_Amount
    , SUM(nvl(MAIN_REFILL_AMOUNT,0)) DEBIT_AMOUNT
    , 'ZTE' Source_Platform
    , 'FT_BILLED_TRANSACTION_PREPAID' Source_Data
    , CURRENT_TIMESTAMP() INSERT_DATE
    , fn_get_operator_code(SERVED_PARTY) OPERATOR_CODE   
    , COUNT(Distinct SERVED_PARTY) MSISDN_COUNT
    , TRANSACTION_DATE
FROM (
    SELECT  
        a.*
        , (CASE WHEN (a.LOCATION_MCC<>'624') or (a.LOCATION_MNC<>'02')  THEN '' ELSE a.LOCATION_CI   END ) LOCATION_CI2
        , (CASE WHEN (a.LOCATION_MCC<>'624') or (a.LOCATION_MNC<>'02')  THEN '' ELSE a.LOCATION_LAC   END ) LOCATION_LAC2
    FROM MON.SPARK_FT_BILLED_TRANSACTION_PREPAID a
    WHERE
        TRANSACTION_DATE = "###SLICE_VALUE###"
        AND a.Main_Rated_Amount >= 0
        AND a.Promo_Rated_Amount >= 0
) a
GROUP BY
COMMERCIAL_PROFILE
,(CASE
    WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
    WHEN SERVICE_CODE = 'TEL' THEN 'VOI_VOX'
    WHEN SERVICE_CODE = 'USS' THEN 'NVX_USS'
    WHEN SERVICE_CODE = 'GPR' THEN 'NVX_DAT_GPR'
    WHEN SERVICE_CODE = 'DFX' THEN 'NVX_DFX'
    WHEN SERVICE_CODE = 'DAT' THEN 'NVX_DAT'
    WHEN SERVICE_CODE = 'VDT' THEN 'NVX_VDT'
    WHEN SERVICE_CODE = 'WEB' THEN 'NVX_WEB'
    WHEN UPPER(SERVICE_CODE) IN ('SMSMO','SMSRMG') THEN 'NVX_SMS'
    WHEN UPPER(SERVICE_CODE) IN ('OC','OCFWD','OCRMG','TCRMG') THEN 'VOI_VOX'
    WHEN UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' THEN 'VOI_VOX'
    WHEN UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN 'VOI_VOX'
    ELSE 'AUT'
    END ) 
,LOCATION_MCC , LOCATION_MNC ,LOCATION_LAC2 , LOCATION_CI2 , NETWORK_EVENT_TYPE 
,(CASE WHEN a.NETWORK_ELEMENT_ID IS NOT NULL THEN NETWORK_ELEMENT_ID
                        WHEN UPPER(NETWORK_EVENT_TYPE) IN ('SMSMO', 'OC','OCFWD','OCRMG','TCRMG','SMSRMG') THEN 'IN_TRAFFIC'
                        WHEN UPPER(NETWORK_EVENT_TYPE) IN ('TOPUP','REFILL VIA MENU') THEN 'IN_REFILL'
                        WHEN UPPER(NETWORK_EVENT_TYPE) IN ('DIRECT-DEBIT','DIRECT-CREDIT','DEPOSIT') THEN 'IN_DEPOSIT'
                        WHEN SERVICE_CODE = 'TEL' THEN 'IN_TRAFFIC'
                        WHEN SERVICE_CODE = 'SMS' THEN 'IN_TRAFFIC'
                        ELSE 'IN_SELFCARE'
                        END ) 
,(CASE WHEN a.Call_Destination_Code IN ('ONNET','ONNETFREE','OCM_D') THEN 'OUT_NAT_MOB_OCM'
    WHEN a.Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
    WHEN a.Call_Destination_Code IN ('NEXTTEL') THEN 'OUT_NAT_MOB_NEX'
    WHEN a.Call_Destination_Code IN ('CAM_D','CAM') THEN 'OUT_NAT_MOB_CAM'
    WHEN a.Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
    WHEN a.Call_Destination_Code = 'VAS' THEN 'OUT_SVA'
    WHEN a.Call_Destination_Code = 'EMERG' THEN 'OUT_CCSVA'
    WHEN a.Call_Destination_Code = 'OCRMG' THEN 'OUT_ROAM_MO'
    WHEN a.Call_Destination_Code = 'INT' THEN 'OUT_INT'
    WHEN a.Call_Destination_Code = 'MVNO' THEN 'OUT_NAT_MOB_MVO'
    ELSE Call_Destination_Code END ) 
,OTHER_PARTY_ZONE,SPECIFIC_TARIFF_INDICATOR 
,fn_get_operator_code(SERVED_PARTY)
,transaction_date