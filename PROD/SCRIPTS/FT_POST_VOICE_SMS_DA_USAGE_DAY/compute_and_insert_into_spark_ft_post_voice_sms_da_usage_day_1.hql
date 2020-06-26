INSERT  INTO TMP.TT_POST_VOICE_SMS_DA_USAGE_DAY

SELECT
    'rating1' rating_context,
    msisdn,
    identif1 da_name,
    unit1 da_unit,
    null da_type,
    cast(charge1 as DECIMAL(17,2) ) charge,
    operator_code,
    commercial_profile,
    Tariff_Plan,
    Other_Party_Zone,
    Destination,
    Usage,
    'FT_BILLED_TRANSACTION_POSTPAID' SOURCE_TABLE,
    CURRENT_TIMESTAMP insert_date,
    transaction_date

FROM
    (
    select
        trunc(transaction_date) transaction_date,
        CHARGED_PARTY msisdn,
        (CASE WHEN OPERATOR_CODE IS NULL THEN DECODE (CHARGED_PARTY,NULL,'OCM',mon.fn_get_operator_code(CHARGED_PARTY))
        ELSE OPERATOR_CODE END
        ) operator_code,
        Identifier_list,
        substr(nvl(Identifier_list,''), 1, instr(nvl(Identifier_list,''),'|',1,1)-1) Identif1,
        substr(nvl(Identifier_list,''), instr(nvl(Identifier_list,''),'|',1,1)+1, instr(nvl(Identifier_list,''),'|',1,2)-instr(nvl(Identifier_list,''),'|',1,1)-1) Identif2,
        substr(nvl(Identifier_list,''), instr(nvl(Identifier_list,''),'|',1,2)+1, instr(nvl(Identifier_list,''),'|',1,3)-instr(nvl(Identifier_list,''),'|',1,2)-1) Identif3,
        substr(nvl(Identifier_list,''), instr(nvl(Identifier_list,''),'|',1,3)+1) Identif4,
        Volume_List,
        substr(nvl(Volume_List,''), 1, instr(nvl(Volume_List,''),'|',1,1)-1) charge1,
        substr(nvl(Volume_List,''), instr(nvl(Volume_List,''),'|',1,1)+1, instr(nvl(Volume_List,''),'|',1,2)-instr(nvl(Volume_List,''),'|',1,1)-1) charge2,
        substr(nvl(Volume_List,''), instr(nvl(Volume_List,''),'|',1,2)+1, instr(nvl(Volume_List,''),'|',1,3)-instr(nvl(Volume_List,''),'|',1,2)-1) charge3,
        substr(nvl(Volume_List,''), instr(nvl(Volume_List,''),'|',1,3)+1) charge4,
        Unit_Of_Measurement_List,
        substr(nvl(Unit_Of_Measurement_List,''), 1, instr(nvl(Unit_Of_Measurement_List,''),'|',1,1)-1) unit1,
        substr(nvl(Unit_Of_Measurement_List,''), instr(nvl(Unit_Of_Measurement_List,''),'|',1,1)+1, instr(nvl(Unit_Of_Measurement_List,''),'|',1,2)-instr(nvl(Unit_Of_Measurement_List,''),'|',1,1)-1) unit2,
        substr(nvl(Unit_Of_Measurement_List,''), instr(nvl(Unit_Of_Measurement_List,''),'|',1,2)+1, instr(nvl(Unit_Of_Measurement_List,''),'|',1,3)-instr(nvl(Unit_Of_Measurement_List,''),'|',1,2)-1) unit3,
        substr(nvl(Unit_Of_Measurement_List,''), instr(nvl(Unit_Of_Measurement_List,''),'|',1,3)+1) unit4,
        commercial_profile,
        RAW_TARIFF_PLAN Tariff_Plan,
        Other_Party_Zone,
        (CASE WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
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
        END ) Usage,
        (CASE WHEN a.Call_Destination_Code IN ('ONNET','ONNETFREE','OCM_D') THEN 'OUT_NAT_MOB_OCM'
        WHEN a.Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
        WHEN a.Call_Destination_Code IN ('NEXTTEL') THEN 'OUT_NAT_MOB_NEX'
        WHEN a.Call_Destination_Code IN ('CAM_D','CAM') THEN 'OUT_NAT_MOB_CAM'
        WHEN a.Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
        WHEN a.Call_Destination_Code = 'VAS' THEN 'OUT_SVA'
        WHEN a.Call_Destination_Code = 'EMERG' THEN 'OUT_CCSVA'
        WHEN a.Call_Destination_Code = 'OCRMG' THEN 'OUT_ROAM_MO'
        WHEN a.Call_Destination_Code = 'INT' THEN 'OUT_INT'
        WHEN a.Call_Destination_Code = 'MVNO' THEN 'OUT_NAT_MOB_MVO'
        WHEN a.Call_Destination_Code = 'TCRMG' THEN 'OUT_ROAM_MT'
        ELSE Call_Destination_Code END ) Destination

    from MON.SPARK_FT_BILLED_TRANSACTION_POSTPAID a
    where TRANSACTION_DATE = '###SLICE_VALUE###'

    )
WHERE length(Identif1) <> 0
