INSERT  INTO TMP.TT_POST_DATA_DA_USAGE_DAY

SELECT
    'rating2' rating_context,
    msisdn,
    identif2 da_name,
    unit2 da_unit,
    NULL da_type,
    cast(charge1 as DECIMAL(17,2) ) charge,
    operator_code,
    contract_type,
    commercial_profile,
    Tariff_Plan,
    Type_of_measurement,
    service_type,
    service_zone,
    sdp_gos_service,
    'FT_CRA_GPRS_POST' source_table,
    CURRENT_TIMESTAMP insert_date,
    session_date

FROM
    (
    SELECT
        trunc(SESSION_DATE) SESSION_DATE,
        SERVED_PARTY_MSISDN msisdn,
        (CASE WHEN OPERATOR_CODE IS NULL THEN DECODE (SERVED_PARTY_MSISDN,NULL,'OCM',mon.fn_get_operator_code(SERVED_PARTY_MSISDN))
        ELSE OPERATOR_CODE END
        ) operator_code,
        USED_BALANCE_LIST,
        substr(nvl(USED_BALANCE_LIST,''), 1, instr(nvl(USED_BALANCE_LIST,''),'|',1,1)-1) Identif1,
        substr(nvl(USED_BALANCE_LIST,''), instr(nvl(USED_BALANCE_LIST,''),'|',1,1)+1, instr(nvl(USED_BALANCE_LIST,''),'|',1,2)-instr(nvl(USED_BALANCE_LIST,''),'|',1,1)-1) Identif2,
        substr(nvl(USED_BALANCE_LIST,''), instr(nvl(USED_BALANCE_LIST,''),'|',1,2)+1, instr(nvl(USED_BALANCE_LIST,''),'|',1,3)-instr(nvl(USED_BALANCE_LIST,''),'|',1,2)-1) Identif3,
        substr(nvl(USED_BALANCE_LIST,''), instr(nvl(USED_BALANCE_LIST,''),'|',1,3)+1) Identif4,
        USED_VOLUME_LIST,
        substr(nvl(USED_VOLUME_LIST,''), 1, instr(nvl(USED_VOLUME_LIST,''),'|',1,1)-1) charge1,
        substr(nvl(USED_VOLUME_LIST,''), instr(nvl(USED_VOLUME_LIST,''),'|',1,1)+1, instr(nvl(USED_VOLUME_LIST,''),'|',1,2)-instr(nvl(USED_VOLUME_LIST,''),'|',1,1)-1) charge2,
        substr(nvl(USED_VOLUME_LIST,''), instr(nvl(USED_VOLUME_LIST,''),'|',1,2)+1, instr(nvl(USED_VOLUME_LIST,''),'|',1,3)-instr(nvl(USED_VOLUME_LIST,''),'|',1,2)-1) charge3,
        substr(nvl(USED_VOLUME_LIST,''), instr(nvl(USED_VOLUME_LIST,''),'|',1,3)+1) charge4,
        USED_UNIT_LIST,
        substr(nvl(USED_UNIT_LIST,''), 1, instr(nvl(USED_UNIT_LIST,''),'|',1,1)-1) unit1,
        substr(nvl(USED_UNIT_LIST,''), instr(nvl(USED_UNIT_LIST,''),'|',1,1)+1, instr(nvl(USED_UNIT_LIST,''),'|',1,2)-instr(nvl(USED_UNIT_LIST,''),'|',1,1)-1) unit2,
        substr(nvl(USED_UNIT_LIST,''), instr(nvl(USED_UNIT_LIST,''),'|',1,2)+1, instr(nvl(USED_UNIT_LIST,''),'|',1,3)-instr(nvl(USED_UNIT_LIST,''),'|',1,2)-1) unit3,
        substr(nvl(USED_UNIT_LIST,''), instr(nvl(USED_UNIT_LIST,''),'|',1,3)+1) unit4,
        SERVED_PARTY_TYPE  contract_type,
        SERVED_PARTY_OFFER commercial_profile,
        SERVED_PARTY_PRICE_PLAN Tariff_Plan,
        UNIT_OF_MEASUREMENT type_of_measurement,
        SERVICE_TYPE,
        SERVICE_CODE SERVICE_ZONE,
        SDP_GOS_SERV_NAME SDP_GOS_SERVICE

    FROM MON.SPARK_FT_CRA_GPRS_POST a
    WHERE SESSION_DATE = '###SLICE_VALUE###'
    )
WHERE length(Identif2) <> 0
