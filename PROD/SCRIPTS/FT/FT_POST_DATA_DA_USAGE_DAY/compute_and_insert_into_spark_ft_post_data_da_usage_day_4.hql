INSERT INTO TMP.TT_POST_DATA_DA_USAGE_DAY

SELECT
    'rating4' rating_context,
    msisdn,
    identif4 da_name,
    unit4 da_unit,
    null da_type,
    cast(charge4 as DECIMAL(17,2) ) charge,
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
        TO_DATE(SESSION_DATE) SESSION_DATE,
        SERVED_PARTY_MSISDN msisdn,
        (CASE WHEN OPERATOR_CODE IS NULL THEN ( CASE WHEN SERVED_PARTY_MSISDN IS NULL THEN 'OCM'ELSE fn_get_operator_code(SERVED_PARTY_MSISDN) END)
        ELSE OPERATOR_CODE END
        ) operator_code,
        USED_BALANCE_LIST,
        substr(nvl(USED_BALANCE_LIST,''), 1, locate('|', nvl(USED_BALANCE_LIST,''), 1) -1) Identif1,
        substr(nvl(USED_BALANCE_LIST,''), locate('|', nvl(USED_BALANCE_LIST,''), 1) +1, locate('|', nvl(USED_BALANCE_LIST,''), locate('|', nvl(USED_BALANCE_LIST,''), 1)+1) - locate('|', nvl(USED_BALANCE_LIST,''), 1)-1) Identif2,
        substr(nvl(USED_BALANCE_LIST,''), locate('|', nvl(USED_BALANCE_LIST,''), locate('|', nvl(USED_BALANCE_LIST,''), 1)+1)+1, locate('|', nvl(USED_BALANCE_LIST,''), locate('|', nvl(USED_BALANCE_LIST,''), locate('|', nvl(USED_BALANCE_LIST,''), 1)+1))-locate('|', nvl(USED_BALANCE_LIST,''), locate('|', nvl(USED_BALANCE_LIST,''), 1)+1)-1) Identif3,
        substr(nvl(USED_BALANCE_LIST,''), locate('|', nvl(USED_BALANCE_LIST,''), locate('|', nvl(USED_BALANCE_LIST,''), locate('|', nvl(USED_BALANCE_LIST,''), 1)+1))+1) Identif4,
        USED_VOLUME_LIST,
        substr(nvl(USED_VOLUME_LIST,''), 1, locate('|', nvl(USED_VOLUME_LIST,''), 1) -1) charge1,
        substr(nvl(USED_VOLUME_LIST,''), locate('|', nvl(USED_VOLUME_LIST,''), 1) +1, locate('|', nvl(USED_VOLUME_LIST,''), locate('|', nvl(USED_VOLUME_LIST,''), 1)+1) - locate('|', nvl(USED_VOLUME_LIST,''), 1)-1) charge2,
        substr(nvl(USED_VOLUME_LIST,''), locate('|', nvl(USED_VOLUME_LIST,''), locate('|', nvl(USED_VOLUME_LIST,''), 1)+1)+1, locate('|', nvl(USED_VOLUME_LIST,''), locate('|', nvl(USED_VOLUME_LIST,''), locate('|', nvl(USED_VOLUME_LIST,''), 1)+1))-locate('|', nvl(USED_VOLUME_LIST,''), locate('|', nvl(USED_VOLUME_LIST,''), 1)+1)-1) charge3,
        substr(nvl(USED_VOLUME_LIST,''), locate('|', nvl(USED_VOLUME_LIST,''), locate('|', nvl(USED_VOLUME_LIST,''), locate('|', nvl(USED_VOLUME_LIST,''), 1)+1))+1) charge4,
        USED_UNIT_LIST,
        substr(nvl(USED_UNIT_LIST,''), 1, locate('|', nvl(USED_UNIT_LIST,''), 1) -1) unit1,
        substr(nvl(USED_UNIT_LIST,''), locate('|', nvl(USED_UNIT_LIST,''), 1) +1, locate('|', nvl(USED_UNIT_LIST,''), locate('|', nvl(USED_UNIT_LIST,''), 1)+1) - locate('|', nvl(USED_UNIT_LIST,''), 1)-1) unit2,
        substr(nvl(USED_UNIT_LIST,''), locate('|', nvl(USED_UNIT_LIST,''), locate('|', nvl(USED_UNIT_LIST,''), 1)+1)+1, locate('|', nvl(USED_UNIT_LIST,''), locate('|', nvl(USED_UNIT_LIST,''), locate('|', nvl(USED_UNIT_LIST,''), 1)+1))-locate('|', nvl(USED_UNIT_LIST,''), locate('|', nvl(USED_UNIT_LIST,''), 1)+1)-1) unit3,
        substr(nvl(USED_UNIT_LIST,''), locate('|', nvl(USED_UNIT_LIST,''), locate('|', nvl(USED_UNIT_LIST,''), locate('|', nvl(USED_UNIT_LIST,''), 1)+1))+1) unit4,
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
WHERE length(Identif4) <> 0