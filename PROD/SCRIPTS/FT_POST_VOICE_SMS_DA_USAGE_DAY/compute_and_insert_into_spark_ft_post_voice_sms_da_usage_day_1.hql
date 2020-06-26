INSERT  INTO TMP.TT_POST_VOICE_SMS_DA_USAGE_DAY
SELECT 'rating1' rating_context
, transaction_date
, msisdn
, identif1 da_name
, unit1 da_unit
, null da_type
, TO_NUMBER(charge1) charge
, operator_code
, commercial_profile
, Tariff_Plan
, Other_Party_Zone
, Destination
, Usage
, 'FT_BILLED_TRANSACTION_POSTPAID' SOURCE_TABLE
, SYSDATE insert_date
FROM
(
select trunc(transaction_date) transaction_date
, CHARGED_PARTY msisdn
, (CASE WHEN OPERATOR_CODE IS NULL THEN DECODE (CHARGED_PARTY,NULL,'OCM',mon.fn_get_operator_code(CHARGED_PARTY))
ELSE OPERATOR_CODE END
) operator_code
, Identifier_list
, substr(nvl(Identifier_list,''), 1, instr(nvl(Identifier_list,''),'|',1,1)-1) Identif1
, substr(nvl(Identifier_list,''), instr(nvl(Identifier_list,''),'|',1,1)+1, instr(nvl(Identifier_list,''),'|',1,2)-instr(nvl(Identifier_list,''),'|',1,1)-1) Identif2
, substr(nvl(Identifier_list,''), instr(nvl(Identifier_list,''),'|',1,2)+1, instr(nvl(Identifier_list,''),'|',1,3)-instr(nvl(Identifier_list,''),'|',1,2)-1) Identif3
, substr(nvl(Identifier_list,''), instr(nvl(Identifier_list,''),'|',1,3)+1) Identif4
, Volume_List
, substr(nvl(Volume_List,''), 1, instr(nvl(Volume_List,''),'|',1,1)-1) charge1
, substr(nvl(Volume_List,''), instr(nvl(Volume_List,''),'|',1,1)+1, instr(nvl(Volume_List,''),'|',1,2)-instr(nvl(Volume_List,''),'|',1,1)-1) charge2
, substr(nvl(Volume_List,''), instr(nvl(Volume_List,''),'|',1,2)+1, instr(nvl(Volume_List,''),'|',1,3)-instr(nvl(Volume_List,''),'|',1,2)-1) charge3
, substr(nvl(Volume_List,''), instr(nvl(Volume_List,''),'|',1,3)+1) charge4
, Unit_Of_Measurement_List
, substr(nvl(Unit_Of_Measurement_List,''), 1, instr(nvl(Unit_Of_Measurement_List,''),'|',1,1)-1) unit1
, substr(nvl(Unit_Of_Measurement_List,''), instr(nvl(Unit_Of_Measurement_List,''),'|',1,1)+1, instr(nvl(Unit_Of_Measurement_List,''),'|',1,2)-instr(nvl(Unit_Of_Measurement_List,''),'|',1,1)-1) unit2
, substr(nvl(Unit_Of_Measurement_List,''), instr(nvl(Unit_Of_Measurement_List,''),'|',1,2)+1, instr(nvl(Unit_Of_Measurement_List,''),'|',1,3)-instr(nvl(Unit_Of_Measurement_List,''),'|',1,2)-1) unit3
, substr(nvl(Unit_Of_Measurement_List,''), instr(nvl(Unit_Of_Measurement_List,''),'|',1,3)+1) unit4
, commercial_profile
, RAW_TARIFF_PLAN Tariff_Plan
, Other_Party_Zone
, (CASE WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
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
END ) Usage
, (CASE WHEN a.Call_Destination_Code IN ('ONNET','ONNETFREE','OCM_D') THEN 'OUT_NAT_MOB_OCM'
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
/*, (CASE WHEN Served_Party LIKE '92%' THEN 'Y' ELSE 'N' end) callingnbr_commence_par_92
, (CASE WHEN Charged_Party LIKE '92%' THEN 'Y' ELSE 'N' end) billingnbr_commence_par_92
, (CASE WHEN Other_Party LIKE '92%' THEN 'Y' ELSE 'N' end) callednbr_commence_par_92
, (CASE WHEN Main_Rated_Amount < 0 or Promo_Rated_Amount < 0 THEN 'Y' ELSE 'N' END) mauvais_ticket
, Promo_Rated_Amount
, Main_Rated_Amount
, CALL_PROCESS_TOTAL_DURATION
, Rated_Duration
--, a.*/
from MON.SPARK_FT_BILLED_TRANSACTION_POSTPAID a
where TRANSACTION_DATE BETWEEN TO_DATE (s_slice_value||' 000000','yyyymmdd hh24miss')  AND TO_DATE (s_slice_value||' 235959','yyyymmdd hh24miss')
--AND ROWNUM < 1
--and identifier_list like '%M%|%P%'
--AND volume_list LIKE '%,%'
)
WHERE length(Identif1) <> 0
--AND mauvais_ticket = 'N'