INSERT INTO MON.SPARK_TF_BASE_GROUP_CONSO_MONTH
SELECT
b.msisdn msisdn
--
, upper(nvl(d.PRENOM,'')) firstname
, upper(nvl(d.NOM,'')) lastname
, nvl(d.NEE_LE, null) birthdate
, b.activation_date activation_date
, b.formule profile
, nvl(d.VILLE_VILLAGE,'') identif_townname
, nvl(d.DATE_IDENTIFICATION,NULL) registration_details_date
--
, nvl(b1.main_cost_total,0) main_cost_total
, nvl(b1.main_cost_voice_sms,0) main_cost_voice_sms
, nvl(b1.main_cost_data,0) main_cost_data
, nvl(b1.main_cost_subscr,0) main_cost_subscr
--
, nvl(a.montant_plenty,0) main_cost_subscr_plenty
, nvl(a.montant_orange_bonus,0) main_cost_subscr_orange_bonus
, nvl(a.montant_fnf_modify,0) main_cost_subscr_fnf_modify
, nvl(a.montant_forfait_data,0) main_cost_subscr_forfait_data
--
, nvl(b1.main_cost_total_roam,0) main_cost_total_roam
, nvl(b1.main_cost_voice_sms_roam,0) main_cost_voice_sms_roam
, nvl(b1.main_cost_data_roam,0) main_cost_data_roam
, nvl(b1.main_cost_subscr_roam,0) main_cost_subscr_roam
--
, nvl(b1.main_cost_total_roamout,0) main_cost_total_roamout
, nvl(b1.main_cost_voice_sms_roamout,0) main_cost_voice_sms_roamout
, nvl(b1.main_cost_data_roamout,0) main_cost_data_roamout
, nvl(b1.main_cost_subscr_roamout,0) main_cost_subscr_roamout
--
, nvl(b1.main_cost_total_roamin,0) main_cost_total_roamin
, nvl(b1.main_cost_voice_sms_roamin,0) main_cost_voice_sms_roamin
, nvl(b1.main_cost_data_roamin,0) main_cost_data_roamin
, nvl(b1.main_cost_subscr_roamin,0) main_cost_subscr_roamin
--
, nvl(c.site_name,'') site_name
, nvl(c.townname,'') townname
, nvl(c.administrative_region,'') admin_region
, nvl(c.commercial_region, '') commerc_region
--
, nvl(e.tac_code_handset,'') tac_code_handset
, nvl(e.constructor, '') constructor_handset
, nvl(e.model,'') model_handset
, nvl(e.data_compatible,'') data_compatible_handset
, b.event_date account_activity_event_date
, nvl(a.nombre_plenty,0) total_subscr_plenty
, nvl(a.nombre_orange_bonus,0) total_subscr_orange_bonus
, nvl(a.nombre_fnf_modify,0) total_subscr_fnf_modify
, nvl(a.nombre_forfait_data,0) total_subscr_forfait_data
--
, nvl(a.montant_maxi_bonus,0) main_cost_subscr_maxi_bonus
, nvl(a.nombre_maxi_bonus,0) total_subscr_maxi_bonus
--
, nvl(a.montant_pro,0) main_cost_subscr_pro
, nvl(a.nombre_pro,0) total_subscr_pro
--
, nvl(a.montant_international,0) main_cost_subscr_international
, nvl(a.nombre_international,0) total_subscr_international
--
, nvl(a.montant_pack_sms,0) main_cost_subscr_pack_sms
, nvl(a.nombre_pack_sms,0) total_subscr_pack_sms
--
, nvl(a.montant_ws,0) main_cost_subscr_ws
, nvl(a.nombre_ws,0) total_subscr_ws
--
, nvl(a.montant_orange_bonus_illimite,0) main_cost_subscr_ob_illimite
, nvl(a.nombre_orange_bonus_illimite,0) total_subscr_ob_illimite
--
, nvl(a.montant_orange_phenix,0) main_cost_subscr_orange_phenix
, nvl(a.nombre_orange_phenix,0) total_subscr_orange_phenix
--
, nvl(a.montant_recharge_plenty,0) main_cost_subscr_refill_plenty
, nvl(a.nombre_recharge_plenty,0) total_subscr_refill_plenty
--
, nvl(a.nombre_total,0) total_subscr
--
, nvl(e.imei,'') imei_handset
, nvl(e.data2g_compatible,'') data2g_compatible_handset
, nvl(e.data3g_compatible,'') data3g_compatible_handset
, nvl(e.data4g_compatible,'') data4g_compatible_handset
, CURRENT_TIMESTAMP insert_date
,'###SLICE_VALUE###' base_month
FROM
(SELECT EVENT_MONTH
, MSISDN
, sum(MONTANT_TOTAL) MONTANT_TOTAL
, sum(MONTANT_PLENTY) MONTANT_PLENTY
, sum(MONTANT_ORANGE_BONUS) MONTANT_ORANGE_BONUS
, sum(MONTANT_FNF_MODIFY) MONTANT_FNF_MODIFY
, sum(MONTANT_FORFAIT_DATA) MONTANT_FORFAIT_DATA
, sum(MONTANT_MAXI_BONUS) MONTANT_MAXI_BONUS
, sum(MONTANT_PRO) MONTANT_PRO
, sum(MONTANT_INTERNATIONAL) MONTANT_INTERNATIONAL
, sum(MONTANT_PACK_SMS) MONTANT_PACK_SMS
, sum(MONTANT_WS) MONTANT_WS
, sum(MONTANT_ORANGE_BONUS_ILLIMITE) MONTANT_ORANGE_BONUS_ILLIMITE
, sum(MONTANT_ORANGE_PHENIX) MONTANT_ORANGE_PHENIX
, sum(MONTANT_RECHARGE_PLENTY) MONTANT_RECHARGE_PLENTY
--
, sum(NOMBRE_TOTAL) NOMBRE_TOTAL
, sum(NOMBRE_PLENTY) NOMBRE_PLENTY
, sum(NOMBRE_ORANGE_BONUS) NOMBRE_ORANGE_BONUS
, sum(NOMBRE_FNF_MODIFY) NOMBRE_FNF_MODIFY
, sum(NOMBRE_FORFAIT_DATA) NOMBRE_FORFAIT_DATA
, sum(NOMBRE_MAXI_BONUS) NOMBRE_MAXI_BONUS
, sum(NOMBRE_PRO) NOMBRE_PRO
, sum(NOMBRE_INTERNATIONAL) NOMBRE_INTERNATIONAL
, sum(NOMBRE_PACK_SMS) NOMBRE_PACK_SMS
, sum(NOMBRE_WS) NOMBRE_WS
, sum(NOMBRE_ORANGE_BONUS_ILLIMITE) NOMBRE_ORANGE_BONUS_ILLIMITE
, sum(NOMBRE_ORANGE_PHENIX) NOMBRE_ORANGE_PHENIX
, sum(NOMBRE_RECHARGE_PLENTY) NOMBRE_RECHARGE_PLENTY
from MON.SPARK_TF_SPECIFIC_SUBSCRIPTION_MONTH
WHERE EVENT_MONTH = '###SLICE_VALUE###'
GROUP BY EVENT_MONTH
, MSISDN
) a
RIGHT JOIN (SELECT a.*

FROM MON.SPARK_FT_ACCOUNT_ACTIVITY a
WHERE EVENT_DATE = DATE_SUB(LAST_DAY('###SLICE_VALUE###'),-1)
--AND ROWNUM < 1
AND GP_STATUS = 'ACTIF'
) b ON(b.msisdn = a.msisdn)
RIGHT JOIN (SELECT * FROM MON.SPARK_TF_GLOBAL_CONSO_MSISDN_MONTH WHERE EVENT_MONTH = '###SLICE_VALUE###' ) b1 ON(b.msisdn = b1.msisdn)
RIGHT JOIN (
select MSISDN
, SITE_NAME
, TOWNNAME
, ADMINISTRATIVE_REGION
, COMMERCIAL_REGION
from mon.SPARK_FT_CLIENT_LAST_SITE_LOCATION b
where b.EVENT_MONTH = '###SLICE_VALUE###'

) c ON(b.msisdn = c.msisdn)
RIGHT JOIN (SELECT * FROM DIM.SPARK_DT_BASE_IDENTIFICATION) d ON(b.msisdn = d.msisdn)
RIGHT JOIN (
SELECT substr(e.imei,1,8) tac_code_handset, e.*, f.*
FROM
(SELECT a.*
, ROW_NUMBER() OVER(PARTITION BY MSISDN ORDER BY TOTAL_DAYS_COUNT DESC) AS IMEI_RN
FROM MON.SPARK_FT_IMEI_TRAFFIC_MONTHLY a
WHERE smonth = REPLACE('###SLICE_VALUE###','-','')
) e
RIGHT JOIN (select
tac_code ,constructor ,model ,x_phase ,capacity,wap,gprs,market_entry,ussd_level,mms,umts ,color_screen,port,camera,edge,java,gallery,video,wap_push,talk_now,sms_cliquable,mms_push_class,gps,
hsdpa,unik_uma,insert_refresh_date,amr,lte,bluetooth,hsupa,html,multitouch,open_os,videotelephony,wifi,technologie,os,ref_month,terminal_type,tek_radio,ind,source,
(CASE WHEN UMTS = 'O' or GPRS = 'O' or EDGE = 'O' or EDGE = 'E' or LTE = 'O' THEN 'YES' ELSE 'NO' END) DATA_COMPATIBLE,
(CASE WHEN GPRS = 'O' or EDGE = 'O' or EDGE = 'E' THEN 'YES' ELSE 'NO' END) DATA2G_COMPATIBLE,
(CASE WHEN UMTS = 'O' or LTE = 'O' THEN 'YES' ELSE 'NO' END) DATA3G_COMPATIBLE,
(CASE WHEN LTE = 'O' THEN 'YES' ELSE 'NO' END) DATA4G_COMPATIBLE
from
dim.dt_handset_ref
) f
ON(substr(e.imei,1,8) = f.tac_code)
where e.IMEI_RN = 1
) e ON(b.msisdn = e.msisdn)