    insert into MON.SPARK_TT_ANALYSE_DECON_SUBSC_SPEC1
     SELECT a.*
     --
     , nvl(c.MONTANT_PLENTY,0) MONTANT_PLENTY
     , nvl(c.MONTANT_ORANGE_BONUS,0) MONTANT_ORANGE_BONUS
     , nvl(c.MONTANT_FNF_MODIFY,0) MONTANT_FNF_MODIFY
     , nvl(c.MONTANT_FORFAIT_DATA,0) MONTANT_FORFAIT_DATA
     --
     , nvl(c.NOMBRE_PLENTY,0) NOMBRE_PLENTY
     , nvl(c.NOMBRE_ORANGE_BONUS,0) NOMBRE_ORANGE_BONUS
     , nvl(c.NOMBRE_FNF_MODIFY,0) NOMBRE_FNF_MODIFY
     , nvl(c.NOMBRE_FORFAIT_DATA,0) NOMBRE_FORFAIT_DATA
     --
     , nvl(b.ADMINISTRATIVE_REGION,'') ADMINISTRATIVE_REGION
     , nvl(b.COMMERCIAL_REGION,'') COMMERCIAL_REGION
     , nvl(b.TOWNNAME,'') TOWNNAME
     , nvl(b.SITE_NAME,'') SITE_NAME
     --
     , nvl(e.TAC_CODE_HANDSET,'') TAC_CODE_HANDSET
     , nvl(e.CONSTRUCTOR, '') CONSTRUCTOR_HANDSET
     , nvl(e.MODEL,'') MODEL_HANDSET
     , nvl(e.DATA_COMPATIBLE,'') DATA_COMPATIBLE_HANDSET
     --
     , nvl(f.TAC_CODE_HANDSET,'') TAC_CODE_HANDSET_3MBE4
     , nvl(f.CONSTRUCTOR, '') CONSTRUCTOR_HANDSET_3MBE4
     , nvl(f.MODEL,'') MODEL_HANDSET_3MBE4
     , nvl(f.DATA_COMPATIBLE,'') DATA_COMPATIBLE_HANDSET_3MBE4
     --
     , nvl(c.MONTANT_MAXI_BONUS,0) MONTANT_MAXI_BONUS
     , nvl(c.MONTANT_PRO,0) MONTANT_PRO
     , nvl(c.MONTANT_INTERNATIONAL,0) MONTANT_INTERNATIONAL
     , nvl(c.MONTANT_PACK_SMS,0) MONTANT_PACK_SMS
     , nvl(c.MONTANT_WS,0) MONTANT_WS
     , nvl(c.MONTANT_ORANGE_BONUS_ILLIMITE,0) MONTANT_ORANGE_BONUS_ILLIMITE
     , nvl(c.MONTANT_ORANGE_PHENIX,0) MONTANT_ORANGE_PHENIX
     , nvl(c.MONTANT_RECHARGE_PLENTY,0) MONTANT_RECHARGE_PLENTY
     --
     , nvl(c.NOMBRE_MAXI_BONUS,0) NOMBRE_MAXI_BONUS
     , nvl(c.NOMBRE_PRO,0) NOMBRE_PRO
     , nvl(c.NOMBRE_INTERNATIONAL,0) NOMBRE_INTERNATIONAL
     , nvl(c.NOMBRE_PACK_SMS,0) NOMBRE_PACK_SMS
     , nvl(c.NOMBRE_WS,0) NOMBRE_WS
     , nvl(c.NOMBRE_ORANGE_BONUS_ILLIMITE,0) NOMBRE_ORANGE_BONUS_ILLIMITE
     , nvl(c.NOMBRE_ORANGE_PHENIX,0) NOMBRE_ORANGE_PHENIX
     , nvl(c.NOMBRE_RECHARGE_PLENTY,0) NOMBRE_RECHARGE_PLENTY
     --
     , nvl(e.IMEI,'') IMEI_HANDSET
     , nvl(e.DATA2G_COMPATIBLE,'') DAT2G_COMPATIBLE_HANDSET
     , nvl(e.DATA3G_COMPATIBLE,'') DAT3G_COMPATIBLE_HANDSET
     , nvl(e.DATA4G_COMPATIBLE,'') DAT4G_COMPATIBLE_HANDSET
     --
     , nvl(f.IMEI,'') IMEI_HANDSET_3MBE4
     , nvl(f.DATA2G_COMPATIBLE,'') DAT2G_COMPATIBLE_HANDSET_3MBE4
     , nvl(f.DATA3G_COMPATIBLE,'') DAT3G_COMPATIBLE_HANDSET_3MBE4
     , nvl(f.DATA4G_COMPATIBLE,'') DAT4G_COMPATIBLE_HANDSET_3MBE4
      ,CURRENT_TIMESTAMP INSERT_DATE
      , c.EVENT_MONTH EVENT_MONTH
     FROM
     (SELECT EVENT_DATE ACCOUNT_ACTIVITY_EVENT_DATE
     , MSISDN
     ,FORMULE
     ,GP_STATUS_DATE DISCONNEXION_DATE
     ,ACTIVATION_DATE
     ,DATE_FORMAT(abs(cast(months_between(GP_STATUS_DATE, ACTIVATION_DATE)as int))) AGE
     ,MAX(SUBSTR(OG_CALL,1,7)) LAST_OUTGOING_CALL_MONTH
     ,SUBSTR(GREATEST(IC_CALL_1,IC_CALL_2,IC_CALL_3,IC_CALL_4),1,7) LAST_INCOMING_CALL_MONTH
     ,SUM(REMAIN_CREDIT_MAIN) REMAIN_CREDIT_MAIN
     ,SUM(REMAIN_CREDIT_PROMO) REMAIN_CREDIT_PROMO
     ,PLATFORM_STATUS
     FROM MON.SPARK_TT_GROUP_SUBS_ACCT_DISCONNECT
     WHERE MSISDN NOT LIKE '692%'
     AND EVENT_DATE =date_sub(last_day('###SLICE_VALUE###'|| '-01'),-1)
     GROUP BY EVENT_DATE
     , MSISDN
     ,FORMULE
     ,GP_STATUS_DATE
     ,ACTIVATION_DATE
     ,DATE_FORMAT(abs(cast(months_between(GP_STATUS_DATE, ACTIVATION_DATE)as int)))
     ,SUBSTR(GREATEST(IC_CALL_1,IC_CALL_2,IC_CALL_3,IC_CALL_4),1,7),PLATFORM_STATUS
     ) a
     LEFT JOIN
     ( select MSISDN
     , SITE_NAME
     , TOWNNAME
     , ADMINISTRATIVE_REGION
     , COMMERCIAL_REGION
     from MON.SPARK_FT_CLIENT_LAST_SITE_LOCATION b
     where b.EVENT_MONTH =  '###SLICE_VALUE###'
     ) b
       ON a.MSISDN=b.MSISDN
     LEFT JOIN
       (SELECT EVENT_MONTH
     , (CASE WHEN length(b.MSISDN) = 8 THEN '6'||b.MSISDN ELSE b.MSISDN END) MSISDN
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
     from MON.SPARK_TF_SPECIFIC_SUBSCRIPTION_MONTH b
     WHERE EVENT_MONTH BETWEEN  SUBSTR(ADD_MONTHS('###SLICE_VALUE###' || '-01' , -5),1,7) AND '###SLICE_VALUE###'
     GROUP BY EVENT_MONTH
     ,(CASE WHEN length(b.MSISDN) = 8 THEN '6'||b.MSISDN ELSE b.MSISDN END)
     ) c
       ON a.MSISDN=c.MSISDN
     LEFT JOIN (
     SELECT substring(e.imei,1,8) tac_code_handset, e.*, f.*
     FROM  (SELECT a.*
     , ROW_NUMBER() OVER(PARTITION BY MSISDN ORDER BY TOTAL_DAYS_COUNT DESC) AS IMEI_RN
     FROM MON.SPARK_FT_IMEI_TRAFFIC_MONTHLY a
     WHERE smonth =  '###SLICE_VALUE###'
     ) e
     LEFT JOIN (select a.* , (CASE WHEN UMTS = 'O' or GPRS = 'O' or EDGE = 'O' or LTE = 'O' THEN 'YES' ELSE 'NO' END) DATA_COMPATIBLE  , (CASE WHEN GPRS = 'O' or EDGE = 'O' or EDGE = 'E' THEN 'YES' ELSE 'NO' END) DATA2G_COMPATIBLE , (CASE WHEN UMTS = 'O' or LTE = 'O' THEN 'YES' ELSE 'NO' END) DATA3G_COMPATIBLE , (CASE WHEN LTE = 'O' THEN 'YES' ELSE 'NO' END) DATA4G_COMPATIBLE  from dim.spark_dt_handset_ref a
     ) f
     ON substring(e.imei,1,8) = f.tac_code
     WHERE e.IMEI_RN = 1
     ) e
        ON a.MSISDN=e.MSISDN
     LEFT JOIN (
     SELECT substring(e.imei,1,8) tac_code_handset, e.*, f.* FROM (SELECT a.* , ROW_NUMBER() OVER(PARTITION BY MSISDN ORDER BY TOTAL_DAYS_COUNT DESC) AS IMEI_RN FROM MON.SPARK_FT_IMEI_TRAFFIC_MONTHLY a WHERE smonth = SUBSTR(ADD_MONTHS('###SLICE_VALUE###' || '-01' , -3),1,7)) e LEFT JOIN (select a.*, (CASE WHEN UMTS = 'O' or GPRS = 'O' or EDGE = 'O' or LTE = 'O' THEN 'YES' ELSE 'NO' END) DATA_COMPATIBLE, (CASE WHEN GPRS = 'O' or EDGE = 'O' or EDGE = 'E' THEN 'YES' ELSE 'NO' END) DATA2G_COMPATIBLE ,(CASE WHEN UMTS = 'O' or LTE = 'O' THEN 'YES' ELSE 'NO' END) DATA3G_COMPATIBLE , (CASE WHEN LTE = 'O' THEN 'YES' ELSE 'NO' END) DATA4G_COMPATIBLE from dim.spark_dt_handset_ref a ) f ON substring(e.imei,1,8) = f.tac_code WHERE e.IMEI_RN = 1
     ) f ON a.MSISDN=f.MSISDN