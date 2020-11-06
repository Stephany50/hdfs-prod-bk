INSERT INTO MON.SPARK_FT_CRM_OUTPUT_ABONNEMENT
    SELECT DISTINCT '###SLICE_VALUE###' event_month,MSISDN_IDWIMAX
        , nvl(voix,0) VALEUR_LIGNE_VOIX
        , nvl(sms,0)VALEUR_LIGNE_SMS
        , nvl(gprs,0) VALEUR_LIGNE_DATA
        --Ajout du revenu moyen om
        , c.REV_OM  VALEUR_LIGNE_OM
        , nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) VALEUR_LIGNE_GLOBALE
        ,(case when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and voix is null and nvl(DISTRIBUTEUR,0)=0 then 100
              when CONTRACT_TYPE='HYBRID' and voix is null  and nvl(DISTRIBUTEUR,0)=0 then 110
              when CONTRACT_TYPE='PURE POSTPAID' and voix is null  and nvl(DISTRIBUTEUR,0)=0 then 120
              when voix is null and nvl(DISTRIBUTEUR,0)=1 then 130
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(voix,0) >= 0 and nvl(voix,0) <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 200
              when CONTRACT_TYPE='HYBRID' and nvl(voix,0) >= 0 and nvl(voix,0) <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 210
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(voix,0) >= 0 and nvl(voix,0) <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 220
              when nvl(voix,0) >= 0 and nvl(voix,0) <= 1500  and nvl(DISTRIBUTEUR,0)=1 then 230
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(voix,0) > 1500 and nvl(voix,0) <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 300
              when CONTRACT_TYPE='HYBRID' and nvl(voix,0) > 1500 and nvl(voix,0) <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 310
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(voix,0) > 1500 and nvl(voix,0) <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 320
              when nvl(voix,0) > 1500 and nvl(voix,0) <= 6500  and nvl(DISTRIBUTEUR,0)=1 then 330
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(voix,0) > 6500 and nvl(voix,0) <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 400
              when CONTRACT_TYPE='HYBRID' and nvl(voix,0) > 6500 and nvl(voix,0) <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 410
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(voix,0) > 6500 and nvl(voix,0) <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 420
              when nvl(voix,0) > 6500 and nvl(voix,0) <= 12000  and nvl(DISTRIBUTEUR,0)=1 then 430      
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(voix,0) > 12000 and nvl(DISTRIBUTEUR,0)=0 then 500
              when CONTRACT_TYPE='HYBRID' and nvl(voix,0) > 12000  and nvl(DISTRIBUTEUR,0)=0 then 510
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(voix,0) > 12000  and nvl(DISTRIBUTEUR,0)=0 then 520
              when nvl(voix,0) > 12000  and nvl(DISTRIBUTEUR,0)=1 then 530 ELSE 100          
        end) SEGMENT_LIGNE_VOIX
        ,(case when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and gprs is null  and nvl(DISTRIBUTEUR,0)=0 then 102
              when CONTRACT_TYPE='HYBRID' and gprs is null and nvl(DISTRIBUTEUR,0)=0 then 112
              when CONTRACT_TYPE='PURE POSTPAID' and gprs is null  and nvl(DISTRIBUTEUR,0)=0 then 122
              when gprs is null and nvl(DISTRIBUTEUR,0)=1 then 132
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(gprs,0) >= 0 and nvl(gprs,0) <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 202
              when CONTRACT_TYPE='HYBRID' and nvl(gprs,0) >= 0 and nvl(gprs,0) <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 212
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(gprs,0) >= 0 and nvl(gprs,0) <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 222
              when nvl(gprs,0) >= 0 and nvl(gprs,0) <= 1500  and nvl(DISTRIBUTEUR,0)=1 then 232
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(gprs,0) > 1500 and nvl(gprs,0) <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 302
              when CONTRACT_TYPE='HYBRID' and nvl(gprs,0) > 1500 and nvl(gprs,0) <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 312
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(gprs,0) > 1500 and nvl(gprs,0) <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 322
              when nvl(gprs,0) > 1500 and nvl(gprs,0) <= 6500  and nvl(DISTRIBUTEUR,0)=1 then 332
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(gprs,0) > 6500 and nvl(gprs,0) <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 402
              when CONTRACT_TYPE='HYBRID' and nvl(gprs,0) > 6500 and nvl(gprs,0) <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 412
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(gprs,0) > 6500 and nvl(gprs,0) <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 422
              when nvl(gprs,0) > 6500 and nvl(gprs,0) <= 12000  and nvl(DISTRIBUTEUR,0)=1 then 432      
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(gprs,0) > 12000 and nvl(DISTRIBUTEUR,0)=0 then 502
              when CONTRACT_TYPE='HYBRID' and nvl(gprs,0) > 12000  and nvl(DISTRIBUTEUR,0)=0 then 512
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(gprs,0) > 12000  and nvl(DISTRIBUTEUR,0)=0 then 522
              when nvl(gprs,0) > 12000  and nvl(DISTRIBUTEUR,0)=1 then 532 ELSE 102          
        end) SEGMENT_LIGNE_DATA                     
        ,(case when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and sms is null  and nvl(DISTRIBUTEUR,0)=0 then 101
              when CONTRACT_TYPE='HYBRID' and sms is null  and nvl(DISTRIBUTEUR,0)=0 then 111
              when CONTRACT_TYPE='PURE POSTPAID' and sms is null  and nvl(DISTRIBUTEUR,0)=0 then 121
              when sms is null and nvl(DISTRIBUTEUR,0)=1 then 131
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(sms,0) >= 0 and nvl(sms,0) <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 201
              when CONTRACT_TYPE='HYBRID' and nvl(sms,0) >= 0 and nvl(sms,0) <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 211
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(sms,0) >= 0 and nvl(sms,0) <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 221
              when nvl(sms,0) >= 0 and nvl(sms,0) <= 1500  and nvl(DISTRIBUTEUR,0)=1 then 231
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(sms,0) > 1500 and nvl(sms,0) <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 301
              when CONTRACT_TYPE='HYBRID' and nvl(sms,0) > 1500 and nvl(sms,0) <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 311
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(sms,0) > 1500 and nvl(sms,0) <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 321
              when nvl(sms,0) > 1500 and nvl(sms,0) <= 6500  and nvl(DISTRIBUTEUR,0)=1 then 331
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(sms,0) > 6500 and nvl(sms,0) <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 401
              when CONTRACT_TYPE='HYBRID' and nvl(sms,0) > 6500 and nvl(sms,0) <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 411
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(sms,0) > 6500 and nvl(sms,0) <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 421
              when nvl(sms,0) > 6500 and nvl(sms,0) <= 12000  and nvl(DISTRIBUTEUR,0)=1 then 431      
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(sms,0) > 12000 and nvl(DISTRIBUTEUR,0)=0 then 501
              when CONTRACT_TYPE='HYBRID' and nvl(sms,0) > 12000  and nvl(DISTRIBUTEUR,0)=0 then 511
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(sms,0) > 12000  and nvl(DISTRIBUTEUR,0)=0 then 521
              when nvl(sms,0) > 12000  and nvl(DISTRIBUTEUR,0)=1 then 531 ELSE 101          
        end) SEGMENT_LIGNE_SMS
        ,(case when  c.REV_OM is null then 14
              when  nvl(c.REV_OM,0) >= 0 and nvl(c.REV_OM,0) <= 500   then 24
              when  nvl(c.REV_OM,0) > 500 and nvl(c.REV_OM,0) <= 3000   then 34
              when  nvl(c.REV_OM,0) > 3000 and nvl(c.REV_OM,0) <= 12000   then 44 
              when  nvl(c.REV_OM,0) > 12000 then 54 ELSE 14         
        end) SEGMENT_LIGNE_OM 
        ,(case 
              when d.msisdn is not null then 50
              --when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) > 12000 and nvl(DISTRIBUTEUR,0)=0 then 50
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and voix is null and sms is null and gprs is null and c.REV_OM is null and nvl(DISTRIBUTEUR,0)=0 then 10
              when CONTRACT_TYPE='HYBRID' and voix is null and sms is null and gprs is null and c.REV_OM is null and nvl(DISTRIBUTEUR,0)=0 then 11
              when CONTRACT_TYPE='PURE POSTPAID' and voix is null and sms is null and gprs is null and c.REV_OM is null and nvl(DISTRIBUTEUR,0)=0 then 12
              when voix is null and sms is null and gprs is null and nvl(DISTRIBUTEUR,0)=1 and c.REV_OM is null then 13
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) >= 0 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) <= 500  and nvl(DISTRIBUTEUR,0)=0 then 20
              when CONTRACT_TYPE='HYBRID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) >= 0 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) <= 500  and nvl(DISTRIBUTEUR,0)=0 then 21
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) >= 0 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) <= 500  and nvl(DISTRIBUTEUR,0)=0 then 22
              when nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) >= 0 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) <= 500  and nvl(DISTRIBUTEUR,0)=1 then 23
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) > 500 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) <= 3000  and nvl(DISTRIBUTEUR,0)=0 then 30
              when CONTRACT_TYPE='HYBRID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) > 500 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) <= 3000  and nvl(DISTRIBUTEUR,0)=0 then 31
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) > 500 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) <= 3000  and nvl(DISTRIBUTEUR,0)=0 then 32
              when nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) > 500 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) <= 3000  and nvl(DISTRIBUTEUR,0)=1 then 33
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) > 3000 and nvl(DISTRIBUTEUR,0)=0 then 40
              when CONTRACT_TYPE='HYBRID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) > 3000 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 41
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) > 3000 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 42
              when nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) > 3000 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) <= 12000  and nvl(DISTRIBUTEUR,0)=1 then 43
              --
              when CONTRACT_TYPE='HYBRID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) > 12000  and nvl(DISTRIBUTEUR,0)=0 then 51
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) > 12000  and nvl(DISTRIBUTEUR,0)=0 then 52
              when nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)+nvl(c.REV_OM,0) > 12000  and nvl(DISTRIBUTEUR,0)=1 then 53 ELSE 10          
        end) SEGMENT_LIGNE_GLOBALE, current_timestamp() AS inserted_date ,
       NULL SEGMENTATION  

    from ( select * from CDR.SPARK_IT_CRM_ABONNEMENTS where ORIGINAL_FILE_DATE = 
        (select max(ORIGINAL_FILE_DATE) from CDR.SPARK_IT_CRM_ABONNEMENTS where ORIGINAL_FILE_DATE BETWEEN ADD_MONTHS(TO_DATE('###SLICE_VALUE###' || '-01'),1) AND LAST_DAY(ADD_MONTHS(TO_DATE('###SLICE_VALUE###' || '-01'),1)))
     ) a
    LEFT JOIN (
      select r.event_month mois, r.msisdn msisdn, r.contract_type CONTRACT_TYPE,
             (CASE 
                    WHEN s.msisdn IS NOT NULL THEN 1 ELSE 0 END) DISTRIBUTEUR,
             (r.total_voice_revenue + r.subs_voice_amount) VOIX,
             (r.total_sms_revenue + r.subs_sms_amount) SMS, (r.total_data_revenue + r.subs_data_amount) GPRS
      from 
          MON.SPARK_FT_MARKETING_DATAMART_MONTH r
          LEFT JOIN
          (SELECT DISTINCT PRIMARY_MSISDN AS MSISDN  FROM CDR.SPARK_IT_ZEBRA_MASTER 
            WHERE TRANSACTION_DATE = LAST_DAY(TO_DATE('###SLICE_VALUE###' || '-01')) AND USER_STATUS IN ( 'Active','Suspend Request','New')) s
          ON r.msisdn = s.msisdn
      where r.event_month = '###SLICE_VALUE###'
     ) b on a.MSISDN_IDWIMAX = b.msisdn

    LEFT JOIN ( select MSISDN, (REV_OM_M_3 + REV_OM_M_2 + case when REV_OM_M_1 =-1 then 0 else REV_OM_M_1 end)/(
        case when (case when REV_OM_M_1 in (-1,0) then 0 else 1 end + case when REV_OM_M_2 in (-1,0) then 0 else 1 end + 
        case when REV_OM_M_3 in (-1,0) then 0 else 1 end)=0 then 1 else (case when REV_OM_M_1 in (-1,0) then 0 else 1 end + case when REV_OM_M_2 in (-1,0) then 0 else 1 end + 
        case when REV_OM_M_3 in (-1,0) then 0 else 1 end) end )  REV_OM
      from MON.SPARK_FT_BDI
      where event_date=(select max(event_date) from MON.SPARK_FT_BDI)) c on a.MSISDN_IDWIMAX=c.MSISDN
    LEFT JOIN (select distinct msisdn from DIM.DT_MSISDN_PREMIUM ) d on a.MSISDN_IDWIMAX = d.msisdn