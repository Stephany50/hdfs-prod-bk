INSERT INTO MON.SPARK_FT_CRM_OUTPUT_B2C
select '###SLICE_VALUE###' EVENT_MONTH,max(NOM),max(PRENOM),a.COMPTE_B2C Code_Client
        ,sum(nvl(voix,0)) Valeur_Client_Voix
        ,sum(nvl(sms,0))Valeur_Client_SMS
        ,sum(nvl(gprs,0))Valeur_Client_Data
        ,0 Valeur_Client_OM
        ,sum(nvl(voix,0)+nvl(sms,0)+nvl(gprs,0))Valeur_Client_Globale
        ,max(case when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(voix,0) <= 0  and nvl(DISTRIBUTEUR,0)=0 then 100
              when CONTRACT_TYPE='HYBRID' and nvl(voix,0) <= 0  and nvl(DISTRIBUTEUR,0)=0 then 110
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(voix,0) <= 0  and nvl(DISTRIBUTEUR,0)=0 then 120
              when nvl(voix,0) <= 0  and nvl(DISTRIBUTEUR,0)=1 then 130
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(voix,0) > 0 and nvl(voix,0) <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 200
              when CONTRACT_TYPE='HYBRID' and nvl(voix,0) > 0 and nvl(voix,0) <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 210
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(voix,0) > 0 and nvl(voix,0) <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 220
              when nvl(voix,0) > 0 and nvl(voix,0) <= 1500  and nvl(DISTRIBUTEUR,0)=1 then 230
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
              when nvl(voix,0) > 12000  and nvl(DISTRIBUTEUR,0)=1 then 530 ELSE 0          
        end) Segment_Valeur_Client_Voix
        ,max(case when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(gprs,0)<= 0  and nvl(DISTRIBUTEUR,0)=0 then 102
              when CONTRACT_TYPE='HYBRID' and nvl(gprs,0)<= 0  and nvl(DISTRIBUTEUR,0)=0 then 112
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(gprs,0)<= 0  and nvl(DISTRIBUTEUR,0)=0 then 122
              when nvl(gprs,0)<= 0  and nvl(DISTRIBUTEUR,0)=1 then 132
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(gprs,0)> 0 and nvl(gprs,0)<= 1500  and nvl(DISTRIBUTEUR,0)=0 then 202
              when CONTRACT_TYPE='HYBRID' and nvl(gprs,0)> 0 and nvl(gprs,0)<= 1500  and nvl(DISTRIBUTEUR,0)=0 then 212
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(gprs,0)> 0 and nvl(gprs,0)<= 1500  and nvl(DISTRIBUTEUR,0)=0 then 222
              when nvl(gprs,0)> 0 and nvl(gprs,0)<= 1500  and nvl(DISTRIBUTEUR,0)=1 then 232
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(gprs,0)> 1500 and nvl(gprs,0)<= 6500  and nvl(DISTRIBUTEUR,0)=0 then 302
              when CONTRACT_TYPE='HYBRID' and nvl(gprs,0)> 1500 and nvl(gprs,0)<= 6500  and nvl(DISTRIBUTEUR,0)=0 then 312
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(gprs,0)> 1500 and nvl(gprs,0)<= 6500  and nvl(DISTRIBUTEUR,0)=0 then 322
              when nvl(gprs,0)> 1500 and nvl(gprs,0)<= 6500  and nvl(DISTRIBUTEUR,0)=1 then 332
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(gprs,0)> 6500 and nvl(gprs,0)<= 12000  and nvl(DISTRIBUTEUR,0)=0 then 402
              when CONTRACT_TYPE='HYBRID' and nvl(gprs,0)> 6500 and nvl(gprs,0)<= 12000  and nvl(DISTRIBUTEUR,0)=0 then 412
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(gprs,0)> 6500 and nvl(gprs,0)<= 12000  and nvl(DISTRIBUTEUR,0)=0 then 422
              when nvl(gprs,0)> 6500 and nvl(gprs,0)<= 12000  and nvl(DISTRIBUTEUR,0)=1 then 432      
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(gprs,0)> 12000 and nvl(DISTRIBUTEUR,0)=0 then 502
              when CONTRACT_TYPE='HYBRID' and nvl(gprs,0)> 12000  and nvl(DISTRIBUTEUR,0)=0 then 512
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(gprs,0)> 12000  and nvl(DISTRIBUTEUR,0)=0 then 522
              when nvl(gprs,0)> 12000  and nvl(DISTRIBUTEUR,0)=1 then 532 ELSE 102          
        end) Segment_Valeur_Client_Data             
        ,max(case when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(sms,0)<= 0  and nvl(DISTRIBUTEUR,0)=0 then 101
              when CONTRACT_TYPE='HYBRID' and nvl(sms,0)<= 0  and nvl(DISTRIBUTEUR,0)=0 then 111
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(sms,0)<= 0  and nvl(DISTRIBUTEUR,0)=0 then 121
              when nvl(sms,0)<= 0  and nvl(DISTRIBUTEUR,0)=1 then 131
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(sms,0)> 0 and nvl(sms,0)<= 1500  and nvl(DISTRIBUTEUR,0)=0 then 201
              when CONTRACT_TYPE='HYBRID' and nvl(sms,0)> 0 and nvl(sms,0)<= 1500  and nvl(DISTRIBUTEUR,0)=0 then 211
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(sms,0)> 0 and nvl(sms,0)<= 1500  and nvl(DISTRIBUTEUR,0)=0 then 221
              when nvl(sms,0)> 0 and nvl(sms,0)<= 1500  and nvl(DISTRIBUTEUR,0)=1 then 231
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(sms,0)> 1500 and nvl(sms,0)<= 6500  and nvl(DISTRIBUTEUR,0)=0 then 301
              when CONTRACT_TYPE='HYBRID' and nvl(sms,0)> 1500 and nvl(sms,0)<= 6500  and nvl(DISTRIBUTEUR,0)=0 then 311
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(sms,0)> 1500 and nvl(sms,0)<= 6500  and nvl(DISTRIBUTEUR,0)=0 then 321
              when nvl(sms,0)> 1500 and nvl(sms,0)<= 6500  and nvl(DISTRIBUTEUR,0)=1 then 331
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(sms,0)> 6500 and nvl(sms,0)<= 12000  and nvl(DISTRIBUTEUR,0)=0 then 401
              when CONTRACT_TYPE='HYBRID' and nvl(sms,0)> 6500 and nvl(sms,0)<= 12000  and nvl(DISTRIBUTEUR,0)=0 then 411
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(sms,0)> 6500 and nvl(sms,0)<= 12000  and nvl(DISTRIBUTEUR,0)=0 then 421
              when nvl(sms,0)> 6500 and nvl(sms,0)<= 12000  and nvl(DISTRIBUTEUR,0)=1 then 431      
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(sms,0)> 12000 and nvl(DISTRIBUTEUR,0)=0 then 501
              when CONTRACT_TYPE='HYBRID' and nvl(sms,0)> 12000  and nvl(DISTRIBUTEUR,0)=0 then 511
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(sms,0)> 12000  and nvl(DISTRIBUTEUR,0)=0 then 521
              when nvl(sms,0)> 12000  and nvl(DISTRIBUTEUR,0)=1 then 531 ELSE 101          
        end) Segment_Valeur_Client_SMS 
        ,14 Segment_Valeur_Client_OM 
        ,max(case when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)<= 0  and nvl(DISTRIBUTEUR,0)=0 then 10
              when CONTRACT_TYPE='HYBRID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)<= 0  and nvl(DISTRIBUTEUR,0)=0 then 11
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)<= 0  and nvl(DISTRIBUTEUR,0)=0 then 12
              when nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)<= 0  and nvl(DISTRIBUTEUR,0)=1 then 13
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)> 0 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)<= 1500  and nvl(DISTRIBUTEUR,0)=0 then 20
              when CONTRACT_TYPE='HYBRID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)> 0 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)<= 1500  and nvl(DISTRIBUTEUR,0)=0 then 21
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)> 0 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)<= 1500  and nvl(DISTRIBUTEUR,0)=0 then 22
              when nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)> 0 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)<= 1500  and nvl(DISTRIBUTEUR,0)=1 then 23
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)> 1500 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)<= 6500  and nvl(DISTRIBUTEUR,0)=0 then 30
              when CONTRACT_TYPE='HYBRID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)> 1500 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)<= 6500  and nvl(DISTRIBUTEUR,0)=0 then 31
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)> 1500 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)<= 6500  and nvl(DISTRIBUTEUR,0)=0 then 32
              when nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)> 1500 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)<= 6500  and nvl(DISTRIBUTEUR,0)=1 then 33
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)> 6500 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)<= 12000  and nvl(DISTRIBUTEUR,0)=0 then 40
              when CONTRACT_TYPE='HYBRID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)> 6500 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)<= 12000  and nvl(DISTRIBUTEUR,0)=0 then 41
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)> 6500 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)<= 12000  and nvl(DISTRIBUTEUR,0)=0 then 42
              when nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)> 6500 and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)<= 12000  and nvl(DISTRIBUTEUR,0)=1 then 43
              --
              when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)> 12000 and nvl(DISTRIBUTEUR,0)=0 then 50
              when CONTRACT_TYPE='HYBRID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)> 12000  and nvl(DISTRIBUTEUR,0)=0 then 51
              when CONTRACT_TYPE='PURE POSTPAID' and nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)> 12000  and nvl(DISTRIBUTEUR,0)=0 then 52
              when nvl(voix,0)+nvl(sms,0)+nvl(gprs,0)> 12000  and nvl(DISTRIBUTEUR,0)=1 then 53 ELSE 10          
        end) Segment_Valeur_Client_Globale ,current_timestamp() inserted_date
        from (
            select * from CDR.SPARK_IT_CRM_COMPTE_B2C where ORIGINAL_FILE_DATE = 
            (select max(ORIGINAL_FILE_DATE) from CDR.SPARK_IT_CRM_COMPTE_B2C where ORIGINAL_FILE_DATE BETWEEN ADD_MONTHS(TO_DATE('###SLICE_VALUE###' || '-01'),1) AND LAST_DAY(ADD_MONTHS(TO_DATE('###SLICE_VALUE###' || '-01'),1)))
          ) a
        LEFT JOIN(
            select * from CDR.SPARK_IT_CRM_ABONNEMENTS where ORIGINAL_FILE_DATE = 
            (select max(ORIGINAL_FILE_DATE) from CDR.SPARK_IT_CRM_ABONNEMENTS where ORIGINAL_FILE_DATE BETWEEN ADD_MONTHS(TO_DATE('###SLICE_VALUE###' || '-01'),1) AND LAST_DAY(ADD_MONTHS(TO_DATE('###SLICE_VALUE###' || '-01'),1)))
          ) b ON a.COMPTE_B2C = b.COMPTE_B2C
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
        ) c ON b.MSISDN_IDWIMAX = c.MSISDN
        group by a.COMPTE_B2C