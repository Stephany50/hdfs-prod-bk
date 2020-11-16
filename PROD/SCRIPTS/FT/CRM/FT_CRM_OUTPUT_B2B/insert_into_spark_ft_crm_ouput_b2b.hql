INSERT INTO MON.SPARK_FT_CRM_OUTPUT_B2B
select ID_CLIENT_B2B_CRM RAISON_SOCIALE
, ID_CLIENT_B2B_CRM CODE_CLIENT
, sum(voix) VALEUR_CLIENT_VOIX
, sum(sms) VALEUR_CLIENT_SMS
, sum(gprs) VALEUR_CLIENT_DATA
, 0 VALEUR_CLIENT_OM
, sum(voix+sms+gprs) VALEUR_CLIENT_GLOBALE
,max(case when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and voix <= 0  and nvl(DISTRIBUTEUR,0)=0 then 100
        when CONTRACT_TYPE='HYBRID' and voix <= 0  and nvl(DISTRIBUTEUR,0)=0 then 110
        when CONTRACT_TYPE='PURE POSTPAID' and voix <= 0  and nvl(DISTRIBUTEUR,0)=0 then 120
        when voix <= 0  and nvl(DISTRIBUTEUR,0)=1 then 130
        --
        when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and voix > 0 and voix <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 200
        when CONTRACT_TYPE='HYBRID' and voix > 0 and voix <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 210
        when CONTRACT_TYPE='PURE POSTPAID' and voix > 0 and voix <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 220
        when voix > 0 and voix <= 1500  and nvl(DISTRIBUTEUR,0)=1 then 230
        --
        when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and voix > 1500 and voix <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 300
        when CONTRACT_TYPE='HYBRID' and voix > 1500 and voix <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 310
        when CONTRACT_TYPE='PURE POSTPAID' and voix > 1500 and voix <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 320
        when voix > 1500 and voix <= 6500  and nvl(DISTRIBUTEUR,0)=1 then 330
        --
        when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and voix > 6500 and voix <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 400
        when CONTRACT_TYPE='HYBRID' and voix > 6500 and voix <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 410
        when CONTRACT_TYPE='PURE POSTPAID' and voix > 6500 and voix <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 420
        when voix > 6500 and voix <= 12000  and nvl(DISTRIBUTEUR,0)=1 then 430      
        --
        when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and voix > 12000 and nvl(DISTRIBUTEUR,0)=0 then 500
        when CONTRACT_TYPE='HYBRID' and voix > 12000  and nvl(DISTRIBUTEUR,0)=0 then 510
        when CONTRACT_TYPE='PURE POSTPAID' and voix > 12000  and nvl(DISTRIBUTEUR,0)=0 then 520
        when voix > 12000  and nvl(DISTRIBUTEUR,0)=1 then 530 ELSE 100          
end) SEGMENT_VALEUR_CLIENT_VOIX
,max(case when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and gprs <= 0  and nvl(DISTRIBUTEUR,0)=0 then 102
        when CONTRACT_TYPE='HYBRID' and gprs <= 0  and nvl(DISTRIBUTEUR,0)=0 then 112
        when CONTRACT_TYPE='PURE POSTPAID' and gprs <= 0  and nvl(DISTRIBUTEUR,0)=0 then 122
        when gprs <= 0  and nvl(DISTRIBUTEUR,0)=1 then 132
        --
        when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and gprs > 0 and gprs <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 202
        when CONTRACT_TYPE='HYBRID' and gprs > 0 and gprs <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 212
        when CONTRACT_TYPE='PURE POSTPAID' and gprs > 0 and gprs <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 222
        when gprs > 0 and gprs <= 1500  and nvl(DISTRIBUTEUR,0)=1 then 232
        --
        when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and gprs > 1500 and gprs <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 302
        when CONTRACT_TYPE='HYBRID' and gprs > 1500 and gprs <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 312
        when CONTRACT_TYPE='PURE POSTPAID' and gprs > 1500 and gprs <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 322
        when gprs > 1500 and gprs <= 6500  and nvl(DISTRIBUTEUR,0)=1 then 332
        --
        when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and gprs > 6500 and gprs <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 402
        when CONTRACT_TYPE='HYBRID' and gprs > 6500 and gprs <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 412
        when CONTRACT_TYPE='PURE POSTPAID' and gprs > 6500 and gprs <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 422
        when gprs > 6500 and gprs <= 12000  and nvl(DISTRIBUTEUR,0)=1 then 432      
        --
        when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and gprs > 12000 and nvl(DISTRIBUTEUR,0)=0 then 502
        when CONTRACT_TYPE='HYBRID' and gprs > 12000  and nvl(DISTRIBUTEUR,0)=0 then 512
        when CONTRACT_TYPE='PURE POSTPAID' and gprs > 12000  and nvl(DISTRIBUTEUR,0)=0 then 522
        when gprs > 12000  and nvl(DISTRIBUTEUR,0)=1 then 532 ELSE 102          
end) SEGMENT_VALEUR_CLIENT_DATA                     
,max(case when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and sms <= 0  and nvl(DISTRIBUTEUR,0)=0 then 101
        when CONTRACT_TYPE='HYBRID' and sms <= 0  and nvl(DISTRIBUTEUR,0)=0 then 111
        when CONTRACT_TYPE='PURE POSTPAID' and sms <= 0  and nvl(DISTRIBUTEUR,0)=0 then 121
        when sms <= 0  and nvl(DISTRIBUTEUR,0)=1 then 131
        --
        when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and sms > 0 and sms <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 201
        when CONTRACT_TYPE='HYBRID' and sms > 0 and sms <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 211
        when CONTRACT_TYPE='PURE POSTPAID' and sms > 0 and sms <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 221
        when sms > 0 and sms <= 1500  and nvl(DISTRIBUTEUR,0)=1 then 231
        --
        when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and sms > 1500 and sms <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 301
        when CONTRACT_TYPE='HYBRID' and sms > 1500 and sms <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 311
        when CONTRACT_TYPE='PURE POSTPAID' and sms > 1500 and sms <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 321
        when sms > 1500 and sms <= 6500  and nvl(DISTRIBUTEUR,0)=1 then 331
        --
        when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and sms > 6500 and sms <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 401
        when CONTRACT_TYPE='HYBRID' and sms > 6500 and sms <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 411
        when CONTRACT_TYPE='PURE POSTPAID' and sms > 6500 and sms <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 421
        when sms > 6500 and sms <= 12000  and nvl(DISTRIBUTEUR,0)=1 then 431      
        --
        when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and sms > 12000 and nvl(DISTRIBUTEUR,0)=0 then 501
        when CONTRACT_TYPE='HYBRID' and sms > 12000  and nvl(DISTRIBUTEUR,0)=0 then 511
        when CONTRACT_TYPE='PURE POSTPAID' and sms > 12000  and nvl(DISTRIBUTEUR,0)=0 then 521
        when sms > 12000  and nvl(DISTRIBUTEUR,0)=1 then 531 ELSE 101          
end) SEGMENT_VALEUR_CLIENT_SMS
,14 SEGMENT_VALEUR_CLIENT_OM 
,max(case when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and voix+sms+gprs <= 0  and nvl(DISTRIBUTEUR,0)=0 then 10
        when CONTRACT_TYPE='HYBRID' and voix+sms+gprs <= 0  and nvl(DISTRIBUTEUR,0)=0 then 11
        when CONTRACT_TYPE='PURE POSTPAID' and voix+sms+gprs <= 0  and nvl(DISTRIBUTEUR,0)=0 then 12
        when voix+sms+gprs <= 0  and nvl(DISTRIBUTEUR,0)=1 then 13
        --
        when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and voix+sms+gprs > 0 and voix+sms+gprs <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 20
        when CONTRACT_TYPE='HYBRID' and voix+sms+gprs > 0 and voix+sms+gprs <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 21
        when CONTRACT_TYPE='PURE POSTPAID' and voix+sms+gprs > 0 and voix+sms+gprs <= 1500  and nvl(DISTRIBUTEUR,0)=0 then 22
        when voix+sms+gprs > 0 and voix+sms+gprs <= 1500  and nvl(DISTRIBUTEUR,0)=1 then 23
        --
        when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and voix+sms+gprs > 1500 and voix+sms+gprs <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 30
        when CONTRACT_TYPE='HYBRID' and voix+sms+gprs > 1500 and voix+sms+gprs <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 31
        when CONTRACT_TYPE='PURE POSTPAID' and voix+sms+gprs > 1500 and voix+sms+gprs <= 6500  and nvl(DISTRIBUTEUR,0)=0 then 32
        when voix+sms+gprs > 1500 and voix+sms+gprs <= 6500  and nvl(DISTRIBUTEUR,0)=1 then 33
        --
        when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and voix+sms+gprs > 6500 and voix+sms+gprs <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 40
        when CONTRACT_TYPE='HYBRID' and voix+sms+gprs > 6500 and voix+sms+gprs <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 41
        when CONTRACT_TYPE='PURE POSTPAID' and voix+sms+gprs > 6500 and voix+sms+gprs <= 12000  and nvl(DISTRIBUTEUR,0)=0 then 42
        when voix+sms+gprs > 6500 and voix+sms+gprs <= 12000  and nvl(DISTRIBUTEUR,0)=1 then 43
        --
        when nvl(CONTRACT_TYPE,'PURE PREPAID')='PURE PREPAID' and voix+sms+gprs > 12000 and nvl(DISTRIBUTEUR,0)=0 then 50
        when CONTRACT_TYPE='HYBRID' and voix+sms+gprs > 12000  and nvl(DISTRIBUTEUR,0)=0 then 51
        when CONTRACT_TYPE='PURE POSTPAID' and voix+sms+gprs > 12000  and nvl(DISTRIBUTEUR,0)=0 then 52
        when voix+sms+gprs > 12000  and nvl(DISTRIBUTEUR,0)=1 then 53 ELSE 10          
end) SEGMENT_VALEUR_CLIENT_GLOBALE, current_timestamp() inserted_date,'###SLICE_VALUE###' EVENT_MONTH    
FROM (
    select * from CDR.SPARK_IT_CRM_ABONNEMENT_HIERARCH where ORIGINAL_FILE_DATE = 
        (select max(ORIGINAL_FILE_DATE) from CDR.SPARK_IT_CRM_ABONNEMENT_HIERARCH where ORIGINAL_FILE_DATE BETWEEN ADD_MONTHS(TO_DATE('###SLICE_VALUE###' || '-01'),1) AND LAST_DAY(ADD_MONTHS(TO_DATE('###SLICE_VALUE###' || '-01'),1)))
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
) b
ON a.MSISDN = b.msisdn  
group by ID_CLIENT_B2B_CRM