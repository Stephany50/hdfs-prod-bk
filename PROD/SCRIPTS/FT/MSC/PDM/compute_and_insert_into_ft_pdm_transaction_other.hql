 INSERT INTO MON.FT_PDM_TRANSACTION_OTHER (EVENT_DATE,MSISDN,OCM_OUT_CALL,DATE_OCM_OUT_CALL,OCM_IN_CALL,DATE_OCM_IN_CALL,OCM_OUT_SMS,DATE_OCM_OUT_SMS,
    OCM_IN_SMS,DATE_OCM_IN_SMS,MTN_OUT_CALL,DATE_MTN_OUT_CALL,MTN_IN_CALL,DATE_MTN_IN_CALL,MTN_OUT_SMS,DATE_MTN_OUT_SMS,MTN_IN_SMS,
    DATE_MTN_IN_SMS,VIETTEL_OUT_CALL,DATE_VIETTEL_OUT_CALL,VIETTEL_IN_CALL,DATE_VIETTEL_IN_CALL,VIETTEL_OUT_SMS,DATE_VIETTEL_OUT_SMS,
    VIETTEL_IN_SMS,DATE_VIETTEL_IN_SMS,CAMTEL_OUT_CALL,DATE_CAMTEL_OUT_CALL,CAMTEL_IN_CALL,DATE_CAMTEL_IN_CALL,CAMTEL_OUT_SMS,
    DATE_CAMTEL_OUT_SMS,CAMTEL_IN_SMS,DATE_CAMTEL_IN_SMS,INTER_OUT_CALL,DATE_INTER_OUT_CALL,INTER_IN_CALL,DATE_INTER_IN_CALL,INTER_OUT_SMS,
    DATE_INTER_OUT_SMS,INTER_IN_SMS,DATE_INTER_IN_SMS,ROAM_OUT_CALL,DATE_ROAM_OUT_CALL,ROAM_IN_CALL,DATE_ROAM_IN_CALL,ROAM_OUT_SMS,
    DATE_ROAM_OUT_SMS,ROAM_IN_SMS,DATE_ROAM_IN_SMS,INSERT_DATE)
SELECT '###SLICE_VALUE###' EVENT_DATE, MSISDN,
    MAX(OCM_OUT_CALL),MAX(DATE_OCM_OUT_CALL),MAX(OCM_IN_CALL),
    MAX(DATE_OCM_IN_CALL),MAX(OCM_OUT_SMS),MAX(DATE_OCM_OUT_SMS),
    MAX(OCM_IN_SMS),MAX(DATE_OCM_IN_SMS),MAX(MTN_OUT_CALL),MAX(DATE_MTN_OUT_CALL),
    MAX(MTN_IN_CALL),MAX(DATE_MTN_IN_CALL),MAX(MTN_OUT_SMS),MAX(DATE_MTN_OUT_SMS),
    MAX(MTN_IN_SMS),MAX(DATE_MTN_IN_SMS),MAX(VIETTEL_OUT_CALL),MAX(DATE_VIETTEL_OUT_CALL),
    MAX(VIETTEL_IN_CALL),MAX(DATE_VIETTEL_IN_CALL),MAX(VIETTEL_OUT_SMS),MAX(DATE_VIETTEL_OUT_SMS),
    MAX(VIETTEL_IN_SMS),MAX(DATE_VIETTEL_IN_SMS),MAX(CAMTEL_OUT_CALL),MAX(DATE_CAMTEL_OUT_CALL),
    MAX(CAMTEL_IN_CALL),MAX(DATE_CAMTEL_IN_CALL),MAX(CAMTEL_OUT_SMS),MAX(DATE_CAMTEL_OUT_SMS),
    MAX(CAMTEL_IN_SMS),MAX(DATE_CAMTEL_IN_SMS),MAX(INTER_OUT_CALL),MAX(DATE_INTER_OUT_CALL),
    MAX(INTER_IN_CALL),MAX(DATE_INTER_IN_CALL),MAX(INTER_OUT_SMS),MAX(DATE_INTER_OUT_SMS),
    MAX(INTER_IN_SMS),MAX(DATE_INTER_IN_SMS),MAX(ROAM_OUT_CALL),MAX(DATE_ROAM_OUT_CALL),
    MAX(ROAM_IN_CALL),MAX(DATE_ROAM_IN_CALL),MAX(ROAM_OUT_SMS),MAX(DATE_ROAM_OUT_SMS),MAX(ROAM_IN_SMS),
    MAX(DATE_ROAM_IN_SMS), CURRENT_TIMESTAMP INSERT_DATE
FROM(
         -- la photo DU JOUR
        SELECT  DATE_SUB('###SLICE_VALUE###',0)  EVENT_DATE, CASE WHEN LENGTH(OTHER_PARTY) = 13 AND substr(OTHER_PARTY,1,3) = '160' AND FN_GET_NNP_MSISDN_SIMPLE_DESTN(OTHER_PARTY)
                            IN ('MTN','VIETTEL','OCM','CAMTEL') THEN SUBSTR(OTHER_PARTY,-9) ELSE OTHER_PARTY END MSISDN
            ,MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'OCM' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL'  THEN 1 ELSE 0 END) OCM_OUT_CALL
            ,MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'OCM' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN TRANSACTION_DATE  ELSE NULL END) DATE_OCM_OUT_CALL
            ,MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'OCM' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN 1 ELSE 0 END) OCM_IN_CALL
            ,MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'OCM' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN TRANSACTION_DATE  ELSE NULL END) DATE_OCM_IN_CALL
            ---For the sms
            ,MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'OCM' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS'  THEN 1 ELSE 0 END) OCM_OUT_SMS
            ,MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'OCM' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN TRANSACTION_DATE  ELSE NULL END) DATE_OCM_OUT_SMS
            ,MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'OCM' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN 1 ELSE 0 END) OCM_IN_SMS
            ,MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'OCM' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN TRANSACTION_DATE  ELSE NULL END) DATE_OCM_IN_SMS
            --
            ,MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'MTN' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL'  THEN 1 ELSE 0 END) MTN_OUT_CALL
            ,MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'MTN' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN TRANSACTION_DATE  ELSE NULL END) DATE_MTN_OUT_CALL
            ,MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'MTN' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN 1 ELSE 0 END) MTN_IN_CALL
            ,MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'MTN' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN TRANSACTION_DATE  ELSE NULL END) DATE_MTN_IN_CALL
            ---For the sms
            ,MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'MTN' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS'  THEN 1 ELSE 0 END) MTN_OUT_SMS
            ,MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'MTN' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN TRANSACTION_DATE  ELSE NULL END) DATE_MTN_OUT_SMS
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'MTN' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN 1 ELSE 0 END) MTN_IN_SMS
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'MTN' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN TRANSACTION_DATE  ELSE NULL END) DATE_MTN_IN_SMS
            ---
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'VIETTEL' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL'  THEN 1 ELSE 0 END) VIETTEL_OUT_CALL
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'VIETTEL' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN TRANSACTION_DATE  ELSE NULL END) DATE_VIETTEL_OUT_CALL
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'VIETTEL' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN 1 ELSE 0 END) VIETTEL_IN_CALL
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'VIETTEL' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN TRANSACTION_DATE  ELSE NULL END) DATE_VIETTEL_IN_CALL
            ---For the sms
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'VIETTEL' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS'  THEN 1 ELSE 0 END) VIETTEL_OUT_SMS
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'VIETTEL' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN TRANSACTION_DATE  ELSE NULL END) DATE_VIETTEL_OUT_SMS
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'VIETTEL' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN 1 ELSE 0 END) VIETTEL_IN_SMS
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'VIETTEL' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN TRANSACTION_DATE  ELSE NULL END) DATE_VIETTEL_IN_SMS
            ---
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'CAMTEL' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL'  THEN 1 ELSE 0 END) CAMTEL_OUT_CALL
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'CAMTEL' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN TRANSACTION_DATE  ELSE NULL END) DATE_CAMTEL_OUT_CALL
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'CAMTEL' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN 1 ELSE 0 END) CAMTEL_IN_CALL
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'CAMTEL' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN TRANSACTION_DATE  ELSE NULL END) DATE_CAMTEL_IN_CALL
            ---For the sms
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'CAMTEL' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS'  THEN 1 ELSE 0 END) CAMTEL_OUT_SMS
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'CAMTEL' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN TRANSACTION_DATE  ELSE NULL END) DATE_CAMTEL_OUT_SMS
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'CAMTEL' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN 1 ELSE 0 END) CAMTEL_IN_SMS
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'CAMTEL' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN TRANSACTION_DATE  ELSE NULL END) DATE_CAMTEL_IN_SMS
            ---
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'INTER' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL'  THEN 1 ELSE 0 END) INTER_OUT_CALL
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'INTER' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN TRANSACTION_DATE  ELSE NULL END) DATE_INTER_OUT_CALL
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'INTER' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN 1 ELSE 0 END) INTER_IN_CALL
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'INTER' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN TRANSACTION_DATE  ELSE NULL END) DATE_INTER_IN_CALL
            ---For the sms
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'INTER' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS'  THEN 1 ELSE 0 END) INTER_OUT_SMS
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'INTER' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN TRANSACTION_DATE  ELSE NULL END) DATE_INTER_OUT_SMS
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'INTER' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN 1 ELSE 0 END) INTER_IN_SMS
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'INTER' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN TRANSACTION_DATE  ELSE NULL END) DATE_INTER_IN_SMS
            ---
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'ROAM' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL'  THEN 1 ELSE 0 END) ROAM_OUT_CALL
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'ROAM' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN TRANSACTION_DATE  ELSE NULL END) DATE_ROAM_OUT_CALL
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'ROAM' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN 1 ELSE 0 END) ROAM_IN_CALL
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'ROAM' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='TEL' THEN TRANSACTION_DATE  ELSE NULL END) DATE_ROAM_IN_CALL
            ---For the sms
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'ROAM' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS'  THEN 1 ELSE 0 END) ROAM_OUT_SMS
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'ROAM' and TRANSACTION_DIRECTION='Entrant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN TRANSACTION_DATE  ELSE NULL END) DATE_ROAM_OUT_SMS
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'ROAM' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN 1 ELSE 0 END) ROAM_IN_SMS
            , MAX(CASE WHEN FN_GET_NNP_MSISDN_SIMPLE_DESTN (SERVED_MSISDN) = 'ROAM' and TRANSACTION_DIRECTION='Sortant' and UPPER(substr(TRANSACTION_TYPE,1,3))='SMS' THEN TRANSACTION_DATE  ELSE NULL END) DATE_ROAM_IN_SMS
            --, CURRENT_TIMESTAMP INSERT_DATE
        FROM MON.FT_MSC_TRANSACTION
        WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
               -- Exclure les numeros alphanumeriques et numero de push sms ocm
          AND (CASE WHEN OLD_CALLING_NUMBER IS NULL THEN 1
                WHEN OLD_CALLING_NUMBER IN ('23799900929', '99900929', '237699900929', '699900929') THEN 0
                WHEN  OLD_CALLING_NUMBER REGEXP '^[\+]*[0-9]+$'    THEN 1
                ELSE 0 END) = 1
             -- Exclure notifications ZEBRA (liste à vérifier/compléter)
           AND OTHER_PARTY NOT IN ('937','938','924')
        GROUP BY DATE_SUB('###SLICE_VALUE###',0), CASE WHEN LENGTH(OTHER_PARTY) = 13 AND substr(OTHER_PARTY,1,3) = '160' AND FN_GET_NNP_MSISDN_SIMPLE_DESTN(OTHER_PARTY)
                                 IN ('MTN','VIETTEL','OCM','CAMTEL') THEN SUBSTR(OTHER_PARTY,-9) ELSE OTHER_PARTY END
        --
        UNION ALL
        SELECT EVENT_DATE, MSISDN
            ,CASE WHEN DATE_OCM_OUT_CALL>=DATE_SUB('###SLICE_VALUE###',90)    THEN 1 ELSE 0 END OCM_OUT_CALL
            , DATE_OCM_OUT_CALL
            ,CASE WHEN DATE_OCM_IN_CALL>=DATE_SUB('###SLICE_VALUE###',90)   THEN 1 ELSE 0 END OCM_IN_CALL
            , DATE_OCM_IN_CALL
            ---For the sms
            ,CASE WHEN DATE_OCM_OUT_SMS>=DATE_SUB('###SLICE_VALUE###',90)  THEN 1 ELSE 0 END OCM_OUT_SMS
            , DATE_OCM_OUT_SMS
            ,CASE WHEN DATE_OCM_IN_SMS>=DATE_SUB('###SLICE_VALUE###',90)  THEN 1 ELSE 0 END OCM_IN_SMS
            , DATE_OCM_IN_SMS
            --
            ,CASE WHEN DATE_MTN_OUT_CALL >=DATE_SUB('###SLICE_VALUE###',90)   THEN 1 ELSE 0 END MTN_OUT_CALL
            , DATE_MTN_OUT_CALL
            ,CASE WHEN DATE_MTN_IN_CALL >=DATE_SUB('###SLICE_VALUE###',90)  THEN 1 ELSE 0 END MTN_IN_CALL
            , DATE_MTN_IN_CALL
            ---For the sms
            ,CASE WHEN DATE_MTN_OUT_SMS >=DATE_SUB('###SLICE_VALUE###',90)   THEN 1 ELSE 0 END MTN_OUT_SMS
            , DATE_MTN_OUT_SMS
            ,CASE WHEN DATE_MTN_IN_SMS >=DATE_SUB('###SLICE_VALUE###',90)  THEN 1 ELSE 0 END MTN_IN_SMS
            , DATE_MTN_IN_SMS
            ---
            ,CASE WHEN DATE_VIETTEL_OUT_CALL >=DATE_SUB('###SLICE_VALUE###',90)   THEN 1 ELSE 0 END VIETTEL_OUT_CALL
            , DATE_VIETTEL_OUT_CALL
            --
            ,CASE WHEN DATE_VIETTEL_IN_CALL >=DATE_SUB('###SLICE_VALUE###',90)  THEN 1 ELSE 0 END VIETTEL_IN_CALL
            , DATE_VIETTEL_IN_CALL
            ---For the sms
            ,CASE WHEN DATE_VIETTEL_OUT_SMS >=DATE_SUB('###SLICE_VALUE###',90)   THEN 1 ELSE 0 END VIETTEL_OUT_SMS
            , DATE_VIETTEL_OUT_SMS
            --
            ,CASE WHEN DATE_VIETTEL_IN_SMS >=DATE_SUB('###SLICE_VALUE###',90)  THEN 1 ELSE 0 END VIETTEL_IN_SMS
            , DATE_VIETTEL_IN_SMS
            ---
            ,CASE WHEN DATE_CAMTEL_OUT_CALL >=DATE_SUB('###SLICE_VALUE###',90)   THEN 1 ELSE 0 END CAMTEL_OUT_CALL
            , DATE_CAMTEL_OUT_CALL
            ,CASE WHEN DATE_CAMTEL_IN_CALL >=DATE_SUB('###SLICE_VALUE###',90)  THEN 1 ELSE 0 END CAMTEL_IN_CALL
            , DATE_CAMTEL_IN_CALL
            ---For the sms
            ,CASE WHEN DATE_CAMTEL_OUT_SMS >=DATE_SUB('###SLICE_VALUE###',90)   THEN 1 ELSE 0 END CAMTEL_OUT_SMS
            , DATE_CAMTEL_OUT_SMS
            ,CASE WHEN DATE_CAMTEL_IN_SMS >=DATE_SUB('###SLICE_VALUE###',90)  THEN 1 ELSE 0 END CAMTEL_IN_SMS
            , DATE_CAMTEL_IN_SMS
            ---
            ,CASE WHEN DATE_INTER_OUT_CALL >=DATE_SUB('###SLICE_VALUE###',90)   THEN 1 ELSE 0 END INTER_OUT_CALL
            , DATE_INTER_OUT_CALL
            ,CASE WHEN DATE_INTER_IN_CALL >=DATE_SUB('###SLICE_VALUE###',90)  THEN 1 ELSE 0 END INTER_IN_CALL
            , DATE_INTER_IN_CALL
            ---For the sms
            ,CASE WHEN DATE_INTER_OUT_SMS >=DATE_SUB('###SLICE_VALUE###',90)   THEN 1 ELSE 0 END INTER_OUT_SMS
            , DATE_INTER_OUT_SMS
            ,CASE WHEN DATE_INTER_IN_SMS >=DATE_SUB('###SLICE_VALUE###',90)  THEN 1 ELSE 0 END INTER_IN_SMS
            , DATE_INTER_IN_SMS
            ---
            ,CASE WHEN DATE_ROAM_OUT_CALL >=DATE_SUB('###SLICE_VALUE###',90)   THEN 1 ELSE 0 END ROAM_OUT_CALL
            , DATE_ROAM_OUT_CALL
            ,CASE WHEN DATE_ROAM_IN_CALL >=DATE_SUB('###SLICE_VALUE###',90)  THEN 1 ELSE 0 END ROAM_IN_CALL
            , DATE_ROAM_IN_CALL
            ---For the sms
            ,CASE WHEN DATE_ROAM_OUT_SMS >=DATE_SUB('###SLICE_VALUE###',90)   THEN 1 ELSE 0 END ROAM_OUT_SMS
            , DATE_ROAM_OUT_SMS  --
            ,CASE WHEN DATE_ROAM_IN_SMS >=DATE_SUB('###SLICE_VALUE###',90)  THEN 1 ELSE 0 END ROAM_IN_SMS
            , DATE_ROAM_IN_SMS
            --, CURRENT_TIMESTAMP INSERT_DATE
        FROM MON.FT_PDM_TRANSACTION_OTHER
        WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###',1)
    ) A GROUP BY MSISDN--, EVENT_DATE
;

