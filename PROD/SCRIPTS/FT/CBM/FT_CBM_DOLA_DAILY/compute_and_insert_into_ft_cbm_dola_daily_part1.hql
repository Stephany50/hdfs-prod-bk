INSERT INTO TMP.TT_FT_CBM_DOLA_DAILY_PRE (MSISDN, DOLA, ACTIVITY_TYPE)
    SELECT SERVED_PARTY MSISDN,(TRANSACTION_DATE) DOLA,SERVICE_CODE ACTIVITY_TYPE
    FROM MON.FT_BILLED_TRANSACTION_PREPAID 
    WHERE TRANSACTION_DATE ='###SLICE_VALUE###'
    AND (MAIN_RATED_AMOUNT + PROMO_RATED_AMOUNT) > 0
    GROUP BY SERVED_PARTY,(TRANSACTION_DATE),SERVICE_CODE 
   
UNION ALL
    --INSERTION DATA 
    SELECT SERVED_PARTY_MSISDN MSISDN,SESSION_DATE DOLA,'DATA' ACTIVITY_TYPE 
    FROM MON.SPARK_FT_CRA_GPRS
    WHERE SESSION_DATE = '###SLICE_VALUE###'
    AND (MAIN_COST + PROMO_COST) > 0
    GROUP BY SERVED_PARTY_MSISDN,SESSION_DATE

UNION ALL
    --INSERTION SOUSCRIPTION 
    SELECT SERVED_PARTY_MSISDN MSISDN, TRANSACTION_DATE DOLA ,'SOUSCRIPTION' ACTIVITY_TYPE
    FROM MON.SPARK_FT_SUBSCRIPTION
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    AND RATED_AMOUNT > 0
    GROUP BY SERVED_PARTY_MSISDN, TRANSACTION_DATE

UNION ALL  
    --INSERTION RECHARGE 
    SELECT SUBSTR(ACC_NBR,-9)MSISDN, TO_DATE(PAY_TIME) DOLA,'RECHARGE' ACTIVITY_TYPE FROM CDR.IT_ZTE_RECHARGE
    WHERE TO_DATE(PAY_TIME) = '###SLICE_VALUE###'
    AND (-BILL_AMOUNT) > 0
    GROUP BY SUBSTR(ACC_NBR,-9),TO_DATE(PAY_TIME);
