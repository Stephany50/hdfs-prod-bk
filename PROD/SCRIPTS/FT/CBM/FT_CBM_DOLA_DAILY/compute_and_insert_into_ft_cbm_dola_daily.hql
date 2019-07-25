INSERT INTO MON.FT_CBM_DOLA_DAILY PARTITION(EVENT_DATE)
SELECT 
    A.MSISDN,
    ACTIVATION_DATE,
    DOLA,
    GREATEST(NVL(DOLIA,'1970-01-01'),IC_CALL_4) DOLIA,
    ACTIVITY_TYPE,
    DATEDIFF(EVENT_DATE,DOLA) RGSX,
    REGION,
    TOWN,
    QUARTER,
    INSERT_DATE,
    EVENT_DATE
FROM 
    (
    SELECT
        NVL(S.EVENT_DATE, D.EVENT_DATE) EVENT_DATE,
        NVL(S.MSISDN, D.MSISDN) MSISDN,
        NVL(S.ACTIVATION_DATE, D.ACTIVATION_DATE) ACTIVATION_DATE,
        (CASE  WHEN S.DOLA IS NOT NULL  AND D.DOLA IS NOT NULL THEN D.DOLA
              ELSE NVL(S.DOLA, D.DOLA) 
              END) DOLA,
        S.DOLIA DOLIA, 
        NVL(S.ACTIVITY_TYPE, D.ACTIVITY_TYPE) ACTIVITY_TYPE,
        NVL(S.RGSX, D.RGSX) RGSX,
        (CASE  WHEN S.REGION IS NOT NULL  AND D.REGION IS NOT NULL THEN D.REGION
              ELSE NVL(S.REGION, D.REGION) 
              END) REGION,
        (CASE  WHEN S.TOWN IS NOT NULL  AND D.TOWN IS NOT NULL THEN D.TOWN
              ELSE NVL(S.TOWN, D.TOWN) 
              END) TOWN,
        (CASE  WHEN S.QUARTER IS NOT NULL  AND D.QUARTER IS NOT NULL THEN D.QUARTER
              ELSE NVL(S.QUARTER, D.QUARTER) 
              END) QUARTER,
        NVL(S.INSERT_DATE, CURRENT_TIMESTAMP)  INSERT_DATE
    FROM 
        (
            SELECT '###SLICE_VALUE###' EVENT_DATE,MSISDN,ACTIVATION_DATE,DOLA,DOLIA,ACTIVITY_TYPE,RGSX,REGION,TOWN,QUARTER,INSERT_DATE 
            FROM MON.FT_CBM_DOLA_DAILY  
            WHERE EVENT_DATE = date_sub('###SLICE_VALUE###', 1)
        ) S
        FULL OUTER JOIN
        (
            SELECT  '###SLICE_VALUE###' EVENT_DATE,A.MSISDN, B.ACTIVATION_DATE, A.DOLA, A.ACTIVITY_TYPE,'' RGSX,C.REGION,C.TOWN,C.QUARTER 
            FROM 
            (
                SELECT SERVED_PARTY MSISDN,(TRANSACTION_DATE) DOLA,SERVICE_CODE ACTIVITY_TYPE
                FROM MON.FT_BILLED_TRANSACTION_PREPAID 
                WHERE TRANSACTION_DATE ='###SLICE_VALUE###'
                AND (MAIN_RATED_AMOUNT + PROMO_RATED_AMOUNT) > 0
                GROUP BY SERVED_PARTY,(TRANSACTION_DATE),SERVICE_CODE 

                UNION ALL
                --INSERTION DATA 
                SELECT SERVED_PARTY_MSISDN MSISDN,SESSION_DATE DOLA,'DATA' ACTIVITY_TYPE 
                FROM MON.FT_CRA_GPRS 
                WHERE SESSION_DATE = '###SLICE_VALUE###'
                AND (MAIN_COST + PROMO_COST) > 0
                GROUP BY SERVED_PARTY_MSISDN,SESSION_DATE

                UNION ALL
                --INSERTION SOUSCRIPTION 
                SELECT SERVED_PARTY_MSISDN MSISDN, TRANSACTION_DATE DOLA ,'SOUSCRIPTION' ACTIVITY_TYPE
                FROM MON.FT_SUBSCRIPTION 
                WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
                AND RATED_AMOUNT > 0
                GROUP BY SERVED_PARTY_MSISDN, TRANSACTION_DATE

                UNION ALL  
                --INSERTION RECHARGE 
                SELECT SUBSTR(ACC_NBR,-9)MSISDN, TO_DATE(PAY_TIME) DOLA,'RECHARGE' ACTIVITY_TYPE FROM CDR.IT_ZTE_RECHARGE
                WHERE TO_DATE(PAY_TIME) = '###SLICE_VALUE###'
                AND (-BILL_AMOUNT) > 0
                GROUP BY SUBSTR(ACC_NBR,-9),TO_DATE(PAY_TIME)
            ) A
            INNER JOIN    
            (
                SELECT ACCESS_KEY MSISDN,ACTIVATION_DATE 
                FROM MON.FT_CONTRACT_SNAPSHOT 
                WHERE EVENT_DATE = date_add('###SLICE_VALUE###', 1)
            ) B
            ON(A.MSISDN = B.MSISDN)
            LEFT JOIN    
            (
                SELECT MSISDN,COMMERCIAL_REGION REGION  ,TOWNNAME TOWN,SITE_NAME QUARTER 
                FROM MON.FT_CLIENT_LAST_SITE_DAY  WHERE EVENT_DATE ='###SLICE_VALUE###'
            ) C
            ON(A.MSISDN = C.MSISDN) 
        )D
        ON(S.MSISDN=D.MSISDN AND S.ACTIVITY_TYPE=D.ACTIVITY_TYPE)
    ) A
    LEFT JOIN 
    (
		SELECT MSISDN,IC_CALL_4 
		FROM MON.FT_OG_IC_CALL_SNAPSHOT
		WHERE EVENT_DATE = DATE_ADD('###SLICE_VALUE###',1)
		AND IC_CALL_4 IS NOT NULL
    ) B
    ON(A.MSISDN = B.MSISDN)