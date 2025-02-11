INSERT INTO AGG.FT_A_EMERGENCY_DATA PARTITION (EVENT_DATE)
SELECT     
    MAX( tt.NBRE_SOUSCRIPTION_UNIQUE) NBRE_SOUSCRIPTION_UNIQUE,
    SUM(CASE TRANSACTION_TYPE WHEN 'LOAN' THEN AMOUNT ELSE 0 END ) MONTANT_LOAN,
    SUM(CASE TRANSACTION_TYPE WHEN 'PAYBACK' THEN -AMOUNT ELSE 0 END ) MONTANT_PAYBACK,
    COUNT((CASE TRANSACTION_TYPE WHEN 'LOAN' THEN MSISDN ELSE NULL END)) NBRE_SOUSCRIPTION,
    SUM(NVL(BYTES_OBTAINED, 0)) BYTES_OBTAINED,
    TRANSACTION_TYPE,
    substr(CONTACT_CHANNEL,1,3) CONTACT_CHANNEL,
    CASE WHEN AMOUNT = 100 THEN '100' WHEN AMOUNT = 200 THEN '200' WHEN AMOUNT = 500 THEN '500' ELSE 'AUTRES' END BUNDLE,
    OFFER_PROFILE_CODE,
    OPERATOR_CODE,
    CURRENT_TIMESTAMP INSERT_DATE,
    TRANSACTION_DATE EVENT_DATE
    FROM MON.FT_EMERGENCY_DATA ft,             
        (
            SELECT TRANSACTION_DATE as TRANSACT_DATE, COUNT(DISTINCT MSISDN) as NBRE_SOUSCRIPTION_UNIQUE
            FROM MON.FT_EMERGENCY_DATA 
            WHERE TRANSACTION_DATE ='###SLICE_VALUE###'
            AND TRANSACTION_TYPE = 'LOAN'
            GROUP BY TRANSACTION_DATE
        ) tt
    WHERE TRANSACTION_DATE ='###SLICE_VALUE###' and
    ft.TRANSACTION_DATE = tt.TRANSACT_DATE
    GROUP BY TRANSACTION_DATE,
             TRANSACTION_TYPE, 
             CONTACT_CHANNEL, 
             OFFER_PROFILE_CODE, 
             OPERATOR_CODE,
            CASE  WHEN AMOUNT = 100 THEN '100' 
                  WHEN AMOUNT = 200 THEN '200' 
                  WHEN AMOUNT = 500 THEN '500' 
                  ELSE 'AUTRES' 
            END; 