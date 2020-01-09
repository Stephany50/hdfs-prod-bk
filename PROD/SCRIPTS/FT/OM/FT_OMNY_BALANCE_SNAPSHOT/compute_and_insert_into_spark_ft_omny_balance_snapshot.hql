 INSERT INTO MON.SPARK_FT_OMNY_BALANCE_SNAPSHOT
SELECT

       USER_CATEGORY,
       USER_ID,
       MSISDN,
       BALANCE,
       current_timestamp INSERT_DATE,
       PARTNER_FIRST_NAME,
       PARTNER_LAST_NAME,
       ACCOUNT_NUMBER,
       "###SLICE_VALUE###" EVENT_DATE
FROM(
       SELECT
              (CASE
                  WHEN ACCOUNT_ID IS NOT NULL THEN USER_CATEGORY
                  WHEN ACCOUNT_ID IS NULL AND UPPER(USER_CATEGORY) LIKE '%SUBS%' THEN 'SUBS'
                  ELSE 'SDIST'
                END )  USER_CATEGORY,
              (CASE
                  WHEN ACCOUNT_ID IS NOT NULL THEN ACCOUNT_ID
                  WHEN ACCOUNT_ID IS NULL AND UPPER(USER_CATEGORY) LIKE '%SUBS%' THEN 'SUBS'
                  ELSE 'SDIST'
                END ) USER_ID,
              (CASE
                  WHEN ACCOUNT_MSISDN IS NOT NULL THEN
                 -- DECODE(ACCOUNT_MSISDN, '2a6e7a3a2fa013beb937b2afe628607f' , 'IND04', ACCOUNT_MSISDN)
                  (CASE
                    WHEN ACCOUNT_MSISDN = '2a6e7a3a2fa013beb937b2afe628607f'  THEN 'IND04'
                    ELSE ACCOUNT_MSISDN
                   END)
                  WHEN ACCOUNT_MSISDN IS NULL AND UPPER(USER_CATEGORY) LIKE '%SUBS%' THEN 'SUBS'
                  ELSE 'SDIST'
                END)MSISDN,
              SUM(ACCOUNT_BALANCE) BALANCE,
              ( CASE
                  WHEN PARTNER_FIRST_NAME IS NOT NULL THEN PARTNER_FIRST_NAME
                  WHEN PARTNER_FIRST_NAME IS NULL AND UPPER(USER_CATEGORY) LIKE '%SUBS%' THEN 'ORANGE MONEY UTILISATEUR FINAL'
                  ELSE 'ORANGE MONEY PETITS DETAILLANTS'
                END )  PARTNER_FIRST_NAME,
               PARTNER_LAST_NAME,
              ( CASE
                  WHEN ACCOUNT_NUMBER IS NOT NULL THEN ACCOUNT_NUMBER
                  WHEN ACCOUNT_NUMBER IS NULL AND UPPER(USER_CATEGORY) LIKE '%SUBS%' THEN '38859999997'
                  ELSE '38499999999'
                END )  ACCOUNT_NUMBER
       FROM
         (    SELECT
                  USER_CATEGORY,
                  --DECODE(USER_ID, 'BK130405.1017.026385', 'BANK', USER_ID) USER_ID
                  (CASE
                   WHEN USER_ID = 'BK130405.1017.026385'  THEN 'BANK'
                    ELSE USER_ID
                   END)USER_ID,
                  SUM(ACCOUNT_BALANCE) ACCOUNT_BALANCE
               FROM   MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT
               WHERE  EVENT_DATE = "###SLICE_VALUE###"
               GROUP BY 
                  USER_CATEGORY,
                  (CASE
                    WHEN USER_ID = 'BK130405.1017.026385'  THEN 'BANK'
                    ELSE USER_ID
                  END)         
          )A
               LEFT JOIN DIM.DT_OM_BANK_PARTNER_ACCOUNT B
               ON  A.USER_ID = B.ACCOUNT_ID
       GROUP  BY 
                  ( CASE
                     WHEN ACCOUNT_ID IS NOT NULL THEN USER_CATEGORY
                     WHEN ACCOUNT_ID IS NULL AND UPPER(USER_CATEGORY) LIKE '%SUBS%' THEN 'SUBS'
                     ELSE 'SDIST'
                   END ),
                 ( CASE
                     WHEN ACCOUNT_ID IS NOT NULL THEN ACCOUNT_ID
                     WHEN ACCOUNT_ID IS NULL AND UPPER(USER_CATEGORY) LIKE '%SUBS%' THEN 'SUBS'
                     ELSE 'SDIST'
                   END ),
                 ( CASE
                     WHEN ACCOUNT_MSISDN IS NOT NULL THEN 
                     (CASE
                        WHEN ACCOUNT_MSISDN = '2a6e7a3a2fa013beb937b2afe628607f'  THEN 'IND04'
                        ELSE ACCOUNT_MSISDN
                      END) 
                     WHEN ACCOUNT_MSISDN IS NULL AND UPPER(USER_CATEGORY) LIKE '%SUBS%' THEN 'SUBS'
                     ELSE 'SDIST'
                   END ),
                 ( CASE
                     WHEN PARTNER_FIRST_NAME IS NOT NULL THEN PARTNER_FIRST_NAME
                     WHEN PARTNER_FIRST_NAME IS NULL AND UPPER(USER_CATEGORY) LIKE '%SUBS%' THEN 'ORANGE MONEY UTILISATEUR FINAL'
                     ELSE 'ORANGE MONEY PETITS DETAILLANTS'
                   END ),
                  PARTNER_LAST_NAME,
                 ( CASE
                     WHEN ACCOUNT_NUMBER IS NOT NULL THEN ACCOUNT_NUMBER
                     WHEN ACCOUNT_NUMBER IS NULL AND UPPER(USER_CATEGORY) LIKE '%SUBS%' THEN '38859999997'
                     ELSE '38499999999'
                   END )
        )M
