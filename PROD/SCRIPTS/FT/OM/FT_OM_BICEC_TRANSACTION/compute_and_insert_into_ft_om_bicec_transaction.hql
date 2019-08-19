INSERT INTO TTVMW_OM_BICEC_TRANS

SELECT *
FROM
(
--################################################################ TRANSACTIONS  ######################################
SELECT
 (CASE WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY ELSE '06815' END) AGE
,(CASE WHEN MONEY_CODE IS NOT NULL THEN MONEY_CODE ELSE '001' END) DEV
,(CASE WHEN FINANCIAL_CHAPTER IS NOT NULL THEN FINANCIAL_CHAPTER ELSE '384999' END) CHA
,(CASE WHEN ACCOUNT_NUMBER IS NOT NULL THEN ACCOUNT_NUMBER
       WHEN ACCOUNT_NUMBER IS NULL AND USER_CATEGORY_CODE='SUBS' 
       THEN '38499999997' ELSE '38499999999' 
  END) NCP
,'  ' SUF
, CASE  WHEN OPERATIONTYPE_CODE IS NOT NULL  THEN OPERATIONTYPE_CODE
    ELSE  MON.FN_THROW_EXCEPTION ( 'error: SERVICE_TYPE '||A.SERVICE_TYPE||' absent de la table DIM.DT_OM_OPERATION_TYPE. Event date: ' || TO_CHAR (EVENT_DATE, 'yyyymmdd') || ',  ACCOUNT_NUMBER=' || ACCOUNT_NUMBER 
    ) END OPE
, 'AUTO' UTI     
,(CASE WHEN ACCOUNT_KEY IS NOT NULL THEN ACCOUNT_KEY
       WHEN ACCOUNT_KEY IS NULL AND USER_CATEGORY_CODE='SUBS' 
       THEN '55' ELSE '49' 
  END) CLC
, TO_DATE (EVENT_DATE) DCO
, TO_DATE (EVENT_DATE) DVA
, AMOUNT  MON
, SENS SEN
, SUBSTR (OM_OPERATION_NAME || '.' || ' ' ||'.amnt', 1, 30)   LIB
, 'OM' || LPAD (OM_OPERATION_CODE, 3, '0') || LPAD (N_ROWNUM, 6, '0')  PIE
,' ' MAR
, (CASE WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY ELSE '06815' END) AGSA
, (CASE WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY ELSE '06815' END) AGEM
, (CASE WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY ELSE '06815' END) AGDE
, (CASE WHEN MONEY_CODE IS NOT NULL THEN MONEY_CODE ELSE '001' END) DEVC
, AMOUNT MCTV
, 'OM' || LPAD (OM_OPERATION_CODE, 3, '0') || LPAD (N_ROWNUM, 6, '0') PIEO
, EVENT_DATE
, CURRENT_TIMESTAMP INSERT_DATE
FROM
( SELECT N_ROWNUM,EVENT_DATE,A.SERVICE_TYPE,USER_ID,USER_CATEGORY_CODE,AMOUNT,SENS,OM_OPERATION_NAME,OM_OPERATION_DESC,OPERATIONTYPE_CODE,OM_OPERATION_CODE
    FROM
(
-- OPÉRATION DE DEBIT
(
    SELECT
    COUNT(*) N_ROWNUM
    ,TRUNC(TRANSFER_DATETIME) EVENT_DATE
    ,SERVICE_TYPE
    ,SENDER_USER_ID USER_ID
    ,SENDER_CATEGORY_CODE USER_CATEGORY_CODE
    ,TRANSACTION_AMOUNT AMOUNT
    ,'D' SENS
    FROM CDR.IT_OM_TRANSACTIONS
    WHERE (TRANSFER_STATUS='TS' OR (TRANSFER_STATUS='TF'AND  RECONCILIATION_BY IS NOT NULL) OR (TRANSFER_STATUS='TF'AND SENDER_PRE_BAL<>SENDER_POST_BAL))
    AND TRANSFER_DATETIME >=X
    AND TRANSFER_DATETIME < X+1
)
UNION

-- OPÉRATION DE CREDIT
(
SELECT
 COUNT(*)  N_ROWNUM
,TO_DATE(TRANSFER_DATETIME) EVENT_DATE
,SERVICE_TYPE
,RECEIVER_USER_ID USER_ID
,RECEIVER_CATEGORY_CODE USER_CATEGORY_CODE
,TRANSACTION_AMOUNT AMOUNT
,'C' SENS
FROM CDR.IT_OM_TRANSACTIONS
WHERE (TRANSFER_STATUS='TS' OR (TRANSFER_STATUS='TF'AND  RECONCILIATION_BY IS NOT NULL) OR (TRANSFER_STATUS='TF'AND SENDER_PRE_BAL<>SENDER_POST_BAL))
AND TRANSFER_DATETIME >=X
AND TRANSFER_DATETIME < X+1
)
) A 
LEFT JOIN DIM.DT_OM_OPERATION_TYPE C
ON(A.SERVICE_TYPE = C.SERVICE_TYPE)
LEFT JOIN  DIM.DT_OM_BANK_PARTNER_ACCOUNT B
ON(A.USER_ID=B.ACCOUNT_ID)

--################################################################ COMMISSIONS  ######################################

UNION ALL

SELECT
 (CASE WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY ELSE '06815' END) AGE
,(CASE WHEN MONEY_CODE IS NOT NULL THEN MONEY_CODE ELSE '001' END) DEV
,(CASE WHEN FINANCIAL_CHAPTER IS NOT NULL THEN FINANCIAL_CHAPTER ELSE '384999' END) CHA
,(CASE WHEN ACCOUNT_NUMBER IS NOT NULL THEN ACCOUNT_NUMBER
       WHEN ACCOUNT_NUMBER IS NULL AND USER_CATEGORY_CODE='SUBS' 
       THEN '38499999997' ELSE '38499999999' 
END) NCP
,'  ' SUF
,CASE  WHEN OPERATIONTYPE_CODE IS NOT NULL  THEN OPERATIONTYPE_CODE
       ELSE  MON.FN_THROW_EXCEPTION ( 'error: SERVICE_TYPE '||A.SERVICE_TYPE||' absent de la table DIM.DT_OM_OPERATION_TYPE. Event date: ' || TO_CHAR (EVENT_DATE, 'yyyymmdd') || ',  ACCOUNT_NUMBER=' || ACCOUNT_NUMBER ) END OPE
, 'AUTO' UTI     
,(CASE WHEN ACCOUNT_KEY IS NOT NULL THEN ACCOUNT_KEY
       WHEN ACCOUNT_KEY IS NULL AND USER_CATEGORY_CODE='SUBS' 
       THEN '55' ELSE '49' END) CLC
, TO_CHAR (EVENT_DATE, 'dd/mm/yyyy') DCO
, TO_CHAR (EVENT_DATE, 'dd/mm/yyyy') DVA
, AMOUNT  MON
, SENS SEN
, SUBSTR (OM_OPERATION_NAME || '.' || ' ' ||'.cmms', 1, 30)   LIB
, 'OM' || LPAD (OM_OPERATION_CODE, 3, '0') || LPAD (N_ROWNUM, 6, '0')  PIE
,' ' MAR
, (CASE WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY ELSE '06815' END) AGSA
, (CASE WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY ELSE '06815' END) AGEM
, (CASE WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY ELSE '06815' END) AGDE
,(CASE WHEN MONEY_CODE IS NOT NULL THEN MONEY_CODE ELSE '001' END) DEVC
, AMOUNT MCTV
, 'OM' || LPAD (OM_OPERATION_CODE, 3, '0') || LPAD (N_ROWNUM, 6, '0') PIEO
, EVENT_DATE
, CURRENT_TIMESTAMP INSERT_DATE
FROM
(
-- OPÉRATION DE DEBIT
(
SELECT
ROWNUM N_ROWNUM
,TO_DATE(TRANSACTION_DATE) EVENT_DATE
,SERVICE_TYPE
,PAYER_USER_ID USER_ID
,PAYER_CATEGORY_CODE USER_CATEGORY_CODE
,COMMISSION_AMOUNT AMOUNT
,'D' SENS
FROM CDR.IT_OM_COMMISSION
WHERE COMMISSION_AMOUNT>0
AND TRANSACTION_DATE >=X
AND TRANSACTION_DATE < X+1
)

UNION

-- OPÉRATION DE CREDIT
(
SELECT
  ROWNUM N_ROWNUM
,TO_DATE(TRANSACTION_DATE) EVENT_DATE
,SERVICE_TYPE
,PAYEE_USER_ID USER_ID
,PAYEE_CATEGORY_CODE USER_CATEGORY_CODE
,COMMISSION_AMOUNT AMOUNT
,'C' SENS
FROM CDR.IT_OM_COMMISSION
WHERE COMMISSION_AMOUNT>0
AND TRANSACTION_DATE >=X
AND TRANSACTION_DATE < X+1
)

) A,

DIM.DT_OM_BANK_PARTNER_ACCOUNT B,

DIM.DT_OM_OPERATION_TYPE C

WHERE A.USER_ID=B.ACCOUNT_ID(+)

AND A.SERVICE_TYPE = C.SERVICE_TYPE(+)

--################################################################ CHARGES DE SERVICE  ######################################

UNION ALL

SELECT

(CASE WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY ELSE '06815' END) AGE

,(CASE WHEN MONEY_CODE IS NOT NULL THEN MONEY_CODE ELSE '001' END) DEV

,(CASE WHEN FINANCIAL_CHAPTER IS NOT NULL THEN FINANCIAL_CHAPTER ELSE '384999' END) CHA

,(CASE WHEN ACCOUNT_NUMBER IS NOT NULL THEN ACCOUNT_NUMBER

WHEN ACCOUNT_NUMBER IS NULL AND USER_CATEGORY_CODE='SUBS' THEN '38499999997' ELSE '38499999999' END) NCP

,'  ' SUF

, CASE  WHEN OPERATIONTYPE_CODE IS NOT NULL  THEN OPERATIONTYPE_CODE

ELSE  MON.FN_THROW_EXCEPTION ( 'error: SERVICE_TYPE '||A.SERVICE_TYPE||' absent de la table DIM.DT_OM_OPERATION_TYPE. Event date: ' || TO_CHAR (EVENT_DATE, 'yyyymmdd') || ',  ACCOUNT_NUMBER=' || ACCOUNT_NUMBER ) END OPE

, 'AUTO' UTI     

,(CASE WHEN ACCOUNT_KEY IS NOT NULL THEN ACCOUNT_KEY

WHEN ACCOUNT_KEY IS NULL AND USER_CATEGORY_CODE='SUBS' THEN '55' ELSE '49' END) CLC

, TO_CHAR (EVENT_DATE, 'dd/mm/yyyy') DCO

, TO_CHAR (EVENT_DATE, 'dd/mm/yyyy') DVA

, AMOUNT  MON

, SENS SEN

, SUBSTR (OM_OPERATION_NAME || '.' || ' ' ||'.chrg', 1, 30)   LIB

, 'OM' || LPAD (OM_OPERATION_CODE, 3, '0') || LPAD (N_ROWNUM, 6, '0')  PIE

,' ' MAR

, (CASE WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY ELSE '06815' END) AGSA

, (CASE WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY ELSE '06815' END) AGEM

, (CASE WHEN ACCOUNT_AGENCY IS NOT NULL THEN ACCOUNT_AGENCY ELSE '06815' END) AGDE

,(CASE WHEN MONEY_CODE IS NOT NULL THEN MONEY_CODE ELSE '001' END) DEVC

, AMOUNT MCTV

, 'OM' || LPAD (OM_OPERATION_CODE, 3, '0') || LPAD (N_ROWNUM, 6, '0') PIEO

, EVENT_DATE

, SYSDATE INSERT_DATE

FROM

(

-- OPÉRATION DE DEBIT

(

SELECT

ROWNUM N_ROWNUM

,TRUNC(TRANSACTION_DATE) EVENT_DATE

,SERVICE_TYPE

,PAYER_USER_ID USER_ID

,PAYER_CATEGORY USER_CATEGORY_CODE

,SERVICE_CHARGE_AMOUNT AMOUNT

,'D' SENS

FROM TANGO_CDR.IT_OMNY_SERVICE_CHARGE

WHERE SERVICE_CHARGE_AMOUNT>0

AND TRANSACTION_DATE >=X

AND TRANSACTION_DATE < X+1

)

UNION

-- OPÉRATION DE CREDIT

(

SELECT

ROWNUM N_ROWNUM

,TRUNC(TRANSACTION_DATE) EVENT_DATE

,SERVICE_TYPE

,PAYEE_USER_ID USER_ID

,PAYEE_CATEGORY USER_CATEGORY_CODE

,SERVICE_CHARGE_AMOUNT AMOUNT

,'C' SENS

FROM TANGO_CDR.IT_OMNY_SERVICE_CHARGE

WHERE SERVICE_CHARGE_AMOUNT>0

AND TRANSACTION_DATE >=X

AND TRANSACTION_DATE < X+1

)

) A,

DIM.DT_OM_BANK_PARTNER_ACCOUNT B,

DIM.DT_OM_OPERATION_TYPE C

WHERE A.USER_ID=B.ACCOUNT_ID(+)

AND A.SERVICE_TYPE = C.SERVICE_TYPE(+)

);   

COMMIT;

--  VALIDATION FINALE DES DONNÉES AVANT INSERTION DEFINITIVE : AFIN DE PREVENIR UN CORRUPTION DE CHIFFRE DÛ AU JOINTURE

SELECT
(CASE WHEN ABS( 
        ( 
        SELECT
        ROUND(SUM (AMOUNT) ) N_TOTAL
        FROM
          (
         ( SELECT

            SUM(NVL (TRANSACTION_AMOUNT, 0)) AMOUNT
            FROM CDR.IT_OM_TRANSACTIONS
            WHERE (TRANSFER_STATUS='TS' OR (TRANSFER_STATUS='TF'AND  RECONCILIATION_BY IS NOT NULL) OR (TRANSFER_STATUS='TF'AND SENDER_PRE_BAL<>SENDER_POST_BAL))
            AND TO_DATE(TRANSFER_DATETIME)  >='2019-06-20'
            AND TO_DATE(TRANSFER_DATETIME)  < DATE_ADD('2019-06-20',1)
           )

           UNION

           (
            SELECT
            SUM(NVL (COMMISSION_AMOUNT, 0)*3) AMOUNT
            FROM CDR.IT_OM_COMMISSIONS
            WHERE COMMISSION_AMOUNT>0
            AND TO_DATE(TRANSACTION_DATE)  >='2019-06-20'
            AND TO_DATE(TRANSACTION_DATE)  < DATE_ADD('2019-06-20',1)
           )

           UNION
           (
            SELECT
            SUM(NVL (SERVICE_CHARGE_AMOUNT, 0)*4) AMOUNT
            FROM TANGO_CDR.IT_OM_SERVICES_CHARGES
            WHERE SERVICE_CHARGE_AMOUNT>0
            AND TO_DATE(TRANSACTION_DATE)  >='2019-06-20'
            AND TO_DATE(TRANSACTION_DATE)  < DATE_ADD('2019-06-20',1)
           )
           )

       )*2 - (

    SELECT

          ROUND(SUM (
                        (CASE WHEN LIB LIKE '%cmms%' THEN  NVL (MON, 0) ELSE 0 END  )*3 -- CMMS 

                        + (CASE WHEN LIB LIKE '%chrg%' THEN  NVL (MON, 0) ELSE 0 END  )*4  -- CHRG

                        + (CASE WHEN LIB LIKE '%amnt%' THEN  NVL (MON, 0) ELSE 0 END  ) --  MON

                    )) N_TOTAL
           FROM TTVMW_OM_BICEC_TRANS
        WHERE EVENT_DATE ='2019-06-20'
        ))<100
 THEN 'OK'
 ELSE  ( ' ###ERROR### : La vue <TTVMW_OM_BICEC_TRANS> est incoherente. Possible Pb de jointure '  )
 END
)


INSERT INTO MON.FT_OM_BICEC_TRANSACTION
SELECT 
  AGE, DEV, CHA, NCP, SUF, OPE
, UTI, CLC, DCO, DVA, MON, SEN, LIB
, 'OM' || LPAD (OPE, 3, '0') || LPAD (ROWNUM, 6, '0') PIE
, MAR, AGSA, AGEM, AGDE, DEVC, MCTV
, 'OM' || LPAD (OPE, 3, '0') || LPAD (ROWNUM, 6, '0') PIEO
, EVENT_DATE, INSERT_DATE, USER_ID, NULL
FROM (  
    SELECT 
      AGE, DEV, CHA, NCP, SUF
    , OPE, UTI, CLC, DCO
    , DVA, SUM(MON) MON, SEN, LIB
    , MAR, AGSA, AGEM, AGDE
    , DEVC, SUM(MCTV) MCTV
    , EVENT_DATE, CURRENT_TIMESTAMP INSERT_DATE,NULL USER_ID
FROM TTVMW_OM_BICEC_TRANS
WHERE EVENT_DATE =X
GROUP BY 
  AGE, DEV, CHA, NCP, SUF, OPE
, UTI, CLC, DCO, DVA, SEN, LIB
, MAR, AGSA, AGEM, AGDE, DEVC
, EVENT_DATE, NULL

)    ;