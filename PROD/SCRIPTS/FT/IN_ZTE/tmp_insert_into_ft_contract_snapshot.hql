CREATE OR REPLACE FUNCTION MON.FN_DO_CONTRACT_SNAPSHOT(process_date IN VARCHAR2) RETURN VARCHAR2 IS
 --Section déclaration
 v_current_date varchar2(50):=process_date;
 v_snapshot_return_code VARCHAR2(50):='OK';
 v_zte_return_code VARCHAR2(50):='OK';
 v_bscs_contract_return_code VARCHAR2(50):='OK';
 v_bscs_customer_return_code VARCHAR2(50):='OK';
 --
 v_olddwh_snapshot_return_code VARCHAR2(50):='OK';
 
 --Section Exception;
 e_cont_snap_already_done EXCEPTION;
 e_bscs_src_unavailable EXCEPTION;
 e_zte_snapshot_unvailable EXCEPTION;
 e_zte_snapshot_inconsistant EXCEPTION;
 
 --
  
BEGIN     
  
SELECT DECODE(COUNT(1),0,'KO','OK') INTO v_snapshot_return_code  FROM FT_CONTRACT_SNAPSHOT 
             WHERE EVENT_DATE=TO_DATE(v_current_date,'yyyymmdd')
                     AND ROWNUM <= 1
                  ; 
                  
SELECT ( CASE 
            WHEN  TOTAL_COUNT =0 THEN  'NOTDONE'
            WHEN  TOTAL_COUNT  BETWEEN 8000000 AND 14000000  THEN 'OK'--9000000  THEN 'OK'
            ELSE 'INCONSISTANT'
                  END ) INTO v_zte_return_code 
FROM(SELECT COUNT(*) TOTAL_COUNT FROM FT_ZTE_SNAPSHOT 
                  WHERE EVENT_DATE=TO_DATE(v_current_date,'yyyymmdd') AND PROD_PROD_STATE<>'TERMINATED'
     )               
;
              
SELECT DECODE(COUNT(1),0,'KO','OK') INTO v_bscs_contract_return_code  FROM FT_BSCS_CONTRACT 
              WHERE EVENT_DATE=TO_DATE(v_current_date,'yyyymmdd')
                 AND ROWNUM <= 1
              ;     

SELECT DECODE(COUNT(1),0,'KO','OK') INTO v_bscs_customer_return_code  FROM FT_BSCS_CUSTOMER 
              WHERE EVENT_DATE=TO_DATE(v_current_date,'yyyymmdd')
                 AND ROWNUM <= 1
              ;               

/*                       
SELECT DECODE(COUNT(1),0,'KO','OK') INTO v_olddwh_snapshot_return_code  FROM FT_CONTRACT_SNAPSHOT@db_link_to_old_dwh_mon 
             WHERE EVENT_DATE=TO_DATE(v_current_date,'yyyymmdd')
                     AND ROWNUM <= 1
                  ; 
*/
--Forcer la journée du 03/06/2019(04/06/2019) pour cause d'extract Data_Extract_Acct incoherent ou non conforme ne suivant pas la tendance
   IF v_current_date in ('20190604', '20190724', '20190725','20190726','20190730','20190731'
                         ,'20190802','20190803','20190804','20190805','20190807')then 
     v_zte_return_code :='OK';
   END IF;

         IF  v_snapshot_return_code='KO'  THEN
                      IF   v_zte_return_code ='OK'   THEN 
                               IF  ( v_bscs_contract_return_code='OK' 
                                       AND v_bscs_customer_return_code='OK' 
                                      )       THEN 
                
--Preparation des données pour FT_CONTRACT_SNAPSHOT
     -- Ajouter les données communes ZTE vs BSCS ) + ( données ZTE seulement
     
EXECUTE IMMEDIATE 'TRUNCATE TABLE MON.TT_FT_CONTRACT_SNAPSHOT' ;   
COMMIT;

INSERT /*+ APPEND */ INTO MON.TT_FT_CONTRACT_SNAPSHOT
        SELECT 
        TO_DATE(v_current_date,'yyyymmdd') EVENT_DATE
        ,BSCS_CONTRACT.CONTRACT_ID CONTRACT_ID --,ZTE.SUBS_CUST_ID CONTRACT_ID
        ,BSCS_CONTRACT.CUSTOMER_ID CUSTOMER_ID --,ZTE.CUST_ID CUSTOMER_ID
        ,ZTE.ACC_NBR ACCESS_KEY
        ,ZTE.ACCT_ID ACCOUNT_ID
        ,TRUNC(ZTE.COMPLETED_DATE) ACTIVATION_DATE
        ,TRUNC(ZTE.DEACTIVATION_DATE) DEACTIVATION_DATE
        ,NULL INACTIVITY_BEGIN_DATE
        ,NULL BLOCKED
        ,NULL EXHAUSTED
        ,NULL PERIODIC_FEE
        ,NULL SCRATCH_RELOAD_SUSP
        ,TRUNC(ZTE.UPDATE_DATE) COMMERCIAL_OFFER_ASSIGN_DATE
        ,NVL(ZTE.PRICE_PLAN_NAME,UPPER(SUBSTR(BSCS_CONTRACT.SCOMMERCIAL_OFFER, INSTR(BSCS_CONTRACT.SCOMMERCIAL_OFFER,'|',1,1)+1))) COMMERCIAL_OFFER
        ,BSCS_CONTRACT.CURRENT_STATUS CURRENT_STATUS
        ,BSCS_CONTRACT.STATUS_DATE STATUS_DATE
        ,NULL LOGIN
        ,ZTE.DEF_LANG_ID LANG
        ,NULL LOCATION
        ,ZTE.IMSI MAIN_IMSI
        ,NULL MSID_TYPE
        ,NVL(ZTE.PRICE_PLAN_NAME,UPPER(SUBSTR(BSCS_CONTRACT.SCOMMERCIAL_OFFER, INSTR(BSCS_CONTRACT.SCOMMERCIAL_OFFER,'|',1,1)+1))) PROFILE
        ,NULL BAD_RELOAD_ATTEMPTS
        ,TRUNC(ZTE.ACCT_UPDATE_DATE) LAST_TOPUP_DATE
        ,TRUNC(ZTE.ACCT_UPDATE_DATE) LAST_CREDIT_UPDATE_DATE  --ZTE.MAIN_BALANCE_EFF_DATE
        ,NULL BAD_PIN_ATTEMPTS
        ,NULL BAD_PWD_ATTEMPTS
        ,(CASE WHEN UPPER(PRICE_PLAN_NAME) LIKE 'POSTPAID%' THEN 'POSTPAID'
               WHEN UPPER(PRICE_PLAN_NAME) LIKE 'PREPAID%' THEN 'PREPAID'
          ELSE 'PREPAID' END
         ) OSP_ACCOUNT_TYPE
        ,NULL INACTIVITY_CREDIT_LOSS
        ,BSCS_CUSTOMER.CUST_DEALER_ID DEALER_ID
        ,TRUNC(ZTE.ACCT_CREATED_DATE) PROVISIONING_DATE
        --,ZTE.MAIN_BALANCE MAIN_CREDIT  -- credit en compte non dispo dans les extract ZTE
        --,ZTE.PROMO_BALANCE PROMO_CREDIT
        ,(ZTE.GROSS_MAIN_BALANCE + ZTE.CONSUME_MAIN_BALANCE + ZTE.RESERVE_MAIN_BALANCE) MAIN_CREDIT
        ,(ZTE.GROSS_PROMO_BALANCE + ZTE.CONSUME_PROMO_BALANCE + ZTE.RESERVE_PROMO_BALANCE) PROMO_CREDIT
        ,NULL SMS_CREDIT
        ,NULL DATA_CREDIT
        ,NULL USED_CREDIT_MONTH
        ,NULL USED_CREDIT_LIFE
        ,BSCS_CONTRACT.BUNDLE_LIST BUNDLE_LIST
        ,BSCS_CONTRACT.BUNDLE_UNIT_LIST BUNDLE_UNIT_LIST
        ,BSCS_CONTRACT.PROMO_AND_DISCOUNT_LIST PROMO_AND_DISCOUNT_LIST
        ,SYSDATE INSERT_DATE
        ,(CASE WHEN UPPER(PRICE_PLAN_NAME) LIKE 'POSTPAID%' THEN 'FT_BSCS_CONTRACT'
               WHEN UPPER(PRICE_PLAN_NAME) LIKE 'PREPAID%' THEN 'IT_ICC_ACCOUNT'
          ELSE 'IT_ICC_ACCOUNT' END
         )  SRC_TABLE
        ,(CASE WHEN (ZTE.PROD_PROD_STATE='DEACTIVATED' AND ZTE.BLOCK_REASON='20000000000000') THEN 'INACTIVE' ELSE ZTE.PROD_PROD_STATE END ) OSP_STATUS
        ,BSCS_CONTRACT.COMMERCIAL_OFFER BSCS_COMM_OFFER_ID
        ,BSCS_CONTRACT.SCOMMERCIAL_OFFER BSCS_COMM_OFFER
        ,NULL INITIAL_SELECTION_DONE
        ,NULL NOMORE_CREDIT
        ,NULL PWD_BLOCKED
        ,NULL FIRST_EVENT_DONE
        ,BSCS_CUSTOMER.CUST_EXT_ID CUST_EXT_ID
        ,BSCS_CUSTOMER.CUST_GROUP CUST_GROUP
        ,BSCS_CUSTOMER.CUST_CATEGORY CUST_CATEGORY
        ,BSCS_CUSTOMER.CUST_BILLCYCLE CUST_BILLCYCLE
        ,BSCS_CUSTOMER.CUST_SEGMENT CUST_SEGMENT
        ,NULL OSP_CONTRACT_TYPE
        ,NULL OSP_CUST_COMMERCIAL_OFFER
        ,NULL OSP_CUSTOMER_CGLIST
        ,NULL OSP_CUSTOMER_FORMULE
        ,BSCS_CONTRACT.ACTIVATION_DATE BSCS_ACTIVATION_DATE
        ,BSCS_CONTRACT.DEACTIVATION_DATE BSCS_DEACTIVATION_DATE
        ,ZTE.OPERATOR_CODE OPERATOR_CODE
        ,ZTE.BALANCE_LIST
        ,PREVIOUS_STATUS
        ,ZTE.CURRENT_STATUS CURRENT_STATUS_1
        ,PROD_PROD_STATE_DATE STATE_DATETIME
        FROM 
          ( SELECT UNIQUE
            FIRST_VALUE(EVENT_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) EVENT_DATE
            , ACC_NBR
            ,FIRST_VALUE(ACC_NBR_STATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) ACC_NBR_STATE
            ,FIRST_VALUE(STATE_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) STATE_DATE
            ,FIRST_VALUE(ACC_NBR_TYPE_ID) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) ACC_NBR_TYPE_ID
            ,FIRST_VALUE(IMSI) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) IMSI
            ,FIRST_VALUE(UPDATE_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) UPDATE_DATE
            ,FIRST_VALUE(SUBS_CUST_ID) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) SUBS_CUST_ID
            ,FIRST_VALUE(ACCT_ID) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) ACCT_ID
            ,FIRST_VALUE(PRICE_PLAN_ID) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PRICE_PLAN_ID
            ,FIRST_VALUE(DEF_LANG_ID) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) DEF_LANG_ID
            ,FIRST_VALUE(CUST_ID) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CUST_ID
            ,FIRST_VALUE(CUST_NAME) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CUST_NAME
            ,FIRST_VALUE(ADDRESS) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) ADDRESS
            ,FIRST_VALUE(CUST_TYPE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CUST_TYPE
            ,FIRST_VALUE(PARENT_ID) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PARENT_ID
            ,FIRST_VALUE(CREATED_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CREATED_DATE
            ,FIRST_VALUE(CUST_UPDATE_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CUST_UPDATE_DATE
            ,FIRST_VALUE(CUST_STATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CUST_STATE
            ,FIRST_VALUE(CUST_STATE_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CUST_STATE_DATE
            ,FIRST_VALUE(CUST_CODE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CUST_CODE
            ,FIRST_VALUE(PROD_CREATED_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROD_CREATED_DATE
            ,FIRST_VALUE(BLOCK_REASON) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) BLOCK_REASON
            ,FIRST_VALUE(COMPLETED_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) COMPLETED_DATE
            ,FIRST_VALUE(PROD_PROD_STATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROD_PROD_STATE
            ,FIRST_VALUE(PROD_PROD_STATE_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROD_PROD_STATE_DATE
            ,FIRST_VALUE(PROD_UPDATE_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROD_UPDATE_DATE
            ,FIRST_VALUE(PROD_STATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROD_STATE
            ,FIRST_VALUE(PROD_STATE_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROD_STATE_DATE
            ,FIRST_VALUE(PRICE_PLAN_NAME) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PRICE_PLAN_NAME
            ,FIRST_VALUE(MAIN_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) MAIN_BALANCE
            ,FIRST_VALUE(PROMO_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROMO_BALANCE
            ,FIRST_VALUE(MAIN_BALANCE_EFF_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) MAIN_BALANCE_EFF_DATE
            ,FIRST_VALUE(MAIN_BALANCE_EXP_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) MAIN_BALANCE_EXP_DATE
            ,FIRST_VALUE(PROMO_BALANCE_EFF_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROMO_BALANCE_EFF_DATE
            ,FIRST_VALUE(PROMO_BALANCE_EXP_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROMO_BALANCE_EXP_DATE
            ,FIRST_VALUE(ACCT_UPDATE_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) ACCT_UPDATE_DATE
            ,FIRST_VALUE(ACCT_CREATED_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) ACCT_CREATED_DATE
            ,FIRST_VALUE(INSERT_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) INSERT_DATE
            ,FIRST_VALUE(DEACTIVATION_DATE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) DEACTIVATION_DATE
            ,FIRST_VALUE(GROSS_MAIN_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) GROSS_MAIN_BALANCE
            ,FIRST_VALUE(CONSUME_MAIN_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CONSUME_MAIN_BALANCE
            ,FIRST_VALUE(GROSS_PROMO_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) GROSS_PROMO_BALANCE
            ,FIRST_VALUE(CONSUME_PROMO_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CONSUME_PROMO_BALANCE
            ,FIRST_VALUE(OPERATOR_CODE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) OPERATOR_CODE
            ,FIRST_VALUE(BALANCE_LIST) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) BALANCE_LIST
            ,FIRST_VALUE(RESERVE_PROMO_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) RESERVE_PROMO_BALANCE
            ,FIRST_VALUE(RESERVE_MAIN_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) RESERVE_MAIN_BALANCE
            ,FIRST_VALUE(BUNDLE_MONEY_INTER) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) BUNDLE_MONEY_INTER
            ,FIRST_VALUE(BUNDLE_OFFNET_MONEY) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) BUNDLE_OFFNET_MONEY
            ,FIRST_VALUE(MAIN_BALANCE_REAL) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) MAIN_BALANCE_REAL
            ,FIRST_VALUE(PROMO_ONNET_SPECIAL) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROMO_ONNET_SPECIAL
            ,FIRST_VALUE(LOAN_BALANCE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) LOAN_BALANCE
            ,FIRST_VALUE(MAIN_SASSAYE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) MAIN_SASSAYE
            ,FIRST_VALUE(PROMO_SASSAYE) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROMO_SASSAYE
            ,FIRST_VALUE(PROMO) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROMO
            ,FIRST_VALUE(PROMO_ONNET) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROMO_ONNET
            ,FIRST_VALUE(SET_VOICEONNET) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) SET_VOICEONNET
            ,FIRST_VALUE(SET_PROMO) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) SET_PROMO
            ,FIRST_VALUE(PROD_STATUS) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PROD_STATUS
            ,FIRST_VALUE(CURRENT_STATUS) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) CURRENT_STATUS
            ,FIRST_VALUE(PREVIOUS_STATUS) OVER (PARTITION BY ACC_NBR ORDER BY PROD_CREATED_DATE DESC) PREVIOUS_STATUS
            FROM MON.FT_ZTE_SNAPSHOT 
            WHERE EVENT_DATE=TO_DATE (v_current_date, 'yyyymmdd')  AND PROD_PROD_STATE <> 'TERMINATED') ZTE
        , ( SELECT * FROM MON.FT_BSCS_CONTRACT d WHERE d.EVENT_DATE = TO_DATE (v_current_date, 'yyyymmdd') 
                                                    ) BSCS_CONTRACT
        , ( SELECT * FROM MON.FT_BSCS_CUSTOMER e WHERE  e.EVENT_DATE = TO_DATE (v_current_date, 'yyyymmdd') 
                                                    ) BSCS_CUSTOMER
        WHERE
                 ZTE.ACC_NBR = BSCS_CONTRACT.ACCESS_KEY (+)
                AND BSCS_CONTRACT.CUSTOMER_ID = BSCS_CUSTOMER.CUST_ID (+)               
                ;
COMMIT; 

commit;       


-- Ajouter les données BSCS non présent dans ZTE
/*
INSERT INTO MON.TT_FT_CONTRACT_SNAPSHOT
        SELECT 
        TO_DATE(v_current_date,'yyyymmdd') EVENT_DATE
        ,BSCS_CONTRACT.CONTRACT_ID CONTRACT_ID --,ZTE.SUBS_CUST_ID CONTRACT_ID
        ,BSCS_CONTRACT.CUSTOMER_ID CUSTOMER_ID --,ZTE.CUST_ID CUSTOMER_ID
        ,BSCS_CONTRACT.ACCESS_KEY ACCESS_KEY
        ,NULL ACCOUNT_ID
        ,BSCS_CONTRACT.ACTIVATION_DATE ACTIVATION_DATE
        ,BSCS_CONTRACT.DEACTIVATION_DATE  DEACTIVATION_DATE
        ,NULL INACTIVITY_BEGIN_DATE
        ,NULL BLOCKED
        ,NULL EXHAUSTED
        ,NULL PERIODIC_FEE
        ,NULL SCRATCH_RELOAD_SUSP
        ,NULL COMMERCIAL_OFFER_ASSIGN_DATE
        ,NULL COMMERCIAL_OFFER
        ,BSCS_CONTRACT.CURRENT_STATUS CURRENT_STATUS
        ,BSCS_CONTRACT.STATUS_DATE STATUS_DATE
        ,NULL LOGIN
        ,NVL (BSCS_CUSTOMER.CUST_LANGUAGE, 'FR') LANG
        ,NULL LOCATION
        ,BSCS_CONTRACT.MAIN_IMSI MAIN_IMSI
        ,NULL MSID_TYPE
        ,NULL PROFILE
        ,NULL BAD_RELOAD_ATTEMPTS
        ,NULL LAST_TOPUP_DATE
        ,NULL LAST_CREDIT_UPDATE_DATE
        ,NULL BAD_PIN_ATTEMPTS
        ,NULL BAD_PWD_ATTEMPTS
        ,'POSTPAID' OSP_ACCOUNT_TYPE
        ,NULL INACTIVITY_CREDIT_LOSS
        ,BSCS_CUSTOMER.CUST_DEALER_ID DEALER_ID
        ,BSCS_CONTRACT.ACTIVATION_DATE PROVISIONING_DATE
        ,NULL MAIN_CREDIT
        ,NULL PROMO_CREDIT
        ,NULL SMS_CREDIT
        ,NULL DATA_CREDIT
        ,NULL USED_CREDIT_MONTH
        ,NULL USED_CREDIT_LIFE
        ,BSCS_CONTRACT.BUNDLE_LIST BUNDLE_LIST
        ,BSCS_CONTRACT.BUNDLE_UNIT_LIST BUNDLE_UNIT_LIST
        ,BSCS_CONTRACT.PROMO_AND_DISCOUNT_LIST PROMO_AND_DISCOUNT_LIST
        ,SYSDATE INSERT_DATE
        ,'FT_BSCS_CONTRACT' SRC_TABLE
        ,NULL OSP_STATUS
        ,BSCS_CONTRACT.COMMERCIAL_OFFER BSCS_COMM_OFFER_ID
        ,BSCS_CONTRACT.SCOMMERCIAL_OFFER BSCS_COMM_OFFER
        ,NULL INITIAL_SELECTION_DONE
        ,NULL NOMORE_CREDIT
        ,NULL PWD_BLOCKED
        ,NULL FIRST_EVENT_DONE
        ,BSCS_CUSTOMER.CUST_EXT_ID CUST_EXT_ID
        ,BSCS_CUSTOMER.CUST_GROUP CUST_GROUP
        ,BSCS_CUSTOMER.CUST_CATEGORY CUST_CATEGORY
        ,BSCS_CUSTOMER.CUST_BILLCYCLE CUST_BILLCYCLE
        ,BSCS_CUSTOMER.CUST_SEGMENT CUST_SEGMENT
        ,NULL OSP_CONTRACT_TYPE
        ,NULL OSP_CUST_COMMERCIAL_OFFER
        ,NULL OSP_CUSTOMER_CGLIST
        ,NULL OSP_CUSTOMER_FORMULE
        ,BSCS_CONTRACT.ACTIVATION_DATE BSCS_ACTIVATION_DATE
        ,BSCS_CONTRACT.DEACTIVATION_DATE BSCS_DEACTIVATION_DATE
                FROM
                (    ( SELECT   DISTINCT a.ACCESS_KEY ACCESS_KEY  FROM  MON.FT_BSCS_CONTRACT a
                        WHERE a.EVENT_DATE  = TO_DATE (v_current_date, 'yyyymmdd') -- AND ROWNUM < 10
                        )
                    MINUS
                    ( SELECT   DISTINCT a.ACC_NBR ACCESS_KEY  FROM  MON.FT_ZTE_SNAPSHOT  a
                       WHERE a.EVENT_DATE=TO_DATE (v_current_date, 'yyyymmdd') AND PROD_PROD_STATE<>'TERMINATED'  AND ACC_NBR NOT IN ('90052208','90056677')-- AND ROWNUM < 10
                        ) )  BSCS_MINUS_ZTE
                , MON.FT_BSCS_CONTRACT BSCS_CONTRACT
                , ( SELECT   * FROM MON.FT_BSCS_CUSTOMER e WHERE  e.EVENT_DATE = TO_DATE (v_current_date, 'yyyymmdd') -- AND ROWNUM < 10
                    ) BSCS_CUSTOMER
                WHERE                                                           
                    BSCS_CONTRACT.EVENT_DATE = TO_DATE (v_current_date, 'yyyymmdd')
                    AND BSCS_MINUS_ZTE.ACCESS_KEY = BSCS_CONTRACT.ACCESS_KEY
                    AND BSCS_CONTRACT.CUSTOMER_ID = BSCS_CUSTOMER.CUST_ID (+)         
                    ;
COMMIT;    
*/        
 
 -- inserer les données finales
    INSERT /*+ APPEND */ INTO MON.FT_CONTRACT_SNAPSHOT
        (EVENT_DATE, CONTRACT_ID, CUSTOMER_ID, LOGIN, ACCESS_KEY, ACCOUNT_ID, ACTIVATION_DATE, DEACTIVATION_DATE
                , INACTIVITY_BEGIN_DATE, BLOCKED, EXHAUSTED, PERIODIC_FEE, SCRATCH_RELOAD_SUSP, COMMERCIAL_OFFER_ASSIGN_DATE
                , COMMERCIAL_OFFER, BSCS_COMM_OFFER_ID, BSCS_COMM_OFFER, CURRENT_STATUS, STATUS_DATE, OSP_STATUS, LANG, LOCATION, MAIN_IMSI, MSID_TYPE, PROFILE
                , BAD_RELOAD_ATTEMPTS, LAST_TOPUP_DATE, LAST_CREDIT_UPDATE_DATE, BAD_PIN_ATTEMPTS, BAD_PWD_ATTEMPTS
                , OSP_ACCOUNT_TYPE, INACTIVITY_CREDIT_LOSS, DEALER_ID, PROVISIONING_DATE, MAIN_CREDIT, PROMO_CREDIT, SMS_CREDIT
                , DATA_CREDIT, USED_CREDIT_MONTH, USED_CREDIT_LIFE, BUNDLE_LIST, BUNDLE_UNIT_LIST, PROMO_AND_DISCOUNT_LIST
                , INITIAL_SELECTION_DONE, NOMORE_CREDIT, PWD_BLOCKED, FIRST_EVENT_DONE, CUST_EXT_ID, CUST_GROUP, CUST_CATEGORY, CUST_BILLCYCLE, CUST_SEGMENT
                , INSERT_DATE, SRC_TABLE , BSCS_ACTIVATION_DATE, BSCS_DEACTIVATION_DATE
                , OSP_CUST_COMMERCIAL_OFFER, OSP_CUSTOMER_CGLIST, OSP_CONTRACT_TYPE, OSP_CUSTOMER_FORMULE,OPERATOR_CODE,BALANCE_LIST,PREVIOUS_STATUS, CURRENT_STATUS_1, STATE_DATETIME)
    SELECT   a.EVENT_DATE, a.CONTRACT_ID, a.CUSTOMER_ID, a.LOGIN, a.ACCESS_KEY, a.ACCOUNT_ID, a.ACTIVATION_DATE, a.DEACTIVATION_DATE
                , a.INACTIVITY_BEGIN_DATE, a.BLOCKED, a.EXHAUSTED, a.PERIODIC_FEE, a.SCRATCH_RELOAD_SUSP, a.COMMERCIAL_OFFER_ASSIGN_DATE
                , a.COMMERCIAL_OFFER, a.BSCS_COMM_OFFER_ID, a.BSCS_COMM_OFFER, a.CURRENT_STATUS, a.STATUS_DATE, a.OSP_STATUS, a.LANG, a.LOCATION, a.MAIN_IMSI, a.MSID_TYPE
                , a.PROFILE , a.BAD_RELOAD_ATTEMPTS, a.LAST_TOPUP_DATE, a.LAST_CREDIT_UPDATE_DATE, a.BAD_PIN_ATTEMPTS, a.BAD_PWD_ATTEMPTS
                , UPPER(NVL(a.OSP_ACCOUNT_TYPE,b.CUSTOMER_TYPE)) OSP_ACCOUNT_TYPE, a.INACTIVITY_CREDIT_LOSS, a.DEALER_ID, a.PROVISIONING_DATE, a.MAIN_CREDIT, a.PROMO_CREDIT, a.SMS_CREDIT
                , a.DATA_CREDIT, a.USED_CREDIT_MONTH, a.USED_CREDIT_LIFE, a.BUNDLE_LIST, a.BUNDLE_UNIT_LIST, a.PROMO_AND_DISCOUNT_LIST
                , a.INITIAL_SELECTION_DONE, a.NOMORE_CREDIT, a.PWD_BLOCKED, a.FIRST_EVENT_DONE, a.CUST_EXT_ID, a.CUST_GROUP, a.CUST_CATEGORY, a.CUST_BILLCYCLE, a.CUST_SEGMENT
                , a.INSERT_DATE , a.SRC_TABLE, a.BSCS_ACTIVATION_DATE, a.BSCS_DEACTIVATION_DATE , a.OSP_CUST_COMMERCIAL_OFFER, a.OSP_CUSTOMER_CGLIST
                , UPPER(b.CONTRACT_TYPE)  OSP_CONTRACT_TYPE , a.PROFILE OSP_CUSTOMER_FORMULE , a.OPERATOR_CODE, a.BALANCE_LIST,a.PREVIOUS_STATUS, a.CURRENT_STATUS_1, a.STATE_DATETIME
                FROM
                    MON.TT_FT_CONTRACT_SNAPSHOT a ,DIM.DT_OFFER_PROFILES b
                WHERE a.PROFILE=b.PROFILE_CODE(+)     
                ;
    COMMIT;

EXECUTE IMMEDIATE 'TRUNCATE TABLE MON.TT_FT_CONTRACT_SNAPSHOT' ;  


--RETURN 'OK';
--Log fin calcul de la journée @G2d.
insert into LOG_ACTIVATION_DAILY values('CONTRACT_SNAPSHOT', TO_DATE(v_current_date,'yyyymmdd'), sysdate, sysdate, 1);
 
COMMIT;  

------- Calcul de contract snapshot sur le schema MONDV --------------

v_snapshot_return_code := MON.FN_DO_FT_CONTRACT_SNAPSHOT_DV ( v_current_date ); 

ELSE RAISE e_bscs_src_unavailable;  
        END IF;      
  ELSE 
       IF v_zte_return_code='INCONSISTANT' THEN 
                RAISE e_zte_snapshot_inconsistant;
              ELSE RAISE   e_zte_snapshot_unvailable;
       END IF;       
   END IF;   
   ELSE   RAISE e_cont_snap_already_done;     
RETURN 'NOK';     
END IF; 


--    -- Insertion dans l ancien DWH
--    IF  v_olddwh_snapshot_return_code='KO'  THEN
--    
--        INSERT /*+ APPEND */ --INTO mon.ft_contract_snapshot@db_link_to_old_dwh_mon
--           SELECT *
--             FROM ft_contract_snapshot
--            WHERE event_date = TO_DATE (v_current_date, 'yyyymmdd');

--        COMMIT ;
--        
--    END IF;

--

RETURN 'OK'; 

EXCEPTION
     WHEN e_bscs_src_unavailable THEN
       RAISE_APPLICATION_ERROR (-20003,'Sources  BSCS du '||v_current_date||' Pour CONTRACT_SNAPSHOT indisponibles (BSCS_CONTRACT ou BSCS_CUSTOMER)');      
     WHEN e_cont_snap_already_done THEN
        RAISE_APPLICATION_ERROR (-20004,'Photo CONTRACT_SNAPSHOT du '||v_current_date||' Déjà Calculée');               
    WHEN  e_zte_snapshot_unvailable    THEN
        RAISE_APPLICATION_ERROR (-20005,'Source ZTE_SNAPSHOT du '||v_current_date||' Non disponible');     
    WHEN  e_zte_snapshot_inconsistant    THEN
        RAISE_APPLICATION_ERROR (-20006,'Source ZTE_SNAPSHOT du  '||v_current_date||' Incohérente');  
--     WHEN OTHERS THEN
--       RAISE_APPLICATION_ERROR (-20007,'Une Erreur s''est produite lors du calcul de CONTRACT_SNAPSHOT');
END;
/