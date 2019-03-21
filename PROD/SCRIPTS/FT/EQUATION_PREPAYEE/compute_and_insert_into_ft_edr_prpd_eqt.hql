
-------------- CONSO DATA -------------
INSERT INTO TABLE MON.FT_EDR_PRPD_EQT PARTITION(EVENT_DATE)
SELECT FROM_UNIXTIME(unix_timestamp(START_TIME), 'yyyy-MM-dd HH:mm:ss') EVENT_TIME, SUBSTR(BILLING_NBR, 4, 9) MSISDN,
    ( CASE WHEN ACCT_RES_ID1 = 1 THEN NVL(CHARGE1, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID2 = 1 THEN NVL(CHARGE2, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID3 = 1 THEN NVL(CHARGE3, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID4 = 1 THEN NVL(CHARGE4, 0) ELSE 0 END
    )/100  MAIN_DEBIT, 0  MAIN_CREDIT,
    ( CASE WHEN ACCT_RES_ID1 = 20 THEN NVL(CHARGE1, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID2 = 20 THEN NVL(CHARGE2, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID3 = 20 THEN NVL(CHARGE3, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID4 = 20 THEN NVL(CHARGE4, 0) ELSE 0 END
    )/100  LOAN_DEBIT, 0  LOAN_CREDIT,
    ( CASE WHEN ACCT_RES_ID1 = 21 THEN NVL(CHARGE1, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID2 = 21 THEN NVL(CHARGE2, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID3 = 21 THEN NVL(CHARGE3, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID4 = 21 THEN NVL(CHARGE4, 0) ELSE 0 END
    )/100 SASSAYE_DEBIT, 0 SASSAYE_CREDIT,
    'CONSO DATA' TYPE, current_timestamp() INSERT_DATE, cast(START_TIME as date) EVENT_DATE
FROM CDR.IT_ZTE_DATA A
WHERE START_DATE ='2019-03-11'
    AND (ACCT_RES_ID1 IN ( 1, 20, 21) OR ACCT_RES_ID2 IN ( 1, 20, 21)
        OR ACCT_RES_ID3 IN ( 1, 20, 21) OR ACCT_RES_ID4 IN ( 1, 20, 21))
    AND (NVL(CHARGE1, 0) !=0 or NVL(CHARGE2, 0) !=0 or NVL(CHARGE3, 0) !=0 or NVL(CHARGE4, 0) !=0)
GROUP BY START_TIME, CALLING_NBR, BILLING_IMSI, CALLING_IMEI, CALLED_NBR, BILLING_NBR,
    PROVIDER_ID, RESULT_CODE,
    -- ajout de critère de dédoublonnage : ronny.samo@orange.com 06/08/2018
    CASE WHEN ACCT_RES_ID1 = 1 THEN NVL(CHARGE1, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID2 = 1 THEN NVL(CHARGE2, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID3 = 1 THEN NVL(CHARGE3, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID4 = 1 THEN NVL(CHARGE4, 0) ELSE 0 END,
    CASE WHEN ACCT_RES_ID1 = 20 THEN NVL(CHARGE1, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID2 = 20 THEN NVL(CHARGE2, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID3 = 20 THEN NVL(CHARGE3, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID4 = 20 THEN NVL(CHARGE4, 0) ELSE 0 END,
    CASE WHEN ACCT_RES_ID1 = 21 THEN NVL(CHARGE1, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID2 = 21 THEN NVL(CHARGE2, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID3 = 21 THEN NVL(CHARGE3, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID4 = 21 THEN NVL(CHARGE4, 0) ELSE 0 END,
    BAL_ID1, BAL_ID2, BAL_ID3, BAL_ID4, --ACCT_RES_ID2, ACCT_RES_ID1, ACCT_RES_ID3, ACCT_RES_ID4
    BALANCE1, BALANCE2, BALANCE3, BALANCE4,   PRICE_PLAN_ID1, PRICE_PLAN_ID2, PRICE_PLAN_ID3, PRICE_PLAN_ID4,
    PRICE_ID1, PRICE_ID2, PRICE_ID3, PRICE_ID4, ACCT_ITEM_TYPE_ID1, ACCT_ITEM_TYPE_ID2, ACCT_ITEM_TYPE_ID3, ACCT_ITEM_TYPE_ID4
         ;

-------------- CONSO VOIX SMS ----------
INSERT INTO TABLE MON.FT_EDR_PRPD_EQT PARTITION(EVENT_DATE)
SELECT FROM_UNIXTIME(unix_timestamp(START_TIME), 'yyyy-MM-dd HH:mm:ss') EVENT_TIME, SUBSTR(BILLING_NBR, 4, 9) MSISDN,
    ( CASE WHEN ACCT_RES_ID1 = 1 THEN NVL(CHARGE1, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID2 = 1 THEN NVL(CHARGE2, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID3 = 1 THEN NVL(CHARGE3, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID4 = 1 THEN NVL(CHARGE4, 0) ELSE 0 END
    )/100  MAIN_DEBIT, 0  MAIN_CREDIT,
    ( CASE WHEN ACCT_RES_ID1 = 20 THEN NVL(CHARGE1, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID2 = 20 THEN NVL(CHARGE2, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID3 = 20 THEN NVL(CHARGE3, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID4 = 20 THEN NVL(CHARGE4, 0) ELSE 0 END
    )/100  LOAN_DEBIT, 0  LOAN_CREDIT,
    ( CASE WHEN ACCT_RES_ID1 = 21 THEN NVL(CHARGE1, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID2 = 21 THEN NVL(CHARGE2, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID3 = 21 THEN NVL(CHARGE3, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID4 = 21 THEN NVL(CHARGE4, 0) ELSE 0 END
    )/100 SASSAYE_DEBIT, 0 SASSAYE_CREDIT,
    'CONSO VOIX SMS' TYPE, current_timestamp() INSERT_DATE, cast(START_TIME as date) EVENT_DATE
FROM CDR.IT_ZTE_VOICE_SMS A
WHERE START_DATE ='2019-03-11'
    AND (ACCT_RES_ID1 IN ( 1, 20, 21) OR ACCT_RES_ID2 IN ( 1, 20, 21)
        OR ACCT_RES_ID3 IN ( 1, 20, 21) OR ACCT_RES_ID4 IN ( 1, 20, 21))
    AND (NVL(CHARGE1, 0) !=0 or NVL(CHARGE2, 0) !=0 or NVL(CHARGE3, 0) !=0 or NVL(CHARGE4, 0) !=0)
GROUP BY START_TIME, CALLING_NBR, BILLING_IMSI, CALLING_IMEI, CALLED_NBR, BILLING_NBR,
    PROVIDER_ID, RESULT_CODE,
    -- ajout de critère de dédoublonnage : ronny.samo@orange.com 06/08/2018
    CASE WHEN ACCT_RES_ID1 = 1 THEN NVL(CHARGE1, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID2 = 1 THEN NVL(CHARGE2, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID3 = 1 THEN NVL(CHARGE3, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID4 = 1 THEN NVL(CHARGE4, 0) ELSE 0 END,
    CASE WHEN ACCT_RES_ID1 = 20 THEN NVL(CHARGE1, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID2 = 20 THEN NVL(CHARGE2, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID3 = 20 THEN NVL(CHARGE3, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID4 = 20 THEN NVL(CHARGE4, 0) ELSE 0 END,
    CASE WHEN ACCT_RES_ID1 = 21 THEN NVL(CHARGE1, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID2 = 21 THEN NVL(CHARGE2, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID3 = 21 THEN NVL(CHARGE3, 0) ELSE 0 END
        + CASE  WHEN ACCT_RES_ID4 = 21 THEN NVL(CHARGE4, 0) ELSE 0 END,
    BAL_ID1, BAL_ID2, BAL_ID3, BAL_ID4, --ACCT_RES_ID2, ACCT_RES_ID1, ACCT_RES_ID3, ACCT_RES_ID4
    BALANCE1, BALANCE2, BALANCE3, BALANCE4,   PRICE_PLAN_ID1, PRICE_PLAN_ID2, PRICE_PLAN_ID3, PRICE_PLAN_ID4,
    PRICE_ID1, PRICE_ID2, PRICE_ID3, PRICE_ID4, ACCT_ITEM_TYPE_ID1, ACCT_ITEM_TYPE_ID2, ACCT_ITEM_TYPE_ID3, ACCT_ITEM_TYPE_ID4
       ;

-------------- RECHARGES  --------------
INSERT INTO TABLE MON.FT_EDR_PRPD_EQT PARTITION(EVENT_DATE)
SELECT FROM_UNIXTIME(unix_timestamp(PAY_TIME), 'yyyy-MM-dd HH:mm:ss') EVENT_TIME, SUBSTR(ACC_NBR, 4, 9) MSISDN,
    CASE WHEN ACCT_RES_CODE = 1 AND BILL_AMOUNT<0 THEN -BILL_AMOUNT ELSE 0 END/100 MAIN_CREDIT,
    CASE WHEN ACCT_RES_CODE = 20 AND BILL_AMOUNT<0 THEN -BILL_AMOUNT ELSE 0 END/100 LOAN_CREDIT,
    CASE WHEN ACCT_RES_CODE = 21 AND BILL_AMOUNT<0 THEN -BILL_AMOUNT ELSE 0 END/100 SASSAYE_CREDIT,
    CASE WHEN ACCT_RES_CODE = 1 AND BILL_AMOUNT>0 THEN BILL_AMOUNT ELSE 0 END/100 MAIN_DEBIT,
    CASE WHEN ACCT_RES_CODE = 20 AND BILL_AMOUNT>0 THEN BILL_AMOUNT ELSE 0 END/100 LOAN_DEBIT,
    CASE WHEN ACCT_RES_CODE = 21 AND BILL_AMOUNT>0 THEN BILL_AMOUNT ELSE 0 END/100 SASSAYE_DEBIT,
    CASE WHEN CHANNEL_ID=9 THEN 'RECHARGE SOS DATA' else 'RECHARGE' end TYPE, current_timestamp() INSERT_DATE, cast(PAY_TIME as date) EVENT_DATE
FROM CDR.IT_ZTE_RECHARGE
WHERE PAY_DATE = '2019-03-11'
    and ACCT_RES_CODE IN ( 1, 20, 21)
    AND  BILL_AMOUNT!= 0
GROUP BY PAY_TIME, PAYMENT_ID, ACC_NBR, PREPAY_FLAG, CHANNEL_ID, PAYMENT_METHOD, ACCT_RES_CODE, BENEFIT_NAME, BILL_AMOUNT, LOAN_AMOUNT, COMMISSION_AMOUNT
    ;

INSERT INTO TABLE MON.FT_EDR_PRPD_EQT PARTITION(EVENT_DATE)
SELECT FROM_UNIXTIME(unix_timestamp(PAY_TIME), 'yyyy-MM-dd HH:mm:ss') EVENT_TIME, SUBSTR(ACC_NBR, 4, 9) MSISDN,
    0 MAIN_CREDIT, 0 LOAN_CREDIT, 0 SASSAYE_CREDIT,
    CASE WHEN ACCT_RES_CODE = 1 THEN LOAN_AMOUNT ELSE 0 END/100 MAIN_DEBIT,
    CASE WHEN ACCT_RES_CODE = 20 THEN LOAN_AMOUNT ELSE 0 END/100 LOAN_DEBIT,
    CASE WHEN ACCT_RES_CODE = 21 THEN LOAN_AMOUNT ELSE 0 END/100 SASSAYE_DEBIT,
    CASE WHEN CHANNEL_ID=9 THEN 'RECHARGE SOS DATA LOAN' else 'RECHARGE LOAN' end TYPE, current_timestamp() INSERT_DATE, cast(PAY_TIME as date) EVENT_DATE
FROM CDR.IT_ZTE_RECHARGE A
WHERE PAY_DATE = '2019-03-11'
    and ACCT_RES_CODE IN ( 1, 20, 21)
    AND  BILL_AMOUNT!= 0
    AND LOAN_AMOUNT != 0
GROUP BY PAY_TIME, PAYMENT_ID, ACC_NBR, PREPAY_FLAG, CHANNEL_ID, PAYMENT_METHOD, ACCT_RES_CODE, BENEFIT_NAME, BILL_AMOUNT, LOAN_AMOUNT, COMMISSION_AMOUNT
    ;

INSERT INTO TABLE MON.FT_EDR_PRPD_EQT PARTITION(EVENT_DATE)
SELECT FROM_UNIXTIME(unix_timestamp(PAY_TIME), 'yyyy-MM-dd HH:mm:ss') EVENT_TIME, SUBSTR(ACC_NBR, 4, 9)  MSISDN,
    0 MAIN_CREDIT, 0 LOAN_CREDIT, 0 SASSAYE_CREDIT,
    CASE WHEN ACCT_RES_CODE = 1  THEN COMMISSION_AMOUNT ELSE 0 END/100 MAIN_DEBIT,
    CASE WHEN ACCT_RES_CODE = 20 THEN COMMISSION_AMOUNT ELSE 0 END/100 LOAN_DEBIT,
    CASE WHEN ACCT_RES_CODE = 21 THEN COMMISSION_AMOUNT ELSE 0 END/100 SASSAYE_DEBIT,
    CASE WHEN CHANNEL_ID=9 THEN 'RECHARGE SOS DATA CMS' else 'RECHARGE LOAN CMS' end TYPE, current_timestamp() INSERT_DATE, cast(PAY_TIME as date) EVENT_DATE
FROM CDR.IT_ZTE_RECHARGE
WHERE PAY_DATE = '2019-03-11'
    and ACCT_RES_CODE IN ( 1, 20, 21)
    AND  BILL_AMOUNT!= 0
    AND COMMISSION_AMOUNT != 0
GROUP BY PAY_TIME, PAYMENT_ID, ACC_NBR, PREPAY_FLAG, CHANNEL_ID, PAYMENT_METHOD, ACCT_RES_CODE, BENEFIT_NAME, BILL_AMOUNT, LOAN_AMOUNT, COMMISSION_AMOUNT
    ;

------------- EMERGENCY DATA ----------
INSERT INTO TABLE MON.FT_EDR_PRPD_EQT PARTITION(EVENT_DATE)
SELECT FROM_UNIXTIME(unix_timestamp(CONCAT(TRANSACTION_DATE, ' ',TRANSACTION_TIME), 'yyyy-MM-dd HHmmss' ), 'yyyy-MM-dd HH:mm:ss') EVENT_TIME, SUBSTR(MSISDN, 4, 9)  MSISDN,
    CASE WHEN TRANSACTION_TYPE= 'LOAN' AND AMOUNT>0 THEN AMOUNT
        WHEN TRANSACTION_TYPE= 'PAYBACK' AND AMOUNT<0 THEN - AMOUNT
        ELSE AMOUNT END /100 MAIN_DEBIT,  0 LOAN_DEBIT, 0 SASSAYE_DEBIT,
    0 MAIN_CREDIT, 0 LOAN_CREDIT, 0 SASSAYE_CREDIT,
    CASE WHEN TRANSACTION_TYPE= 'LOAN' THEN 'SOS DATA LOAN'
        WHEN TRANSACTION_TYPE= 'PAYBACK' THEN 'SOS DATA PAYBACK'
    ELSE 'SOS DATA' END TYPE, current_timestamp() INSERT_DATE, TRANSACTION_DATE EVENT_DATE
FROM CDR.IT_ZTE_EMERGENCY_DATA
WHERE TRANSACTION_DATE = '2019-03-11'
    AND  AMOUNT!= 0
GROUP BY FROM_UNIXTIME(unix_timestamp(CONCAT(TRANSACTION_DATE, ' ',TRANSACTION_TIME), 'yyyy-MM-dd HHmmss' ), 'yyyy-MM-dd HH:mm:ss'), TRANSACTION_DATE, MSISDN, AMOUNT, TRANSACTION_TYPE, FEE, CONTACT_CHANNEL
 ;

-------------- BAL RESET -------------
INSERT INTO TABLE MON.FT_EDR_PRPD_EQT PARTITION(EVENT_DATE)
SELECT event_time, msisdn, case when main_reset <0 then -main_reset/100 else 0 end main_debit, case when loan_reset <0 then -loan_reset/100 else 0 end loan_debit,
   case when sassaye_reset <0 then -sassaye_reset/100 else 0 end sassaye_debit, case when main_reset >0 then main_reset/100 else 0 end main_credit,
   case when loan_reset >0 then loan_reset/100 else 0 end loan_credit, case when sassaye_reset >0 then sassaye_reset/100 else 0 end sassaye_credit,
   'BAL RESET' TYPE, current_timestamp() insert_date, cast(EVENT_TIME as date) EVENT_DATE
FROM (
    SELECT FROM_UNIXTIME(unix_timestamp(BAL_RESET_TIME), 'yyyy-MM-dd HH:mm:ss') event_time, SUBSTR(ACC_NBR, 4, 9) MSISDN,
       case when pre_balance like '1:%' or pre_balance like '%;1:%' then
           substr(substr(PRE_BALANCE, instr(PRE_BALANCE, ';1:')+3),1, instr(substr(PRE_BALANCE, instr(PRE_BALANCE, ';1:')+3), ';')-1)
       else '0' end main_reset,
       case when pre_balance like '21:%' or pre_balance like '%;21:%' then
           substr(substr(PRE_BALANCE, instr(PRE_BALANCE, ';21:')+4),1, instr(substr(PRE_BALANCE, instr(PRE_BALANCE, ';21:')+4), ';')-1)
       else '0' end sassaye_reset,
       case when pre_balance like '20:%' or pre_balance like '%;20:%' then
           substr(substr(PRE_BALANCE, instr(PRE_BALANCE, ';20:')+4),1, instr(substr(PRE_BALANCE, instr(PRE_BALANCE, ';20:')+4), ';')-1)
       else '0' end loan_reset, PRE_BALANCE
    FROM  CDR.IT_ZTE_BALANCE_RESET
    WHERE BAL_RESET_DATE = '2019-03-16'
       and (pre_balance like '20:%' or pre_balance like '%;20:%' or pre_balance like '1:%' or pre_balance like '%;1:%'
               or pre_balance like '21:%' or pre_balance like '%;21:%')
    GROUP by acc_nbr, acct_code, bal_reset_time, pre_balance,  provider_id
) a ;

-------------- DEL EXPBAL -------------
INSERT INTO TABLE MON.FT_EDR_PRPD_EQT PARTITION(EVENT_DATE)
SELECT FROM_UNIXTIME(unix_timestamp(CREATION_DATE), 'yyyy-MM-dd HH:mm:ss') EVENT_TIME, ACC_NBR MSISDN,
  CASE WHEN ACCT_RES_ID = 1 AND ADJUST_AMOUNT<0 THEN -ADJUST_AMOUNT ELSE 0 END/100 MAIN_DEBIT,
  CASE WHEN ACCT_RES_ID = 20 AND ADJUST_AMOUNT<0 THEN -ADJUST_AMOUNT ELSE 0 END/100 LOAN_DEBIT,
  CASE WHEN ACCT_RES_ID = 21 AND ADJUST_AMOUNT<0 THEN -ADJUST_AMOUNT ELSE 0 END/100 SASSAYE_DEBIT,
  CASE WHEN ACCT_RES_ID = 1 AND ADJUST_AMOUNT>0 THEN ADJUST_AMOUNT ELSE 0 END/100 MAIN_CREDIT,
  CASE WHEN ACCT_RES_ID = 20 AND ADJUST_AMOUNT>0 THEN ADJUST_AMOUNT ELSE 0 END/100 LOAN_CREDIT,
  CASE WHEN ACCT_RES_ID = 21 AND ADJUST_AMOUNT>0 THEN ADJUST_AMOUNT ELSE 0 END/100 SASSAYE_CREDIT,
 'DEL EXPBAL' type, current_timestamp() insert_date, cast(FROM_UNIXTIME(unix_timestamp(CREATION_DATE), 'yyyy-MM-dd HH:mm:ss') as date) EVENT_DATE
FROM CDR.IT_ZTE_DEL_EXPBAL
WHERE ORIGINAL_FILE_DATE = '2019-03-11'
    AND ACCT_RES_ID IN (1, 20, 21) AND BALANCE_BEFORE !=0
GROUP BY FROM_UNIXTIME(unix_timestamp(CREATION_DATE), 'yyyy-MM-dd HH:mm:ss'), acc_nbr, subs_id, acct_id, bal_id, acct_res_id, acct_book_type_name, balance_before, adjust_amount
;



select *
from MON.FT_EDR_PRPD_EQT