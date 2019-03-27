------********************************************************************
------*********************Create FT_EDR_PRPD_EQT **********************  
------ ARNOLD CHUENFFO 13-03-2019 ; Modify by SNR 20-03-2019
------*****************************------**********************************

CREATE TABLE MON.FT_EDR_PRPD_EQT(
    EVENT_TIME     TIMESTAMP,
    ACCT_ID_MSISDN  VARCHAR(30),
    MAIN_DEBIT     DECIMAL(13,2),
    LOAN_DEBIT     DECIMAL(13,2),
    SASSAYE_DEBIT  DECIMAL(13,2),
    MAIN_CREDIT    DECIMAL(13,2),
    LOAN_CREDIT    DECIMAL(13,2),
    SASSAYE_CREDIT DECIMAL(13,2),
    TYPE            VARCHAR(30),
    INSERT_DATE    TIMESTAMP
) COMMENT 'FT_EDR_PRPD_EQT'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS ORC TBLPROPERTIES ('transactional'='true','orc.compress'='ZLIB','orc.stripe.size'='67108864')
;