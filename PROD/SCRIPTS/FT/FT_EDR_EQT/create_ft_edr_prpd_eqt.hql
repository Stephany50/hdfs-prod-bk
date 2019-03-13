------********************************************************************
------*********************Create FT_EDR_PRPD_EQT **********************  
------ ARNOLD CHUENFFO 13-03-2019
------*****************************------**********************************

CREATE TABLE FT_EDR_PRPD_EQT(

    EVENT_TIME     DATE,
    ACCT_ID_MSISDN  VARCHAR2(30),
    MAIN_DEBIT     BIGINT,
    LOAN_DEBIT     BIGINT,
    SASSAYE_DEBIT  BIGINT,
    MAIN_CREDIT    BIGINT,
    LOAN_CREDIT    BIGINT,
    SASSAYE_CREDIT BIGINT,
    TYPE            VARCHAR2(30),
    INSERT_DATE    TIMESTAMP
) COMMENT 'FT_EDR_PRPD_EQT'
PARTITIONED BY (EVENT_DATE	DATE)
STORED AS ORC TBLPROPERTIES ("orc.compress"="ZLIB","orc.stripe.size"="67108864")
;		
