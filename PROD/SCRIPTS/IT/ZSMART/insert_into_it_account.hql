INSERT INTO CDR.IT_ACCOUNT
SELECT
    Contract_Id  ,
    Main_MSISDN  ,
    Main_IMSI  ,
    Operator_code  ,
    Commercial_Offer ,
    Contract_Type  ,
    FROM_UNIXTIME(UNIX_TIMESTAMP(Provisionning_Date, 'dd/MM/yy hh:mm:ss')) Provisionning_Date,

    FROM_UNIXTIME(UNIX_TIMESTAMP(Activation_Date, 'dd/MM/yy hh:mm:ss')) Activation_Date ,
    FROM_UNIXTIME(UNIX_TIMESTAMP(Deactivation_date, 'dd/MM/yy hh:mm:ss')) Deactivation_date ,
    Account_Status  ,
    Language  ,
    Main_Used_Credit_Month  ,
    Main_Used_Credit_Life  ,
    Main_Credit  ,
    Promo_Credit  ,
    Credit_SMS  ,
    Credit_Data  ,
    FROM_UNIXTIME(UNIX_TIMESTAMP(Status_Date, 'dd/MM/yy hh:mm:ss')) Status_Date ,
    FROM_UNIXTIME(UNIX_TIMESTAMP(Commercial_Offer_Assign_Date, 'dd/MM/yy hh:mm:ss')) Commercial_Offer_Assign_Date ,
    FROM_UNIXTIME(UNIX_TIMESTAMP(Last_Topup_Date, 'dd/MM/yy hh:mm:ss')) Last_Topup_Date ,
    FROM_UNIXTIME(UNIX_TIMESTAMP(Last_Main_Credit_Update_Date, 'dd/MM/yy hh:mm:ss')) Last_Main_Credit_Update_Date ,
    FROM_UNIXTIME(UNIX_TIMESTAMP(Last_Promo_Credit_Update_Date, 'dd/MM/yy hh:mm:ss')) Last_Promo_Credit_Update_Date ,
    Balance_List  ,
    BUNDLE_ONNET  ,
    BUNDLE_OFFNET  ,
    BUNDLE_CROSS_NET  ,
    BUNDLE_INTERNATIONAL  ,
    ORIGINAL_FILE_NAME	 ,
    ORIGINAL_FILE_SIZE ,
    ORIGINAL_FILE_LINE_COUNT ,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -25, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE


FROM CDR.TT_ACCOUNT  C
         LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM   CDR.IT_ACCOUNT) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL;