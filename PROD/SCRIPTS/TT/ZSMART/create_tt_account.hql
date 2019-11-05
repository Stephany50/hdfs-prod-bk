CREATE EXTERNAL TABLE CDR.TT_ACCOUNT (

    Contract_Id VARCHAR(200)
    ,Main_MSISDN VARCHAR(200)
    ,Main_IMSI VARCHAR(200)
    ,Operator_code VARCHAR(200)
    ,Commercial_Offer VARCHAR(200)
    ,Contract_Type VARCHAR(200)
    ,Provisionning_Date VARCHAR(200)
    ,Activation_Date VARCHAR(200)
    ,Deactivation_date VARCHAR(200)
    ,Account_Status VARCHAR(200)
    ,Language VARCHAR(200)
    ,Main_Used_Credit_Month VARCHAR(200)
    ,Main_Used_Credit_Life VARCHAR(200)
    ,Main_Credit VARCHAR(200)
    ,Promo_Credit VARCHAR(200)
    ,Credit_SMS VARCHAR(200)
    ,Credit_Data VARCHAR(200)
    ,Status_Date VARCHAR(200)
    ,Commercial_Offer_Assign_Date VARCHAR(200)
    ,Last_Topup_Date VARCHAR(200)
    ,Last_Main_Credit_Update_Date VARCHAR(200)
    ,Last_Promo_Credit_Update_Date VARCHAR(200)
    ,Balance_List VARCHAR(200)
    ,BUNDLE_ONNET VARCHAR(200)
    ,BUNDLE_OFFNET VARCHAR(200)
    ,BUNDLE_CROSS_NET VARCHAR(200)
    ,BUNDLE_INTERNATIONAL VARCHAR(200)
    ,BUNDLE_ALLNET
    ,ORIGINAL_FILE_NAME	VARCHAR(200)
    ,ORIGINAL_FILE_SIZE INT
    ,ORIGINAL_FILE_LINE_COUNT INT

)
    COMMENT 'ACCOUNT external tables-TT'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
    LOCATION '/PROD/TT/ZMART/ACCOUNT_/'
    TBLPROPERTIES ('serialization.null.format'='');