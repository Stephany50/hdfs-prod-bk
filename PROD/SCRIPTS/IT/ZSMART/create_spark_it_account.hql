CREATE TABLE CDR.SPARK_IT_ACCOUNT (
     Contract_Id VARCHAR(200)
    ,Main_MSISDN VARCHAR(200)
    ,Main_IMSI VARCHAR(200)
    ,Operator_code VARCHAR(200)
    ,Commercial_Offer VARCHAR(200)
    ,Contract_Type VARCHAR(200)
    ,Provisionning_Date timestamp
    ,Activation_Date timestamp
    ,Deactivation_date timestamp
    ,Account_Status VARCHAR(200)
    ,Language VARCHAR(200)
    ,Main_Used_Credit_Month VARCHAR(200)
    ,Main_Used_Credit_Life VARCHAR(200)
    ,Main_Credit VARCHAR(200)
    ,Promo_Credit VARCHAR(200)
    ,Credit_SMS VARCHAR(200)
    ,Credit_Data VARCHAR(200)
    ,Status_Date timestamp
    ,Commercial_Offer_Assign_Date timestamp
    ,Last_Topup_Date timestamp
    ,Last_Main_Credit_Update_Date timestamp
    ,Last_Promo_Credit_Update_Date timestamp
    ,Balance_List VARCHAR(200)
    ,BUNDLE_ONNET VARCHAR(200)
    ,BUNDLE_OFFNET VARCHAR(200)
    ,BUNDLE_CROSS_NET VARCHAR(200)
    ,BUNDLE_INTERNATIONAL VARCHAR(200)
    ,BUNDLE_ALLNET VARCHAR(200)
    ,ORIGINAL_FILE_NAME VARCHAR(200)
    ,ORIGINAL_FILE_SIZE INT
    ,ORIGINAL_FILE_LINE_COUNT INT
    ,INSERT_DATE timestamp
)
PARTITIONED BY (original_file_date DATE)
CLUSTERED BY(Contract_Id) INTO 8 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY")