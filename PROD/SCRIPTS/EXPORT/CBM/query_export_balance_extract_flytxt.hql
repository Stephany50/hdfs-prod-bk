select
    subs.acc_nbr MSISDN,
    -(nvl(DATA_Internet_Mobile.GROSS_BAL, 0) + nvl(DATA_Internet_Mobile.RESERVE_BAL, 0) + nvl(DATA_Internet_Mobile.CONSUME_BAL, 0)) DATA_Internet_Mobile_balance,
    DATA_Internet_Mobile.exp_date DATA_Internet_Mobile_Expiry_Date,
    -(nvl(Loyalty_Data.GROSS_BAL, 0) + nvl(Loyalty_Data.RESERVE_BAL, 0) + nvl(Loyalty_Data.CONSUME_BAL, 0)) Loyalty_Data_balance,
    Loyalty_Data.exp_date Loyalty_Data_Expiry_Date,
    -(nvl(Loyalty_Credit.GROSS_BAL, 0) + nvl(Loyalty_Credit.RESERVE_BAL, 0) + nvl(Loyalty_Credit.CONSUME_BAL, 0)) Loyalty_Credit_balance,
    Loyalty_Credit.exp_date Loyalty_Credit_Expiry_Date,
    -(nvl(OB_All_Net_Day.GROSS_BAL, 0) + nvl(OB_All_Net_Day.RESERVE_BAL, 0) + nvl(OB_All_Net_Day.CONSUME_BAL, 0)) OB_All_Net_Day_balance,
    OB_All_Net_Day.exp_date OB_All_Net_Day_Expiry_Date,
    -(nvl(OB_All_Net_Month.GROSS_BAL, 0) + nvl(OB_All_Net_Month.RESERVE_BAL, 0) + nvl(OB_All_Net_Month.CONSUME_BAL, 0)) OB_All_Net_Month_balance,
    OB_All_Net_Month.exp_date OB_All_Net_Month_Expiry_Date,
    -(nvl(OB_All_Net_Week.GROSS_BAL, 0) + nvl(OB_All_Net_Week.RESERVE_BAL, 0) + nvl(OB_All_Net_Week.CONSUME_BAL, 0)) OB_All_Net_Week_balance,
    OB_All_Net_Week.exp_date OB_All_Net_Week_Expiry_Date,
    -(nvl(OB_Data_Day.GROSS_BAL, 0) + nvl(OB_Data_Day.RESERVE_BAL, 0) + nvl(OB_Data_Day.CONSUME_BAL, 0)) OB_Data_Day_balance,
    OB_Data_Day.exp_date OB_Data_Day_Expiry_Date,
    -(nvl(OB_Data_Month.GROSS_BAL, 0) + nvl(OB_Data_Month.RESERVE_BAL, 0) + nvl(OB_Data_Month.CONSUME_BAL, 0)) OB_Data_Month_balance,
    OB_Data_Month.exp_date OB_Data_Month_Expiry_Date,
    -(nvl(OB_Data_Week.GROSS_BAL, 0) + nvl(OB_Data_Week.RESERVE_BAL, 0) + nvl(OB_Data_Week.CONSUME_BAL, 0)) OB_Data_Week_balance,
    OB_Data_Week.exp_date OB_Data_Week_Expiry_Date,
    -(nvl(OB_Onnet_Day.GROSS_BAL, 0) + nvl(OB_Onnet_Day.RESERVE_BAL, 0) + nvl(OB_Onnet_Day.CONSUME_BAL, 0)) OB_Onnet_Day_balance,
    OB_Onnet_Day.exp_date OB_Onnet_Day_Expiry_Date,
    CURRENT_TIMESTAMP() INSERT_DATE,
    subs.ORIGINAL_FILE_DATE
from 
(
    select distinct acc_nbr, 
        acct_id, 
        original_file_date 
    from CDR.SPARK_IT_ZTE_SUBS_EXTRACT 
    where original_file_date = '###SLICE_VALUE###'
) subs
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 37 and original_file_date = '###SLICE_VALUE###'
) DATA_Internet_Mobile on subs.acct_id = DATA_Internet_Mobile.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 49 and original_file_date = '###SLICE_VALUE###'
) Loyalty_Data on subs.acct_id = Loyalty_Data.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 47 and original_file_date = '###SLICE_VALUE###'
) Loyalty_Credit on subs.acct_id = Loyalty_Credit.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 198 and original_file_date = '###SLICE_VALUE###'
) OB_All_Net_Day on subs.acct_id = OB_All_Net_Day.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 201 and original_file_date = '###SLICE_VALUE###'
) OB_All_Net_Month on subs.acct_id = OB_All_Net_Month.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 199 and original_file_date = '###SLICE_VALUE###'
) OB_All_Net_Week on subs.acct_id = OB_All_Net_Week.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 834 and original_file_date = '###SLICE_VALUE###'
) OB_Data_Day on subs.acct_id = OB_Data_Day.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 836 and original_file_date = '###SLICE_VALUE###'
) OB_Data_Month on subs.acct_id = OB_Data_Month.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 835 and original_file_date = '###SLICE_VALUE###'
) OB_Data_Week on subs.acct_id = OB_Data_Week.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 16 and original_file_date = '###SLICE_VALUE###'
) OB_Onnet_Day on subs.acct_id = OB_Onnet_Day.acct_id


