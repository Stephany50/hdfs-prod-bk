select
    subs.acc_nbr MSISDN,
    -(nvl(Bonus_Data.GROSS_BAL, 0) + nvl(Bonus_Data.RESERVE_BAL, 0) + nvl(Bonus_Data.CONSUME_BAL, 0)) Bonus_Data_balance,
    Bonus_Data.exp_date Bonus_Data_Expiry_Date,
    -(nvl(Data_All_Browsing_Pro.GROSS_BAL, 0) + nvl(Data_All_Browsing_Pro.RESERVE_BAL, 0) + nvl(Data_All_Browsing_Pro.CONSUME_BAL, 0)) Data_All_Browsing_Pro_balance,
    Data_All_Browsing_Pro.exp_date Data_All_Browsing_Pro_Expiry_Date,
    -(nvl(Voice_Allnet_E237.GROSS_BAL, 0) + nvl(Voice_Allnet_E237.RESERVE_BAL, 0) + nvl(Voice_Allnet_E237.CONSUME_BAL, 0)) Voice_Allnet_E237_balance,
    Voice_Allnet_E237.exp_date Voice_Allnet_E237_Expiry_Date,
    -(nvl(Benefit_Crossnet_Safari.GROSS_BAL, 0) + nvl(Benefit_Crossnet_Safari.RESERVE_BAL, 0) + nvl(Benefit_Crossnet_Safari.CONSUME_BAL, 0)) Benefit_Crossnet_Safari_balance,
    Benefit_Crossnet_Safari.exp_date Benefit_Crossnet_Safari_Expiry_Date,
    -(nvl(Voice_Onnet_E237.GROSS_BAL, 0) + nvl(Voice_Onnet_E237.RESERVE_BAL, 0) + nvl(Voice_Onnet_E237.CONSUME_BAL, 0)) Voice_Onnet_E237_balance,
    Voice_Onnet_E237.exp_date Voice_Onnet_E237_Expiry_Date,
    -(nvl(Credit_Onnet.GROSS_BAL, 0) + nvl(Credit_Onnet.RESERVE_BAL, 0) + nvl(Credit_Onnet.CONSUME_BAL, 0)) Credit_Onnet_balance,
    Credit_Onnet.exp_date Credit_Onnet_Expiry_Date,
    -(nvl(Bundle_Money_Pro.GROSS_BAL, 0) + nvl(Bundle_Money_Pro.RESERVE_BAL, 0) + nvl(Bundle_Money_Pro.CONSUME_BAL, 0)) Bundle_Money_Pro_balance,
    Bundle_Money_Pro.exp_date Bundle_Money_Pro_Expiry_Date,
    -(nvl(OB_Onnet_Month.GROSS_BAL, 0) + nvl(OB_Onnet_Month.RESERVE_BAL, 0) + nvl(OB_Onnet_Month.CONSUME_BAL, 0)) OB_Onnet_Month_balance,
    OB_Onnet_Month.exp_date OB_Onnet_Month_Expiry_Date,
    -(nvl(OB_Onnet_Week.GROSS_BAL, 0) + nvl(OB_Onnet_Week.RESERVE_BAL, 0) + nvl(OB_Onnet_Week.CONSUME_BAL, 0)) OB_Onnet_Week_balance,
    OB_Onnet_Week.exp_date OB_Onnet_Week_Expiry_Date,
    -(nvl(Woila_DATA.GROSS_BAL, 0) + nvl(Woila_DATA.RESERVE_BAL, 0) + nvl(Woila_DATA.CONSUME_BAL, 0)) Woila_DATA_balance,
    Woila_DATA.exp_date Woila_DATA_Expiry_Date,
    -(nvl(Woila_Local.GROSS_BAL, 0) + nvl(Woila_Local.RESERVE_BAL, 0) + nvl(Woila_Local.CONSUME_BAL, 0)) Woila_Local_balance,
    Woila_Local.exp_date Woila_Local_Expiry_Date,
    -(nvl(Woila_Voice.GROSS_BAL, 0) + nvl(Woila_Voice.RESERVE_BAL, 0) + nvl(Woila_Voice.CONSUME_BAL, 0)) Woila_Voice_balance,
    Woila_Voice.exp_date Woila_Voice_Expiry_Date,
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
    where acct_res_id = 50 and original_file_date = '###SLICE_VALUE###'
) Bonus_Data on subs.acct_id = Bonus_Data.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 75 and original_file_date = '###SLICE_VALUE###'
) Data_All_Browsing_Pro on subs.acct_id = Data_All_Browsing_Pro.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 242 and original_file_date = '###SLICE_VALUE###'
) Voice_Allnet_E237 on subs.acct_id = Voice_Allnet_E237.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 183 and original_file_date = '###SLICE_VALUE###'
) Benefit_Crossnet_Safari on subs.acct_id = Benefit_Crossnet_Safari.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 2534 and original_file_date = '###SLICE_VALUE###'
) Voice_Onnet_E237 on subs.acct_id = Voice_Onnet_E237.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 131 and original_file_date = '###SLICE_VALUE###'
) Credit_Onnet on subs.acct_id = Credit_Onnet.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 76 and original_file_date = '###SLICE_VALUE###'
) Bundle_Money_Pro on subs.acct_id = Bundle_Money_Pro.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 200 and original_file_date = '###SLICE_VALUE###'
) OB_Onnet_Month on subs.acct_id = OB_Onnet_Month.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 197 and original_file_date = '###SLICE_VALUE###'
) OB_Onnet_Week on subs.acct_id = OB_Onnet_Week.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 233 and original_file_date = '###SLICE_VALUE###'
) Woila_DATA on subs.acct_id = Woila_DATA.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 2034 and original_file_date = '###SLICE_VALUE###'
) Woila_Local on subs.acct_id = Woila_Local.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 231 and original_file_date = '###SLICE_VALUE###'
) Woila_Voice on subs.acct_id = Woila_Voice.acct_id
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


