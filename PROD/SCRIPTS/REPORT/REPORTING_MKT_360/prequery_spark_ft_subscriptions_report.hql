SELECT 
IF(
    T_1.FT_EXIST = 0  and 
    T_2.IT_EXIST = datediff('###SLICE_VALUE###', substr('###SLICE_VALUE###', 1, 7)||'-01') + 1  and 
    T_3.FT_EXIST = datediff('###SLICE_VALUE###', substr('###SLICE_VALUE###', 1, 7)||'-01') + 1  
    ,"OK","NOK"
    
) STATUT
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.spark_ft_subscriptions_report WHERE event_date='###SLICE_VALUE###') T_1,
(SELECT COUNT(distinct event_date) IT_EXIST from CDR.SPARK_IT_ZEMBLAREPORT where event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###') T_2,
(SELECT COUNT(distinct transaction_date) FT_EXIST from mon.spark_ft_subscription where transaction_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###') T_3



