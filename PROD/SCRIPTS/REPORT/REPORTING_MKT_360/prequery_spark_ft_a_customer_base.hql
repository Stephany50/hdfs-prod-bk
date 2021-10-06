SELECT 
IF(
    T_1.FT_EXIST = 0  and 
    T_2.FT_EXIST > 0  
    ,"OK","NOK"
    
) STATUT
FROM
(SELECT COUNT(*) FT_EXIST FROM agg.spark_ft_a_customer_base WHERE event_date='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_EXIST FROM mon.spark_ft_customer_base WHERE event_date='###SLICE_VALUE###') T_2
