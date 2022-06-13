SELECT IF(
    T_1.ft_exist = 0
    and T_2.it_EXIT > 1
    and T_3.it_EXIT > 1
    , 'OK'
    , 'NOK'
)
FROM
(select count(*) ft_exist from mon.spark_ft_btl_report where TRANSACTION_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) it_EXIT FROM cdr.spark_it_btl_report where original_file_date = '###SLICE_VALUE###') T_2,
(select count(*) it_EXIT from CDR.SPARK_IT_ZTE_IPP_EXTRACT where original_file_date >= date_sub('###SLICE_VALUE###', 7)) T_3