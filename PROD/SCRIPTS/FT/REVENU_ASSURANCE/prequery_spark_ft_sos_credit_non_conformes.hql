SELECT IF(
    T_1.COUNT_1 = 0 and 
    T_2.COUNT_2 = datediff('###SLICE_VALUE###', add_months('###SLICE_VALUE###', -3)) + 1 AND 
    T_3.COUNT_3 > 0 AND 
    T_4.COUNT_4 = datediff(last_day(add_months(concat(substr('###SLICE_VALUE###', 1, 7), '-01'), -1)), add_months(last_day(add_months(concat(substr('###SLICE_VALUE###', 1, 7), '-01'), -1)), -3)) + 1 AND 
    T_5.COUNT_5 = datediff('###SLICE_VALUE###', add_months('###SLICE_VALUE###', -3)) + 1 AND 
    T_6.COUNT_6 = datediff('###SLICE_VALUE###', add_months('###SLICE_VALUE###', -3)) + 1
,"OK","NOK") FT_EXIST
FROM
(SELECT COUNT(*) COUNT_1 FROM mon.spark_ft_sos_credit_non_conformes WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(distinct transaction_date) COUNT_2 FROM MON.SPARK_FT_OVERDRAFT WHERE TRANSACTION_DATE between add_months('###SLICE_VALUE###', -3) AND '###SLICE_VALUE###') T_2,
(SELECT COUNT(*) COUNT_3 from MON.spark_FT_CONTRACT_SNAPSHOT where EVENT_DATE = '###SLICE_VALUE###') T_3,
(SELECT COUNT(distinct event_date) COUNT_4 FROM MON.spark_FT_EDR_PRPD_EQT WHERE EVENT_DATE BETWEEN add_months(last_day(add_months(concat(substr('###SLICE_VALUE###', 1, 7), '-01'), -1)), -3) AND last_day(add_months(concat(substr('###SLICE_VALUE###', 1, 7), '-01'), -1))) T_4,
(SELECT COUNT(distinct transaction_date) COUNT_5 from cdr.spark_it_zte_emergency_data where transaction_date between add_months('###SLICE_VALUE###', -3) AND '###SLICE_VALUE###') T_5,
(SELECT COUNT(distinct original_file_date) COUNT_6 from cdr.spark_it_zte_loan_cdr where original_file_date between add_months('###SLICE_VALUE###', -3) AND '###SLICE_VALUE###') T_6

