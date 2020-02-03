
SELECT IF(T.insert_date<T1.insert_date , 'OK', 'NOK' )
FROM
(select NVL(MAX(insert_date),'2019-01-01 00:00:00') insert_date from MON.SPARK_14_FT_DAILY_STATUS where EVENT_DATE = DATE_SUB(CURRENT_DATE,1) ) T,
(select NVL(MAX(insert_date),'2019-01-01 00:00:01') insert_date from MON.SPARK_FT_DAILY_STATUS where TABLE_DATE between  CURRENT_DATE and DATE_SUB(CURRENT_DATE,15) ) T1



