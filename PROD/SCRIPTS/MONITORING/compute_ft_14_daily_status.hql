
INSERT INTO MON.SPARK_14_FT_DAILY_STATUS
select
T15.table_name table_name,
T15.NB_ROWS jour15,
T14.NB_ROWS jour14,
T13.NB_ROWS jour13,
T12.NB_ROWS jour12,
T11.NB_ROWS jour11,
T10.NB_ROWS jour10,
T09.NB_ROWS jour09,
T08.NB_ROWS jour08,
T07.NB_ROWS jour07,
T06.NB_ROWS jour06,
T05.NB_ROWS jour05,
T04.NB_ROWS jour04,
T03.NB_ROWS jour03,
T02.NB_ROWS jour02,
T01.NB_ROWS jour01,
 CURRENT_TIMESTAMP INSERT_DATE,
CURRENT_DATE EVENT_DATE
FROM
(select table_name, NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =CURRENT_DATE) T15
LEFT JOIN (select table_name, NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,1)) T14 ON T15.table_name=T14.table_name
LEFT JOIN (select table_name, NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,2)) T13 ON T15.table_name=T13.table_name
LEFT JOIN (select table_name, NB_ROWS ,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,3)) T12 ON T15.table_name=T12.table_name
LEFT JOIN (select table_name, NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,4)) T11 ON T15.table_name=T11.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,5)) T10 ON T15.table_name=T10.table_name
LEFT JOIN (select table_name, NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,6)) T09 ON T15.table_name=T09.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,7)) T08 ON T15.table_name=T08.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,8)) T07 ON T15.table_name=T07.table_name
LEFT JOIN (select table_name, NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,9)) T06 ON T15.table_name=T06.table_name
LEFT JOIN (select table_name, NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,10)) T05 ON T15.table_name=T05.table_name
LEFT JOIN (select table_name, NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,11)) T04 ON T15.table_name=T04.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,12)) T03 ON T15.table_name=T03.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,13)) T02 ON T15.table_name=T02.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,14)) T01  ON T15.table_name=T01.table_name

GROUP BY T15.table_name,T15.NB_ROWS,T14.NB_ROWS,T13.NB_ROWS,T12.NB_ROWS,T11.NB_ROWS,T10.NB_ROWS,T09.NB_ROWS,T08.NB_ROWS,T07.NB_ROWS,T06.NB_ROWS,T05.NB_ROWS,T04.NB_ROWS,T03.NB_ROWS,T02.NB_ROWS,T01.NB_ROWS