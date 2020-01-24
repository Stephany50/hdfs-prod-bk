
INSERT INTO MON.SPARK_14_FT_DAILY_STATUS
select
T14.table_name table_name,
T14.statut jour14,
T13.statut jour13,
T12.statut jour12,
T11.statut jour11,
T10.statut jour10,
T09.statut jour09,
T08.statut jour08,
T07.statut jour07,
T06.statut jour06,
T05.statut jour05,
T04.statut jour04,
T03.statut jour03,
T02.statut jour02,
T01.statut jour01,
 CURRENT_TIMESTAMP INSERT_DATE,
CURRENT_DATE EVENT_DATE
FROM (
   select table_name, statut,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,1)
) T14 
LEFT JOIN (select table_name, statut,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,2)) T13 ON T14.table_name=T13.table_name
LEFT JOIN (select table_name, statut ,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,3)) T12 ON T13.table_name=T12.table_name
LEFT JOIN (select table_name, statut,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,4)) T11 ON T12.table_name=T11.table_name
LEFT JOIN (select table_name,  statut,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,5)) T10 ON T11.table_name=T10.table_name
LEFT JOIN (select table_name, statut,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,6)) T09 ON T10.table_name=T09.table_name
LEFT JOIN (select table_name,  statut,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,7)) T08 ON T09.table_name=T08.table_name
LEFT JOIN (select table_name,  statut,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,8)) T07 ON T08.table_name=T07.table_name
LEFT JOIN (select table_name, statut,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,9)) T06 ON T07.table_name=T06.table_name
LEFT JOIN (select table_name, statut,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,10)) T05 ON T06.table_name=T05.table_name
LEFT JOIN (select table_name, statut,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,11)) T04 ON T05.table_name=T04.table_name
LEFT JOIN (select table_name,  statut,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,12)) T03 ON T04.table_name=T03.table_name
LEFT JOIN (select table_name,  statut,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,13)) T02 ON T03.table_name=T02.table_name
LEFT JOIN (select table_name,  statut,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,14)) T01  ON T02.table_name=T01.table_name

GROUP BY T14.table_name,T14.statut,T13.statut,T12.statut,T11.statut,T10.statut,T09.statut,T08.statut,T07.statut,T06.statut,T05.statut,T04.statut,T03.statut,T02.statut,T01.statut