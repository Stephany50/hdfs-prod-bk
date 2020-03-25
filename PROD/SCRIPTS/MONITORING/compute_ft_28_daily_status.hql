INSERT INTO MON.SPARK_28_FT_DAILY_STATUS
select
T_16.table_name table_name,
T_16.NB_ROWS jour_16,
T_17.NB_ROWS jour_17,
T_18.NB_ROWS jour_18,
T_19.NB_ROWS jour_19,
T_20.NB_ROWS jour_20,
T_21.NB_ROWS jour_21,
T_22.NB_ROWS jour_22,
T_23.NB_ROWS jour_23,
T_24.NB_ROWS jour_24,
T_25.NB_ROWS jour_25,
T_26.NB_ROWS jour_26,
T_27.NB_ROWS jour_27,
T_28.NB_ROWS jour_28,
T_29.NB_ROWS jour_29,
 CURRENT_TIMESTAMP INSERT_DATE,
DATE_SUB(CURRENT_DATE,1) EVENT_DATE
FROM

(select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,15)) T_16
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,16)) T_17  ON T_16.table_name=T_17.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,17)) T_18  ON T_17.table_name=T_18.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,18)) T_19  ON T_18.table_name=T_19.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,19)) T_20  ON T_19.table_name=T_20.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,20)) T_21  ON T_20.table_name=T_21.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,21)) T_22  ON T_21.table_name=T_22.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,22)) T_23  ON T_22.table_name=T_23.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,23)) T_24  ON T_23.table_name=T_24.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,24)) T_25  ON T_24.table_name=T_25.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,25)) T_26  ON T_25.table_name=T_26.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,26)) T_27  ON T_26.table_name=T_27.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,27)) T_28  ON T_27.table_name=T_28.table_name
LEFT JOIN (select table_name,  NB_ROWS,table_date from mon.SPARK_FT_DAILY_STATUS where table_date =DATE_SUB(CURRENT_DATE,28)) T_29  ON T_28.table_name=T_29.table_name
GROUP BY T_16.table_name ,T_16.NB_ROWS ,T_17.NB_ROWS,T_18.NB_ROWS,T_19.NB_ROWS,T_20.NB_ROWS,T_21.NB_ROWS,T_22.NB_ROWS,T_23.NB_ROWS,T_24.NB_ROWS,T_25.NB_ROWS,T_26.NB_ROWS,T_27.NB_ROWS,T_28.NB_ROWS,T_29.NB_ROWS