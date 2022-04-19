--Top 500 des identificateurs ayant rapporter le plus d'anomalies dans la BDI par jours.
insert into AGG.SPARK_FT_KYC_DASHBOARD_INDENTIFICATEUR
SELECT *,current_timestamp() AS insert_date,'###SLICE_VALUE###' AS EVENT_DATE FROM
