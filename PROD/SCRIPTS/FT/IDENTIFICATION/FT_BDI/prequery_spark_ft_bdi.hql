SELECT IF(FT_BDI.NB_FT_BDI = 0 AND IT_BDI.NB_IT_BDI >= 10) PRE_REQ
FROM
(select count(distinct msisdn) AS NB_FT_BDI from  MON.SPARK_FT_BDI WHERE EVENT_DATE = '###SLICE_VALUE###') FT_BDI,
(select count(distinct msisdn) AS NB_IT_BDI from  CDR.SPARK_IT_BDI,
    WHERE ORIGINAL_FILE_DATE = date_add(to_date(from_unixtime(UNIX_TIMESTAMP('###SLICE_VALUE###','ddMMyyyy'))),1)) IT_BDI
(select count(distinct msisdn) AS NB_DATA_CONSO from  MON.SPARK_FT_DATA_CONSO_MSISDN_MONTH
    WHERE EVENT_MONTH = date_add(to_date(from_unixtime(UNIX_TIMESTAMP('###SLICE_VALUE###','ddMMyyyy'))),1)) IT_BDI


dim.spark_dt_gsm_cell_code
dim.spark_dt_bdi_derogation
mon.spark_ft_contract_snapshot

mon.spark_ft_client_last_site_day
dim.spark_dt_ref_segmentation_client

select date_add(to_date(from_unixtime(UNIX_TIMESTAMP('18032020 15:25:25','ddMMyyyy'))),1);




