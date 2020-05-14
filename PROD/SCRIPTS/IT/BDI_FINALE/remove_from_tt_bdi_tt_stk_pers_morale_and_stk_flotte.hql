INSERT INTO TMP.tt_bdi2
select c.*
from (
SELECT a.* FROM
(SELECT * FROM TMP.tt_bdi1) a
LEFT JOIN
(SELECT * FROM CDR.spark_it_bdi_stk_pers_morale where original_file_date='2020-03-13' ) b  
on substr(trim(a.msisdn),-9,9) = substr(trim(b.msisdn),-9,9)
WHERE b.msisdn is null) c
left join
(SELECT * FROM CDR.SPARK_IT_BDI_STK_LIGNE_FLOTTE where originale_file_date = '2020-03-13') d
on  substr(trim(c.msisdn),-9,9) = substr(trim(d.msisdn),-9,9)
where  d.msisdn is null