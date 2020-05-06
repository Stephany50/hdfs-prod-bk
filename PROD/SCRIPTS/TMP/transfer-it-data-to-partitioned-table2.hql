add jar hdfs:///PROD/UDF/hive-udf-1.0.jar;
CREATE TEMPORARY FUNCTION FN_GET_NNP_MSISDN_SIMPLE_DESTN as 'cm.orange.bigdata.udf.GetNnpMsisdnSimpleDestn';
--INSERT INTO MON.SPARK_FT_MSC_CMR_BAD_CALL
select *
from
(SELECT * FROM MON.SPARK_FT_MSC_TRANSACTION
WHERE transaction_date = "2020-04-18"
and transaction_type <> 'TEL_CFW'
and substr(transaction_type, 1, 3) = 'TEL'
and other_party not like '160%') a
left join
(SELECT cc,ncc,cc||ncc code_operateur,length(cc||ncc) taille_co  FROM dim.dt_Ref_Operateurs) d
on  substring(if(nvl(a.other_party,'')='','ND',a.other_party),1,taille_co  ) <> code_operateur
INNER JOIN
(SELECT * FROM DIM.SPARK_DT_REF_OPERATEURS
WHERE NVL(prefix_trunck, '0') <> '0' and country_name = 'CAMEROON' ) b
ON substr(a.trunck_in, 1, 5) = b.prefix_trunck
where
NOT (substr(NVL(a.other_party, 0), 1, 2) IN
(select distinct substr(ncc, 1, 2) from DIM.SPARK_DT_REF_OPERATEURS
where b.prefix_trunck = substr(a.trunck_in, 1, 5))
and length(NVL(a.other_party, 0)) = 9)
,