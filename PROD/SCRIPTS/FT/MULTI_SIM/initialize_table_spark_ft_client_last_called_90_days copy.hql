INSERT INTO MON.SPARK_FT_CLIENT_LAST_CALLED_90_DAYS
add jar hdfs:///PROD/UDF/hive-udf-1.0.1.jar;
CREATE TEMPORARY FUNCTION FN_NNP_SIMPLE_DESTINATION as 'cm.orange.bigdata.udf.GetNnpSimpleDestn';
create table tmp.msisdn_calls_list_5
select * from tmp.msisdn_calls_list_4 where WHERE FN_NNP_SIMPLE_DESTINATION(CALLER)='OCM';

create table tmp.msisdn_calls_list_6
SELECT
    A.CALLER,
    CONCAT_WS('|', COLLECT_SET(A.CALLEE)) CALLEE_LIST,
    COUNT(DISTINCT A.CALLEE) NBR_CALLEES,
    MAX(SITE_NAME) SITE_NAME
FROM
(
SELECT
    (
        CASE WHEN UPPER(TRANSACTION_DIRECTION)='SORTANT' THEN SERVED_MSISDN
        ELSE OTHER_PARTY
        END
    ) CALLER,
    (
        CASE WHEN UPPER(TRANSACTION_DIRECTION)='ENTRANT' THEN SERVED_MSISDN
        ELSE OTHER_PARTY
        END
    ) CALLEE
    --TRANSACTION_DATE LAST_CALL_DATE,
    --CURRENT_TIMESTAMP() INSERT_DATE,
    --TRANSACTION_DATE EVENT_DATE
FROM MON.SPARK_FT_MSC_TRANSACTION
WHERE TRANSACTION_DATE BETWEEN DATE_SUB(CURRENT_DATE, 91) AND DATE_SUB(CURRENT_DATE, 1) 
    AND LENGTH(SERVED_MSISDN)=9 
    AND LENGTH(OTHER_PARTY)=9 
    AND UPPER(TRANSACTION_TYPE) LIKE "TEL%"
GROUP BY --TRANSACTION_DATE,
    (
        CASE WHEN UPPER(TRANSACTION_DIRECTION)='SORTANT' THEN SERVED_MSISDN
        ELSE OTHER_PARTY
        END
    ),
    (
        CASE WHEN UPPER(TRANSACTION_DIRECTION)='ENTRANT' THEN SERVED_MSISDN
        ELSE OTHER_PARTY
        END
    )
) A 
LEFT JOIN
(SELECT MSISDN, MAX(SITE_NAME) SITE_NAME FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE='2022-03-29' GROUP BY MSISDN) B
on a.caller=b.msisdn
WHERE FN_NNP_SIMPLE_DESTINATION(CALLER)='OCM'
GROUP BY CALLER

create table tmp.msisdn_calls_list_2
SEL



create table tmp.msisdn_calls_1

create table tmp.msisdn_calls_list
SELECT
    CALLER,
    CONCAT_WS('|', COLLECT_SET(CALLEE)) CALLEE_LIST
FROM
(
SELECT
    SERVED_MSISDN CALLER,
    OTHER_PARTY CALLEE
    --TRANSACTION_DATE LAST_CALL_DATE,
    --CURRENT_TIMESTAMP() INSERT_DATE
    --TRANSACTION_DATE EVENT_DATE
FROM MON.SPARK_FT_MSC_TRANSACTION
WHERE TRANSACTION_DATE ='2022-03-26' --BETWEEN DATE_SUB(CURRENT_DATE, 91) AND DATE_SUB(CURRENT_DATE, 1) 
    AND LENGTH(SERVED_MSISDN)=9 
    AND LENGTH(OTHER_PARTY)=9 
    AND UPPER(TRANSACTION_TYPE) LIKE "TEL%"
    AND UPPER(TRANSACTION_DIRECTION)="SORTANT") A
GROUP BY --TRANSACTION_DATE,
   CALLER

create table tmp.msisdn_calls_list_localised
select B.SITE_NAME, A.* FROM
(SELECT * FROM tmp.msisdn_calls_list) A
LEFT JOIN 
(SELECT MSISDN, SITE_NAME FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE='2022-03-25') B
on a.caller=b.msisdn


add jar hdfs:///DATALAB/UDF/hive-udf-1.0.1.jar;
CREATE OR REPLACE TEMPORARY FUNCTION FN_MULTISIM_SCORE2 as 'cm.orange.bigdata.udf.MultiSimScore';

insert into tmp.multisim
select
a.caller caller1,
b.caller caller2,
a.callee_list callee_list1,
b.callee_list callee_list2,
fn_multisim_score2(nvl(a.caller, ''), nvl(b.caller, ''), nvl(a.callee_list, ''), nvl(b.callee_list, '')) score
from
(select * from tmp.msisdn_calls_list_6 where SITE_NAME is not null and nbr_callees>=3 and nbr_callees<=150) a
INNER join
(select * from tmp.msisdn_calls_list_6 where SITE_NAME IS NOT null and nbr_callees>=3 and nbr_callees<=150) b
on 
a.site_name=b.SITE_NAME and a.caller<>b.caller
WHERE fn_multisim_score2(nvl(a.caller, ''), nvl(b.caller, ''), nvl(a.callee_list, ''), nvl(b.callee_list, '')) >= 0.7;

add jar hdfs:///DATALAB/UDF/hive-udf-1.0.1.jar;
CREATE OR REPLACE TEMPORARY FUNCTION FN_MULTISIM_SCORE as 'cm.orange.bigdata.udf.MultiSimScore';
select
a.caller caller1,
b.caller caller2,
a.callee_list callee_list1,
b.callee_list callee_list2,
fn_multisim_score(a.caller, b.caller, a.callee_list, b.callee_list) score
from
(select * from tmp.msisdn_calls_list_localised where caller is not null and callee_list is not null) a
left join
(select * from tmp.msisdn_calls_list_localised where caller is not null and callee_list is not null) b
on 
a.site_name=b.site_name and a.caller<>b.caller
where fn_multisim_score(a.caller, b.caller, a.callee_list, b.callee_list) > 0 
--and fn_multisim_score(a.caller, b.caller, a.callee_list, b.callee_list) < 1
 limit 100;


 MSISDN_1
 MSISDN_2
 SCORE