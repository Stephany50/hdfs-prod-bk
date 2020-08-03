insert into MON.spark_ft_crm_output_orange_money
select
upper(fname) NOM,
lname PRENOM,
c.compte_b2c CODE_CLIENT,
1 SERVICE_ORANGE_MONEY,
c.ID SOUSCRIPTION_MSISDN_OM,
omny.ACCOUNT_NUMBER MSISDN,
current_timestamp() AS INSERT_DATE,
'###SLICE_VALUE###' AS EVENT_DATE
from (select ACCOUNT_NUMBER , max(case when USER_FIRST_NAME = 'N/A' then ' ' else USER_FIRST_NAME end) fname
,max(case when USER_LAST_NAME='N/A' then ' ' else USER_LAST_NAME end)lname
from MON.spark_ft_omny_account_snapshot
where EVENT_DATE = DATE_SUB(to_date('###SLICE_VALUE###'),1)
group by ACCOUNT_NUMBER) omny
join
(SELECT compte_b2c,msisdn_idwimax msisdn,MAX(id) id
FROM CDR.SPARK_IT_CRM_ABONNEMENTS
WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
GROUP BY compte_b2c,msisdn_idwimax) c
on trim(omny.ACCOUNT_NUMBER) = trim(c.msisdn)