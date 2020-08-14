INSERT INTO MON.SPARK_FT_MSISDN_RECYCLE
SELECT MSISDN, PROD_STATE_DATE, PROD_STATE_NAME, OSP_ACCOUNT_TYPE, EST_PRESENT_OM, DUREE_INACTIVITE_OM, OM_BALANCE,
    CASE WHEN NVL(OM_BALANCE,0.0) >0 THEN 'FALSE' ELSE 'TRUE' END SOLDE_OM_NUL, RECYCLABLE,
    CASE WHEN PROD_STATE_NAME='Non-provisioning' and EST_PRESENT_OM='false' then 'NON PROVISIONING SANS OM'
        WHEN PROD_STATE_NAME='Non-provisioning' and EST_PRESENT_OM='true' then 'NON PROVISIONING AVEC OM'
        WHEN RECYCLABLE=1 AND PROD_STATE_NAME !='Termination' and EST_PRESENT_OM='false' then 'RECYCLABLE SANS OM'
        WHEN RECYCLABLE=1 AND PROD_STATE_NAME !='Termination' and EST_PRESENT_OM='true' then 'RECYCLABLE AVEC OM' END CATEGORIE,
    CURRENT_TIMESTAMP() INSERT_DATE, EVENT_DATE
FROM MON.SPARK_FT_MSISDN_RECYCLAGE A WHERE EVENT_DATE='###SLICE_VALUE###'
    AND upper(OSP_ACCOUNT_TYPE)='PREPAID'
    AND ( RECYCLABLE=1 AND PROD_STATE_NAME !='Termination' or PROD_STATE_NAME='Non-provisioning' )


SELECT MSISDN, PROD_STATE_DATE, PROD_STATE_NAME, OSP_ACCOUNT_TYPE, EST_PRESENT_OM, DUREE_INACTIVITE_OM, OM_BALANCE,
,
FROM MON.SPARK_FT_MSISDN_RECYCLAGE A WHERE EVENT_DATE='2020-05-27'
--and CASE WHEN NVL(OM_BALANCE,0.0) >0 THEN 'FALSE' ELSE 'TRUE' END !='TRUE'
    AND CASE WHEN PROD_STATE_NAME='Non-provisioning' then 'NON PROVISIONING' WHEN  PROD_STATE_NAME !='Termination' and EST_PRESENT_OM='false' then 'RECYCLABLE SANS OM'  WHEN PROD_STATE_NAME !='Termination' and EST_PRESENT_OM='true' then 'RECYCLABLE AVEC OM'
        END ='RECYCLABLE SANS OM' "

select prod_state_name, count(*) nbre_msisdn, count(case when recyclable=1 then msisdn end) nbre_recyclable
from mon.spark_ft_msisdn_recyclage a
LEFT JOIN (
    SELECT accnbr, prod_state, block_reason, order_reason, update_date, activation_date, iccid, imsi, customer_id, subscriber_type, default_price_plan_id, subs_id, row_number()over(partition by accnbr order by update_date desc, subs_id desc) rn_cont
    FROM CDR.SPARK_IT_CONT WHERE original_file_date ='2020-06-16'
) b ON a.msisdn = b.accnbr and a.prod_state_name = b.prod_state and to_date(a.prod_state_date) = to_date(b.update_date) and rn_cont =1
LEFT JOIN(
    SELECT DISTINCT ORIGINAL_FILE_DATE, CUSTID CUSTOMER_ID, GUID, CUSTOMER_PARENT_ID CUSTOMER_TYPE, CUSTSEG PRGCODE
    FROM CDR.SPARK_IT_CUST_FULL WHERE ORIGINAL_FILE_DATE = '2020-06-16'
) C ON C.CUSTOMER_ID = B.CUSTOMER_ID
WHERE EVENT_DATE='2020-06-16'
        AND OSP_ACCOUNT_TYPE='PrePaid' AND EST_PRESENT_ZEBRA='false' AND EST_PRESENT_OM='true'
        AND AGE_IN in ('J180', 'J360', 'JPLUS360')
        AND (PROD_STATE_NAME = 'One-Way Block' or (PROD_STATE_NAME = 'Two-Way Block' and recyclable=1))
        and NVL(C.CUSTOMER_TYPE,'UNK') !='C'
        and nvl(duree_inactivite_om,'INACTIF_PLUS_24MOIS')= 'INACTIF_PLUS_24MOIS'--INACTIF_ENTRE_06_24MOIS'
        --and NVL(OM_BALANCE,0.0) = 0.0
group by prod_state_name--, AGE_IN order by prod_state_name, age_in

select prod_state_name, count(*) nbre_msisdn, count(case when recyclable=1 then msisdn end) nbre_recyclable
from mon.spark_ft_msisdn_recyclage a
LEFT JOIN (
    SELECT accnbr, prod_state, block_reason, order_reason, update_date, activation_date, iccid, imsi, customer_id, subscriber_type, default_price_plan_id, subs_id, row_number()over(partition by accnbr order by update_date desc, subs_id desc) rn_cont
    FROM CDR.SPARK_IT_CONT WHERE original_file_date ='2020-06-16'
) b ON a.msisdn = b.accnbr and a.prod_state_name = b.prod_state and to_date(a.prod_state_date) = to_date(b.update_date) and rn_cont =1
LEFT JOIN(
    SELECT DISTINCT ORIGINAL_FILE_DATE, CUSTID CUSTOMER_ID, GUID, CUSTOMER_PARENT_ID CUSTOMER_TYPE, CUSTSEG PRGCODE
    FROM CDR.SPARK_IT_CUST_FULL WHERE ORIGINAL_FILE_DATE = '2020-06-16'
) C ON C.CUSTOMER_ID = B.CUSTOMER_ID
WHERE EVENT_DATE='2020-06-16'
        AND OSP_ACCOUNT_TYPE='PrePaid' AND EST_PRESENT_ZEBRA='false' AND EST_PRESENT_OM='true'
        AND AGE_IN in ('J360', 'JPLUS360')
        AND (PROD_STATE_NAME = 'One-Way Block' or (PROD_STATE_NAME = 'Two-Way Block' and recyclable=1))
        and NVL(C.CUSTOMER_TYPE,'UNK') !='C'
        and nvl(duree_inactivite_om,'INACTIF_PLUS_24MOIS') in ('INACTIF_ENTRE_06_24MOIS')--, 'INACTIF_PLUS_24MOIS')
        and NVL(OM_BALANCE,0.0) = 0.0
group by prod_state_name

1.	Base 1 : Prepaid sans compte OM, suspendu depuis 3 mois minimum
SELECT MSISDN, PROD_STATE_DATE, PROD_STATE_NAME, OSP_ACCOUNT_TYPE, EST_PRESENT_OM, nvl(duree_inactivite_om,'INACTIF_PLUS_24MOIS') DUREE_INACTIVITE_OM, OM_BALANCE, CASE WHEN NVL(OM_BALANCE,0.0) >0 THEN 'FALSE' ELSE 'TRUE' END SOLDE_OM_NUL
FROM MON.SPARK_FT_MSISDN_RECYCLAGE A --WHERE EVENT_DATE='2020-06-16'
LEFT JOIN (
    SELECT accnbr, prod_state, block_reason, order_reason, update_date, activation_date, iccid, imsi, customer_id, subscriber_type, default_price_plan_id, subs_id, row_number()over(partition by accnbr order by update_date desc, subs_id desc) rn_cont
    FROM CDR.SPARK_IT_CONT WHERE original_file_date ='2020-06-16'
) b ON a.msisdn = b.accnbr and a.prod_state_name = b.prod_state and to_date(a.prod_state_date) = to_date(b.update_date) and rn_cont =1
LEFT JOIN(
    SELECT DISTINCT ORIGINAL_FILE_DATE, CUSTID CUSTOMER_ID, GUID, CUSTOMER_PARENT_ID CUSTOMER_TYPE, CUSTSEG PRGCODE
    FROM CDR.SPARK_IT_CUST_FULL WHERE ORIGINAL_FILE_DATE = '2020-06-16'
) C ON C.CUSTOMER_ID = B.CUSTOMER_ID
WHERE EVENT_DATE='2020-06-16'
        AND OSP_ACCOUNT_TYPE='PrePaid' AND EST_PRESENT_ZEBRA='false' AND EST_PRESENT_OM='false'
        AND AGE_IN in ('J180', 'J360', 'JPLUS360')
        AND (PROD_STATE_NAME = 'One-Way Block' or (PROD_STATE_NAME = 'Two-Way Block' and recyclable=1))
        and NVL(C.CUSTOMER_TYPE,'UNK') !='C'

2.	Base 2 : Prepaid avec compte OM, inactif OM >24 mois et suspendu depuis 3 mois minimum

SELECT MSISDN, PROD_STATE_DATE, PROD_STATE_NAME, OSP_ACCOUNT_TYPE, EST_PRESENT_OM, nvl(duree_inactivite_om,'INACTIF_PLUS_24MOIS') DUREE_INACTIVITE_OM, OM_BALANCE, CASE WHEN NVL(OM_BALANCE,0.0) >0 THEN 'FALSE' ELSE 'TRUE' END SOLDE_OM_NUL
FROM MON.SPARK_FT_MSISDN_RECYCLAGE A --WHERE EVENT_DATE='2020-06-16'
LEFT JOIN (
    SELECT accnbr, prod_state, block_reason, order_reason, update_date, activation_date, iccid, imsi, customer_id, subscriber_type, default_price_plan_id, subs_id, row_number()over(partition by accnbr order by update_date desc, subs_id desc) rn_cont
    FROM CDR.SPARK_IT_CONT WHERE original_file_date ='2020-06-16'
) b ON a.msisdn = b.accnbr and a.prod_state_name = b.prod_state and to_date(a.prod_state_date) = to_date(b.update_date) and rn_cont =1
LEFT JOIN(
    SELECT DISTINCT ORIGINAL_FILE_DATE, CUSTID CUSTOMER_ID, GUID, CUSTOMER_PARENT_ID CUSTOMER_TYPE, CUSTSEG PRGCODE
    FROM CDR.SPARK_IT_CUST_FULL WHERE ORIGINAL_FILE_DATE = '2020-06-16'
) C ON C.CUSTOMER_ID = B.CUSTOMER_ID
WHERE EVENT_DATE='2020-06-16'
        AND OSP_ACCOUNT_TYPE='PrePaid' AND EST_PRESENT_ZEBRA='false' AND EST_PRESENT_OM='true'
        AND AGE_IN in ('J180', 'J360', 'JPLUS360')
        AND (PROD_STATE_NAME = 'One-Way Block' or (PROD_STATE_NAME = 'Two-Way Block' and recyclable=1))
        and NVL(C.CUSTOMER_TYPE,'UNK') !='C'
        and nvl(duree_inactivite_om,'INACTIF_PLUS_24MOIS')= 'INACTIF_PLUS_24MOIS'

3.	Base 3 : Prepaid avec compte OM, solde OM nul et suspendu depuis 6 mois minimum

SELECT MSISDN, PROD_STATE_DATE, PROD_STATE_NAME, OSP_ACCOUNT_TYPE, EST_PRESENT_OM, nvl(duree_inactivite_om,'INACTIF_PLUS_24MOIS') DUREE_INACTIVITE_OM, OM_BALANCE, CASE WHEN NVL(OM_BALANCE,0.0) >0 THEN 'FALSE' ELSE 'TRUE' END SOLDE_OM_NUL
FROM MON.SPARK_FT_MSISDN_RECYCLAGE A --WHERE EVENT_DATE='2020-06-16'
LEFT JOIN (
    SELECT accnbr, prod_state, block_reason, order_reason, update_date, activation_date, iccid, imsi, customer_id, subscriber_type, default_price_plan_id, subs_id, row_number()over(partition by accnbr order by update_date desc, subs_id desc) rn_cont
    FROM CDR.SPARK_IT_CONT WHERE original_file_date ='2020-06-16'
) b ON a.msisdn = b.accnbr and a.prod_state_name = b.prod_state and to_date(a.prod_state_date) = to_date(b.update_date) and rn_cont =1
LEFT JOIN(
    SELECT DISTINCT ORIGINAL_FILE_DATE, CUSTID CUSTOMER_ID, GUID, CUSTOMER_PARENT_ID CUSTOMER_TYPE, CUSTSEG PRGCODE
    FROM CDR.SPARK_IT_CUST_FULL WHERE ORIGINAL_FILE_DATE = '2020-06-16'
) C ON C.CUSTOMER_ID = B.CUSTOMER_ID
WHERE EVENT_DATE='2020-06-16'
        AND OSP_ACCOUNT_TYPE='PrePaid' AND EST_PRESENT_ZEBRA='false' AND EST_PRESENT_OM='true'
        AND AGE_IN in ('J360', 'JPLUS360')
        AND (PROD_STATE_NAME = 'One-Way Block' or (PROD_STATE_NAME = 'Two-Way Block' and recyclable=1))
        and NVL(C.CUSTOMER_TYPE,'UNK') !='C'
        and nvl(duree_inactivite_om,'INACTIF_PLUS_24MOIS') in ('INACTIF_ENTRE_06_24MOIS')--, 'INACTIF_PLUS_24MOIS')
        and NVL(OM_BALANCE,0.0) = 0.0
