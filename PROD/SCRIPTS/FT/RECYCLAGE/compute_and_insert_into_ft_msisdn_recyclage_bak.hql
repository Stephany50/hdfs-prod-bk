cdr.it_account
mon.ft_omny_account_snapshot
cdr.it_zebra_master

SELECT msisdn, count(distinct prod_state)
FROM tmp.tt_msisdn_recyclage
WHERE event_date = date_add('2020-01-19', 1) and length(msisdn)=9
group by msisdn
having count( prod_state)>1 limit 10

 SELECT event_date, CASE WHEN prod_state = 'G' THEN 'Inactive' WHEN prod_state = 'A' THEN 'Active' WHEN prod_state= 'D' THEN 'One-Way Block'
    WHEN prod_state = 'E' THEN 'Two-Way Block' WHEN prod_state = 'B' THEN 'Termination' WHEN prod_state = 'H' THEN 'Non-provisioning'
    WHEN prod_state = 'Y' THEN 'UnPassIdentity' WHEN prod_state = 'L' THEN 'Subscriber Locked' END prod_state_name,
    osp_status, count(*) nbre_total, count(distinct msisdn) nbre_msisdn
FROM tmp.tt_msisdn_recyclage
WHERE event_date = '2020-01-20' and length(msisdn)=9
group by event_date, prod_state, osp_status
limit 5000



insert into tmp.tt_msisdn_recyclage

create table tmp.tt_msisdn_recyclage2 as
SELECT distinct '2020-01-19' event_date, acc_nbr msisdn, subs_id, acct_id, price_plan_id, prod_state_date, block_reason, prod_state,
    CASE WHEN prod_state = 'G' THEN 'Inactive' WHEN prod_state = 'A' THEN 'Active' WHEN prod_state = 'D' THEN 'One-Way Block'
        WHEN prod_state = 'E' THEN 'Two-Way Block' WHEN prod_state = 'B' THEN 'Termination' WHEN prod_state = 'H' THEN 'Non-provisioning'
        WHEN prod_state = 'Y' THEN 'UnPassIdentity' WHEN prod_state = 'L' THEN 'Subscriber Locked' END prod_state_name,
    a.update_date, prod_spec_id, access_key, COMPLETED_DATE activation_date, deactivation_date, commercial_offer, osp_account_type, provisioning_date, osp_status, main_credit,
    CASE WHEN d.msisdn is not null THEN true else false END est_present_om, d.registered_date_om registered_date_om, id_number, d.user_type, creation_date creation_date_om, account_balance om_balance,
    CASE WHEN e.primary_msisdn is not null THEN true else false END est_present_zebra, USER_STATUS statut_zebra, current_timestamp insert_date
FROM(SELECT *, row_number() OVER (PARTITION BY acc_nbr ORDER BY UPDATE_DATE DESC) rn FROM cdr.spark_it_zte_subs_extract WHERE original_file_date = date_add('2020-01-19', 1) )a
left join cdr.spark_it_zte_prod_extract b on b.original_file_date >= date_add('2020-01-19', 1) and a.original_file_date = b.original_file_date  and b.prod_id = a.subs_id
left join mon.spark_ft_contract_snapshot c on c.event_date >= date_add('2020-01-19', 1) and a.original_file_date = c.event_date and c.access_key = a.acc_nbr
left join ( SELECT d.*, row_number() OVER (PARTITION BY msisdn ORDER BY modified_on desc, registered_on desc, account_balance DESC) rn_om, first_value(registered_on) OVER (PARTITION BY msisdn ORDER BY registered_on asc) registered_date_om
    FROM mon.ft_omny_account_snapshot d WHERE d.event_date >= '2020-01-19' ) d on date_sub(a.original_file_date,1)=d.event_date and a.acc_nbr = d.msisdn and rn_om=1
left join (SELECT e.*,  row_number() over (partition by primary_msisdn order by CASE WHEN USER_STATUS='Active' THEN 1
                WHEN USER_STATUS='SuspEND Request' THEN 2  WHEN USER_STATUS='SuspENDed' THEN 3 WHEN USER_STATUS='Removed' THEN 5
                WHEN USER_STATUS='New'	THEN 1.1 WHEN USER_STATUS='Delete Request' THEN 4 END asc, channel_user_id desc) status_order FROM cdr.spark_it_zebra_master e WHERE e.transaction_date ='2020-01-19' ) on status_order=1 and date_sub(a.original_file_date, 1) = e.transaction_date and e.primary_msisdn = a.acc_nbr
WHERE rn=1 and length(acc_nbr)=9



SELECT distinct to_date('25/01/2020') event_date, acc_nbr msisdn, subs_id, acct_id, price_plan_id, prod_state_date, block_reason, prod_state, a.update_date, prod_spec_id, access_key,
    COMPLETED_DATE activation_date, deactivation_date, commercial_offer, osp_account_type, provisioning_date, osp_status, main_credit,
    CASE WHEN d.msisdn is not null THEN 'true' else 'false' END est_present_om, d.registered_date_om registered_date_om, id_number, d.user_type, creation_date creation_date_om, account_balance om_balance, last_transac_om_date,
    CASE WHEN e.primary_msisdn is not null THEN 'true' else 'false' END est_present_zebra, user_status statut_zebra, last_tranfer_date last_transac_zebra_date, current_timestamp insert_date
FROM ( SELECT a.*, row_number() OVER (PARTITION BY acc_nbr ORDER BY UPDATE_DATE DESC) rn FROM cdr.it_zte_subs_extract a WHERE original_file_date = to_date('25/01/2020')+1 )a
left join cdr.it_zte_prod_extract b on b.original_file_date = to_date('25/01/2020')+1 and a.original_file_date = b.original_file_date  and b.prod_id = a.subs_id
left join mon.ft_contract_snapshot c on c.event_date = to_date('25/01/2020')+1 and a.original_file_date = c.event_date and c.access_key = a.acc_nbr
left join ( SELECT d.*, row_number() OVER (PARTITION BY msisdn ORDER BY modified_on desc, registered_on desc, account_balance DESC) rn_om, first_value(registered_on) OVER (PARTITION BY msisdn ORDER BY registered_on asc) registered_date_om
    FROM tango_mon.ft_omny_account_snapshot d WHERE d.event_date = to_date('25/01/2020') ) d on rn_om=1 and a.original_file_date-1 = d.event_date  and a.acc_nbr = d.msisdn
left join ( SELECT e.*,  row_number() over (partition by primary_msisdn order by CASE WHEN USER_STATUS='Active' THEN 1
     WHEN USER_STATUS='SuspEND Request' THEN 2  WHEN USER_STATUS='SuspENDed' THEN 3 WHEN USER_STATUS='Removed' THEN 5
     WHEN USER_STATUS='New'	THEN 1.1 WHEN USER_STATUS='Delete Request' THEN 4 END asc, channel_user_id desc) status_order FROM cdr.it_zebra_master e WHERE e.GENERATED_DATE = to_date('25/01/2020') ) e on  status_order=1 and a.original_file_date-1 = GENERATED_DATE and e.primary_msisdn = a.acc_nbr
left join tt_zebra_active_user2 f on a.acc_nbr = f.sENDer_msisdn and f.event_date = '27/01/2020' -- and  a.original_file_date-1 = f.event_date
left join ( SELECT to_date('25/01/2020') event_date, msisdn, max(last_transaction_date_time) last_transac_om_date
FROM tango_mon.ft_om_active_user WHERE event_date in (to_date('25/01/2020')-59,to_date('25/01/2020')-29, to_date('25/01/2020'))
group by msisdn ) g on a.acc_nbr=g.msisdn
WHERE rn=1 and length(acc_nbr)=9

SELECT main_msisdn, contract_id, main_imsi, contract_type, status_date, a.account_status, provisionning_date, activation_date, deactivation_date, event_date date_photo_om, msisdn, registered_on, id_number, b.account_status statut_oom, user_type, creation_date, account_balance
FROM cdr.it_account a
left join mon.ft_omny_account_snapshot b on b.event_date = '2020-01-02' and a.main_msisdn = b.msisdn
WHERE a.original_file_date = '2020-01-02'


SELECT *
FROM cdr.it_account a
WHERE original_file_date = '2020-01-04' --and activation_date is not null
limit 5;
SELECT distinct account_status -- event_date, msisdn, registered_on, id_number, account_status, user_type, creation_date, account_balance
FROM mon.ft_omny_account_snapshot b
WHERE event_date = date_add('2020-01-19', 1)
    and msisdn = '693936553'

SELECT *
FROM cdr.it_zebra_master
WHERE  original_file_date=date_add('2020-01-19', 1)
limit 5;

SELECT *
FROM cdr.it_zebra_master b
limit 5;

create table tmp.test_recyclage3;
insert into tmp.test_recyclage3
SELECT MSISDN, STATUS_DATE, PROD_STATE_DATE, PROD_STATE_NAME, EST_PRESENT_ZEBRA, EST_PRESENT_OM, co.customer_id custid, b.*, c.*, co.update_date, RECYCLABLE
FROM MON.SPARK_FT_MSISDN_RECYCLAGE A
LEFT JOIN (
    SELECT accnbr, prod_state, block_reason, order_reason, update_date, activation_date, iccid, imsi, customer_id, subscriber_type, default_price_plan_id, subs_id, row_number()over(partition by accnbr order by update_date desc, subs_id desc) rn_cont
    FROM CDR.SPARK_IT_CONT WHERE original_file_date ='2020-04-22'
) co ON a.msisdn = co.accnbr and a.prod_state_name = co.prod_state and to_date(a.prod_state_date) = to_date(co.update_date) and rn_cont =1
LEFT JOIN (
    SELECT CONTRACT_ID, MAIN_MSISDN, MAIN_IMSI, CONTRACT_TYPE, ACCOUNT_STATUS, STATUS_DATE, COUNT(MAIN_MSISDN) OVER(PARTITION BY MAIN_MSISDN ) NBRE_OCCUR,
        ROW_NUMBER() OVER(PARTITION BY MAIN_MSISDN ORDER BY CAST(CONTRACT_ID AS INT) DESC, NVL(STATUS_DATE, CURRENT_DATE) DESC)RN
    FROM CDR.SPARK_IT_ACCOUNT WHERE ORIGINAL_FILE_DATE='2020-04-22'
) B ON A.MSISDN= B.MAIN_MSISDN AND RN=1
LEFT JOIN(
    SELECT DISTINCT ORIGINAL_FILE_DATE, CUSTID CUSTOMER_ID, GUID, CUSTOMER_PARENT_ID CUSTOMER_TYPE, CUSTSEG PRGCODE
    FROM CDR.SPARK_IT_CUST_FULL WHERE ORIGINAL_FILE_DATE = '2020-04-22'
) C ON C.CUSTOMER_ID = co.CUSTOMER_ID
WHERE EVENT_DATE='2020-04-22'  AND RECYCLABLE=1  AND EST_PRESENT_ZEBRA='false' AND EST_PRESENT_OM='false' AND PROD_STATE_NAME !='Termination'

create table tmp.tt_msisdn_a_recycler as
insert into tmp.tt_msisdn_a_recycler
SELECT MSISDN, PROD_STATE_DATE, PROD_STATE_NAME, OSP_ACCOUNT_TYPE, EST_PRESENT_OM, DUREE_INACTIVITE_OM, OM_BALANCE,
    CASE WHEN NVL(OM_BALANCE,0.0) >0 THEN 'FALSE' ELSE 'TRUE' END SOLDE_OM_NUL,
    CASE WHEN RECYCLABLE=1 AND PROD_STATE_NAME !='Termination' and EST_PRESENT_OM='false' then 'RECYCLABLE SANS OM'
        WHEN RECYCLABLE=1 AND PROD_STATE_NAME !='Termination' and EST_PRESENT_OM='true' then 'RECYCLABLE AVEC OM'
        WHEN PROD_STATE_NAME='Non-provisioning' then 'NON PROVISIONING' END CATEGORIE, RECYCLABLE, CURRENT_TIMESTAMP() INSERT_DATE, EVENT_DATE
FROM MON.SPARK_FT_MSISDN_RECYCLAGE A WHERE EVENT_DATE='2020-05-04' and upper(OSP_ACCOUNT_TYPE)='PREPAID'
    AND ( RECYCLABLE=1 AND PROD_STATE_NAME !='Termination' or PROD_STATE_NAME='Non-provisioning' )


hive --outputFormat=csv2 -e "
select msisdn, prod_state_date, prod_state_name, OSP_ACCOUNT_TYPE FROM MON.SPARK_FT_MSISDN_RECYCLAGE A
WHERE EVENT_DATE='2020-05-04'  AND RECYCLABLE=1 AND PROD_STATE_NAME !='Termination' and OSP_ACCOUNT_TYPE='Prepaid' and EST_PRESENT_OM='false' " > msisdn_recyclage_non_OM_Zebra_20200504.csv &

----------------- BON -------------------
INSERT INTO TMP.TT_MSISDN_RECYCLAGE
SELECT distinct TO_DATE('###SLICE_VALUE###') EVENT_DATE, acc_nbr MSISDN, SUBS_ID, A.IMSI, ACCT_ID, PRICE_PLAN_ID, PROD_STATE_DATE, DATEDIFF(TO_DATE('###SLICE_VALUE###'), prod_state_date) AGE, BLOCK_REASON, PROD_STATE,
CASE WHEN b.prod_state = 'G' THEN 'Inactive' WHEN b.prod_state = 'A' THEN 'Active' WHEN b.prod_state = 'D' THEN 'One-Way Block'
        WHEN b.prod_state = 'E' THEN 'Two-Way Block' WHEN b.prod_state = 'B' THEN 'Termination' WHEN b.prod_state = 'H' THEN 'Non-provisioning'
        WHEN b.prod_state = 'Y' THEN 'UnPassIdentity' WHEN b.prod_state = 'L' THEN 'Subscriber Locked' END PROD_STATE_NAME, A.UPDATE_DATE, PROD_SPEC_ID, ACCESS_KEY,
    COMPLETED_DATE ACTIVATION_DATE, DEACTIVATION_DATE, COMMERCIAL_OFFER, h.CONTRACT_TYPE OSP_ACCOUNT_TYPE, PROVISIONING_DATE, OSP_STATUS, MAIN_CREDIT,
    CASE WHEN d.msisdn is not null or LAST_TRANSAC_OM_DATE  is not null THEN 'true'  else 'false' END EST_PRESENT_OM, d.registered_date_om REGISTERED_DATE_OM, ID_NUMBER, d.USER_TYPE, creation_date CREATION_DATE_OM, account_balance OM_BALANCE, LAST_TRANSAC_OM_DATE,
    CASE WHEN e.primary_msisdn is not null or last_transfer_date is not null THEN 'true' else 'false' END EST_PRESENT_ZEBRA, user_status STATUT_ZEBRA, last_transfer_date LAST_TRANSAC_ZEBRA_DATE,
    CAST(null as String) ORDER_REASON, DATEDIFF(TO_DATE('###SLICE_VALUE###'), last_transfer_date) ANTERIORITE_ZEBRA, DATEDIFF(TO_DATE('###SLICE_VALUE###'), LAST_TRANSAC_OM_DATE) ANTERIORITE_OM,
    CASE WHEN DATEDIFF(TO_DATE('###SLICE_VALUE###'), LAST_TRANSAC_OM_DATE)<180 THEN 'INACTIF_MOINS_06MOIS'
         WHEN DATEDIFF(TO_DATE('###SLICE_VALUE###'), LAST_TRANSAC_OM_DATE)>=180 and DATEDIFF(TO_DATE('###SLICE_VALUE###'), LAST_TRANSAC_OM_DATE)<30*12 THEN 'INACTIF_ENTRE_06_12MOIS'
         WHEN DATEDIFF(TO_DATE('###SLICE_VALUE###'), LAST_TRANSAC_OM_DATE)>=30*12 and DATEDIFF(TO_DATE('###SLICE_VALUE###'), LAST_TRANSAC_OM_DATE)<30*24 THEN 'INACTIF_ENTRE_12_24MOIS'
         WHEN DATEDIFF(TO_DATE('###SLICE_VALUE###'), LAST_TRANSAC_OM_DATE)>=30*24 THEN 'INACTIF_PLUS_24MOIS' END DUREE_INACTIVITE_OM,
    CASE WHEN DATEDIFF(TO_DATE('###SLICE_VALUE###'), prod_state_date) <30 THEN 'J30' WHEN DATEDIFF(TO_DATE('###SLICE_VALUE###'), prod_state_date) >= 30 and DATEDIFF(TO_DATE('###SLICE_VALUE###'), prod_state_date) <90 THEN 'J90'
        WHEN DATEDIFF(TO_DATE('###SLICE_VALUE###'), prod_state_date)>=90 and DATEDIFF(TO_DATE('###SLICE_VALUE###'), prod_state_date) <180 THEN 'J180' WHEN DATEDIFF(TO_DATE('###SLICE_VALUE###'), prod_state_date)>=180 and DATEDIFF('###SLICE_VALUE###', prod_state_date)<360 THEN 'J360'
        WHEN DATEDIFF(TO_DATE('###SLICE_VALUE###'), prod_state_date)>=360 THEN 'JPLUS360' END AGE_IN, current_timestamp INSERT_DATE
FROM ( SELECT a.*, row_number() OVER (PARTITION BY acc_nbr ORDER BY UPDATE_DATE DESC) rn FROM cdr.spark_it_zte_subs_extract a WHERE original_file_date = DATE_ADD('###SLICE_VALUE###', 1) )a
LEFT JOIN CDR.SPARK_IT_ZTE_PROD_EXTRACT b on b.original_file_date = DATE_ADD('###SLICE_VALUE###', 1) and b.prod_id = a.subs_id --and a.original_file_date = b.original_file_date
LEFT JOIN MON.SPARK_FT_CONTRACT_SNAPSHOT c on c.event_date = DATE_ADD('###SLICE_VALUE###', 1) and c.access_key = a.acc_nbr -- and a.original_file_date = c.event_date
--LEFT JOIN BACKUP_DWH.TT_OFFER_PROFILE h on h.offer_name= c.commercial_offer
LEFT JOIN (
    SELECT CONTRACT_ID, MAIN_MSISDN, MAIN_IMSI, CONTRACT_TYPE, ACCOUNT_STATUS, STATUS_DATE, COUNT(MAIN_MSISDN) OVER(PARTITION BY MAIN_MSISDN ) NBRE_OCCUR,
        ROW_NUMBER() OVER(PARTITION BY MAIN_MSISDN ORDER BY NVL(STATUS_DATE, CURRENT_DATE) DESC, CAST(CONTRACT_ID AS INT) DESC)RN_A
    FROM CDR.SPARK_IT_ACCOUNT WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###'
) h ON A.acc_nbr = h.MAIN_MSISDN AND RN_A=1
LEFT JOIN ( SELECT d.*, row_number() OVER (PARTITION BY msisdn ORDER BY modified_on desc, registered_on desc, account_balance DESC) rn_om, first_value(registered_on) OVER (PARTITION BY msisdn ORDER BY registered_on asc) registered_date_om
    FROM MON.spark_FT_OMNY_ACCOUNT_SNAPSHOT d WHERE d.event_date = '###SLICE_VALUE###') d on rn_om=1 and a.acc_nbr = d.msisdn
LEFT JOIN ( SELECT e.*,  row_number() over (partition by primary_msisdn order by CASE WHEN USER_STATUS='Active' THEN 1
     WHEN USER_STATUS='Suspend Request' THEN 2  WHEN USER_STATUS='Suspended' THEN 3 WHEN USER_STATUS='Removed' THEN 5
     WHEN USER_STATUS='New' THEN 1.1 WHEN USER_STATUS='Delete Request' THEN 4 END asc, channel_user_id desc) status_order FROM CDR.SPARK_IT_ZEBRA_MASTER e WHERE e.transaction_date = TO_DATE('###SLICE_VALUE###') ) e on status_order=1 and e.primary_msisdn = a.acc_nbr
LEFT JOIN (
    select sender_msisdn, max(nbre_transaction) nbre_transaction, max(nbre_jour)nbre_jour, max(total_transfer_amount) total_transfer_amount, max(last_transfer_date) last_transfer_date
    from TMP.TT_ZEBRA_ACTIVE_USER where event_date = TO_DATE('###SLICE_VALUE###') group by sender_msisdn
)f on a.acc_nbr = f.sender_msisdn
LEFT JOIN (
    SELECT  MSISDN, max(transaction_amount) TRANSACTION_AMOUNT, max(last_transaction_date) LAST_TRANSAC_OM_DATE, sum(nb_count) NB_COUNT
    FROM  TMP.TT_MSISDN_ACTIVE_OM WHERE event_date = TO_DATE('###SLICE_VALUE###') group by MSISDN
) g on a.acc_nbr=g.msisdn
WHERE RN=1 AND length(acc_nbr)=9

INSERT INTO MON.SPARK_FT_MSISDN_RECYCLAGE
--
TRUNCATE TABLE TMP.FT_MSISDN_RECYCLAGE2 ;

INSERT INTO MON.SPARK_FT_MSISDN_RECYCLAGE
SELECT MSISDN, A.SUBS_ID, A.IMSI, B.ICCID, ACCT_ID, PRICE_PLAN_ID, PROD_STATE_DATE, AGE, NVL(B.BLOCK_REASON, A.BLOCK_REASON) BLOCK_REASON, A.PROD_STATE, PROD_STATE_NAME,
    A.UPDATE_DATE, PROD_SPEC_ID, ACCESS_KEY, A.ACTIVATION_DATE, DEACTIVATION_DATE, COMMERCIAL_OFFER, OSP_ACCOUNT_TYPE, PROVISIONING_DATE, OSP_STATUS, MAIN_CREDIT,
    EST_PRESENT_OM, REGISTERED_DATE_OM, ID_NUMBER, USER_TYPE, CREATION_DATE_OM, OM_BALANCE, LAST_TRANSAC_OM_DATE, EST_PRESENT_ZEBRA, STATUT_ZEBRA, LAST_TRANSAC_ZEBRA_DATE,
    B.ORDER_REASON, ANTERIORITE_ZEBRA, ANTERIORITE_OM, DUREE_INACTIVITE_OM, AGE_IN,
    CASE WHEN prod_state_name in ('Termination', 'Two-Way Block') and est_present_zebra = 'false' and NVL(CUSTOMER_TYPE,'UNK') !='C' and (osp_account_type is null or upper(osp_account_type)='PREPAID')
        and (duree_inactivite_om in ('INACTIF_PLUS_24MOIS', 'INACTIF_ENTRE_06_24MOIS') or duree_inactivite_om is null) THEN
        CASE WHEN prod_state_name='Termination' and est_present_om='false' then 1
          when prod_state_name='Termination' and est_present_om='true' and om_balance=0  then 1
          when prod_state_name='Termination' and est_present_om='true' and om_balance>0 and (duree_inactivite_om in ('INACTIF_PLUS_24MOIS') or duree_inactivite_om is null) then 1
          -- cas du Two-Way Block main
          when upper(b.order_reason) like '%DECISION%OCM%' then 1
          when upper(b.order_reason)='SIM CARD/MOBILE OF SUBSCRIBER LOST' and age_in in ('J180', 'J360', 'JPLUS360') then 1
          when est_present_om='true' and om_balance=0 then
             case when (upper(b.order_reason) like '%AUTRE%' or b.order_reason is null) and age_in in ('J360', 'JPLUS360') then 1
                  when upper(b.order_reason) in ( 'IDENTIFICATION REJETE', 'MAUVAISE IDENTIFICATION', 'MAUVAISID', 'DELAI D ATTENTE POUR ACTIVATION DEPASSE' ) and age_in in ('J90', 'J180', 'J360', 'JPLUS360') then 1
                  when (upper(b.order_reason) in ('SIM CARD LOST', 'VOL OU PERTE DE LA CARTE SIM') or upper(b.order_reason) like '%FRAUDE%' or upper(b.order_reason) like '%ANARQUE%') and age_in in ('J180', 'J360', 'JPLUS360') then 1
                  else 0
             end
          when est_present_om='true' and om_balance>0 and (duree_inactivite_om in ('INACTIF_PLUS_24MOIS') or duree_inactivite_om is null) then
             case when (upper(b.order_reason) like '%AUTRE%' or b.order_reason is null or upper(b.order_reason) in ('SIM CARD LOST', 'VOL OU PERTE DE LA CARTE SIM') ) and age_in in ('J360', 'JPLUS360') then 1
                  when upper(b.order_reason) in ( 'IDENTIFICATION REJETE', 'MAUVAISE IDENTIFICATION', 'MAUVAISID', 'DELAI D ATTENTE POUR ACTIVATION DEPASSE' ) and age_in in ('J90', 'J180', 'J360', 'JPLUS360') then 1
                  when  (upper(b.order_reason) like '%FRAUDE%' or upper(b.order_reason) like '%ANARQUE%') and age_in in ('J180', 'J360', 'JPLUS360') then 1
                  else 0
             end
          when est_present_om='false' then
             case when (upper(b.order_reason) like '%AUTRE%' or b.order_reason is null) and age_in in ('J360', 'JPLUS360') then 1
                  when upper(b.order_reason) in ( 'IDENTIFICATION REJETE', 'MAUVAISE IDENTIFICATION', 'MAUVAISID', 'DELAI D ATTENTE POUR ACTIVATION DEPASSE' ) and age_in in ('J90', 'J180', 'J360', 'JPLUS360') then 1
                  when (upper(b.order_reason) in ('SIM CARD LOST', 'VOL OU PERTE DE LA CARTE SIM') or upper(b.order_reason) like '%FRAUDE%' or upper(b.order_reason) like '%ANARQUE%') and age_in in ('J180', 'J360', 'JPLUS360') then 1
                  else 0
             end
        ELSE 0 END
    END RECYCLABLE, current_timestamp INSERT_DATE, EVENT_DATE
FROM TMP.TT_MSISDN_RECYCLAGE A
LEFT JOIN (
    SELECT accnbr, prod_state, block_reason, order_reason, update_date, activation_date, iccid, imsi, customer_id, subscriber_type, default_price_plan_id, subs_id, row_number()over(partition by accnbr order by update_date desc, subs_id desc) rn_cont
    FROM CDR.SPARK_IT_CONT WHERE original_file_date ='###SLICE_VALUE###'
) b ON a.msisdn = b.accnbr and a.prod_state_name = b.prod_state and to_date(a.prod_state_date) = to_date(b.update_date) and rn_cont =1
LEFT JOIN(
    SELECT DISTINCT ORIGINAL_FILE_DATE, CUSTID CUSTOMER_ID, GUID, CUSTOMER_PARENT_ID CUSTOMER_TYPE, CUSTSEG PRGCODE
    FROM CDR.SPARK_IT_CUST_FULL WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
) C ON C.CUSTOMER_ID = B.CUSTOMER_ID
WHERE EVENT_DATE='###SLICE_VALUE###'


==================================================================================================================================================

INSERT INTO MON.SPARK_FT_MSISDN_RECYCLAGE
SELECT MSISDN, A.SUBS_ID, A.IMSI, B.ICCID, ACCT_ID, PRICE_PLAN_ID, PROD_STATE_DATE, AGE, NVL(B.BLOCK_REASON, A.BLOCK_REASON) BLOCK_REASON, A.PROD_STATE, PROD_STATE_NAME,
    A.UPDATE_DATE, PROD_SPEC_ID, ACCESS_KEY, A.ACTIVATION_DATE, DEACTIVATION_DATE, COMMERCIAL_OFFER, OSP_ACCOUNT_TYPE, PROVISIONING_DATE, OSP_STATUS, MAIN_CREDIT,
    EST_PRESENT_OM, REGISTERED_DATE_OM, ID_NUMBER, USER_TYPE, CREATION_DATE_OM, OM_BALANCE, LAST_TRANSAC_OM_DATE, EST_PRESENT_ZEBRA, STATUT_ZEBRA, LAST_TRANSAC_ZEBRA_DATE,
    B.ORDER_REASON, ANTERIORITE_ZEBRA, ANTERIORITE_OM, DUREE_INACTIVITE_OM, AGE_IN,
    CASE WHEN prod_state_name in ('Termination', 'Two-Way Block') and est_present_zebra = 'false' and NVL(CUSTOMER_TYPE,'UNK') !='C' and (osp_account_type is null or upper(osp_account_type)='PREPAID')
        and (duree_inactivite_om in ('INACTIF_PLUS_24MOIS', 'INACTIF_ENTRE_06_24MOIS') or duree_inactivite_om is null) THEN
        CASE WHEN prod_state_name='Termination' and est_present_om='false' then 1
          when prod_state_name='Termination' and est_present_om='true' and om_balance=0  then 1
          when prod_state_name='Termination' and est_present_om='true' and om_balance>0 and (duree_inactivite_om in ('INACTIF_PLUS_24MOIS') or duree_inactivite_om is null) then 1
          -- cas du Two-Way Block main
          when upper(b.order_reason) like '%DECISION%OCM%' then 1
          when upper(b.order_reason)='SIM CARD/MOBILE OF SUBSCRIBER LOST' and age_in in ('J180', 'J360', 'JPLUS360') then 1
          when est_present_om='true' and om_balance=0 then
             case when (upper(b.order_reason) like '%AUTRE%' or b.order_reason is null) and age_in in ('J360', 'JPLUS360') then 1
                  when upper(b.order_reason) in ( 'IDENTIFICATION REJETE', 'MAUVAISE IDENTIFICATION', 'MAUVAISID', 'DELAI D ATTENTE POUR ACTIVATION DEPASSE' ) and age_in in ('J90', 'J180', 'J360', 'JPLUS360') then 1
                  when (upper(b.order_reason) in ('SIM CARD LOST', 'VOL OU PERTE DE LA CARTE SIM') or upper(b.order_reason) like '%FRAUDE%' or upper(b.order_reason) like '%ANARQUE%') and age_in in ('J180', 'J360', 'JPLUS360') then 1
                  else 0
             end
          when est_present_om='true' and om_balance>0 and (duree_inactivite_om in ('INACTIF_PLUS_24MOIS') or duree_inactivite_om is null) then
             case when (upper(b.order_reason) like '%AUTRE%' or b.order_reason is null or upper(b.order_reason) in ('SIM CARD LOST', 'VOL OU PERTE DE LA CARTE SIM') ) and age_in in ('J360', 'JPLUS360') then 1
                  when upper(b.order_reason) in ( 'IDENTIFICATION REJETE', 'MAUVAISE IDENTIFICATION', 'MAUVAISID', 'DELAI D ATTENTE POUR ACTIVATION DEPASSE' ) and age_in in ('J90', 'J180', 'J360', 'JPLUS360') then 1
                  when  (upper(b.order_reason) like '%FRAUDE%' or upper(b.order_reason) like '%ANARQUE%') and age_in in ('J180', 'J360', 'JPLUS360') then 1
                  else 0
             end
          when est_present_om='false' then
             case when (upper(b.order_reason) like '%AUTRE%' or b.order_reason is null) and age_in in ('J360', 'JPLUS360') then 1
                  when upper(b.order_reason) in ( 'IDENTIFICATION REJETE', 'MAUVAISE IDENTIFICATION', 'MAUVAISID', 'DELAI D ATTENTE POUR ACTIVATION DEPASSE' ) and age_in in ('J90', 'J180', 'J360', 'JPLUS360') then 1
                  when (upper(b.order_reason) in ('SIM CARD LOST', 'VOL OU PERTE DE LA CARTE SIM') or upper(b.order_reason) like '%FRAUDE%' or upper(b.order_reason) like '%ANARQUE%') and age_in in ('J180', 'J360', 'JPLUS360') then 1
                  else 0
             end
        ELSE 0 END
      WHEN prod_state_name in ('One-Way Block') and est_present_zebra = 'false' and NVL(CUSTOMER_TYPE,'UNK') !='C' and (osp_account_type is null or upper(osp_account_type)='PREPAID') THEN
        CASE WHEN EST_PRESENT_OM='true' AND (( AGE_IN in ( 'J360', 'JPLUS360') and nvl(duree_inactivite_om,'INACTIF_PLUS_24MOIS') in ('INACTIF_ENTRE_06_24MOIS', 'INACTIF_PLUS_24MOIS') and NVL(OM_BALANCE,0.0) = 0.0 THEN 1
             WHEN EST_PRESENT_OM='true' AND (( AGE_IN in ('J180', 'J360', 'JPLUS360') and nvl(duree_inactivite_om,'INACTIF_PLUS_24MOIS')= 'INACTIF_PLUS_24MOIS' THEN 1
             WHEN EST_PRESENT_OM='false' AND (( AGE_IN in ('J180', 'J360', 'JPLUS360') THEN 1
        ELSE 0 END
    END RECYCLABLE, current_timestamp INSERT_DATE, EVENT_DATE
FROM TMP.TT_MSISDN_RECYCLAGE A
LEFT JOIN (
    SELECT accnbr, prod_state, block_reason, order_reason, update_date, activation_date, iccid, imsi, customer_id, subscriber_type, default_price_plan_id, subs_id, row_number()over(partition by accnbr order by update_date desc, subs_id desc) rn_cont
    FROM CDR.SPARK_IT_CONT WHERE original_file_date ='###SLICE_VALUE###'
) b ON a.msisdn = b.accnbr and a.prod_state_name = b.prod_state and to_date(a.prod_state_date) = to_date(b.update_date) and rn_cont =1
LEFT JOIN(
    SELECT DISTINCT ORIGINAL_FILE_DATE, CUSTID CUSTOMER_ID, GUID, CUSTOMER_PARENT_ID CUSTOMER_TYPE, CUSTSEG PRGCODE
    FROM CDR.SPARK_IT_CUST_FULL WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
) C ON C.CUSTOMER_ID = B.CUSTOMER_ID
WHERE EVENT_DATE='###SLICE_VALUE###'

select prod_state_name, case when block_reason not like '%0%' then block_reason end block_reason, order_reason, est_present_zebra, est_present_om, osp_account_type, case when om_balance =0 then 'true' else 'false' end solde_om_nul,
               sum(case when age <30 then 1 else 0 end) j30, sum(case when age >= 30 and age <90 then 1 else 0 end) j90,
               sum(case when age>=90 and age <180 then 1 else 0 end) j180, sum(case when age between 180 and 360 then 1 else 0 end) j360,
               duree_inactivite_om,
               sum(case when age >= 360 then 1 else 0 end) jplus360,  COUNT (*) nbre_total, sum(recyclable) recyclable
from TMP.FT_MSISDN_RECYCLAGE2
where --recyclable=1
     prod_state_name in ('Termination', 'Two-Way Block') and est_present_zebra = 'false'
    and (osp_account_type is null or osp_account_type='PURE PREPAID')
    --and (duree_inactivite_om in ('INACTIF_PLUS_24MOIS', 'INACTIF_ENTRE_06_24MOIS') or duree_inactivite_om is null)
    --and FN_GET_NNP_MSISDN_SIMPLE_DESTN(msisdn)='OCM'
GROUP BY prod_state_name, duree_inactivite_om,
     case when block_reason not like '%0%' then block_reason end, order_reason,
     est_present_zebra,
     est_present_om,
     osp_account_type,
     case when om_balance =0 then 'true' else 'false' end


UPDATE TMP.FT_MSISDN_RECYCLAGE2
SET RECYCLABLE = case when prod_state_name='Termination' and est_present_om='false' then 1
  when prod_state_name='Termination' and est_present_om='true' and om_balance=0  then 1
  when prod_state_name='Termination' and est_present_om='true' and om_balance>0 and (duree_inactivite_om in ('INACTIF_PLUS_24MOIS') or duree_inactivite_om is null) then 1
  -- cas du Two-Way Block main
  when upper(b.order_reason) like '%DECISION%OCM%' then 1
  when upper(b.order_reason)='SIM CARD/MOBILE OF SUBSCRIBER LOST' and age_in in ('J180', 'J360', 'JPLUS360') then 1
  when est_present_om='true' and om_balance=0 then
     case when (upper(b.order_reason) like '%AUTRE%' or b.order_reason is null) and age_in in ('J360', 'JPLUS360') then 1
          when upper(b.order_reason) in ( 'IDENTIFICATION REJETE', 'MAUVAISE IDENTIFICATION', 'MAUVAISID', 'DELAI D ATTENTE POUR ACTIVATION DEPASSE' ) and age_in in ('J90', 'J180', 'J360', 'JPLUS360') then 1
          when (upper(b.order_reason) in ('SIM CARD LOST', 'VOL OU PERTE DE LA CARTE SIM') or upper(b.order_reason) like '%FRAUDE%' or upper(b.order_reason) like '%ANARQUE%') and age_in in ('J180', 'J360', 'JPLUS360') then 1
          else 0
     end
  when est_present_om='true' and om_balance>0 and (duree_inactivite_om in ('INACTIF_PLUS_24MOIS') or duree_inactivite_om is null) then
     case when (upper(b.order_reason) like '%AUTRE%' or b.order_reason is null or upper(b.order_reason) in ('SIM CARD LOST', 'VOL OU PERTE DE LA CARTE SIM') ) and age_in in ('J360', 'JPLUS360') then 1
          when upper(b.order_reason) in ( 'IDENTIFICATION REJETE', 'MAUVAISE IDENTIFICATION', 'MAUVAISID', 'DELAI D ATTENTE POUR ACTIVATION DEPASSE' ) and age_in in ('J90', 'J180', 'J360', 'JPLUS360') then 1
          when  (upper(b.order_reason) like '%FRAUDE%' or upper(b.order_reason) like '%ANARQUE%') and age_in in ('J180', 'J360', 'JPLUS360') then 1
          else 0
     end
  when est_present_om='false' then
     case when (upper(b.order_reason) like '%AUTRE%' or b.order_reason is null) and age_in in ('J360', 'JPLUS360') then 1
          when upper(b.order_reason) in ( 'IDENTIFICATION REJETE', 'MAUVAISE IDENTIFICATION', 'MAUVAISID', 'DELAI D ATTENTE POUR ACTIVATION DEPASSE' ) and age_in in ('J90', 'J180', 'J360', 'JPLUS360') then 1
          when (upper(b.order_reason) in ('SIM CARD LOST', 'VOL OU PERTE DE LA CARTE SIM') or upper(b.order_reason) like '%FRAUDE%' or upper(b.order_reason) like '%ANARQUE%') and age_in in ('J180', 'J360', 'JPLUS360') then 1
          else 0
     end
  else 0 end
WHERE EVENT_DATE='2020-03-05';

SELECT accnbr, prod_state, block_reason, order_reason, update_date, activation_date, iccid, imsi, customer_id, subscriber_type, default_price_plan_id, subs_id, row_number()over(partition by accnbr order by update_date desc) rn_cont
FROM CDR.SPARK_IT_CONT WHERE original_file_date ='2020-03-05'
    AND ICCID IS NULL
group by SUBSTR(accnbr,1,3)
limit 10

SELECT msisdn, iccid, prod_state_name, prod_state_date
select count(*) , min(prod_state_date), max(prod_state_date)
from TMP.FT_MSISDN_RECYCLAGE a left join ( SELECT accnbr, prod_state, block_reason, order_reason, update_date, activation_date, iccid, imsi, customer_id, subscriber_type, default_price_plan_id, subs_id, row_number()over(partition by accnbr order by update_date desc) rn_cont
FROM CDR.SPARK_IT_CONT WHERE original_file_date ='2020-03-05' ) b on b.accnbr=a.msisdn and rn_cont =1
where est_present_om='false' and est_present_zebra='false' and prod_state_name='Non-provisioning'

     and a.prod_state_name != b.prod_state and  a.subs_id != nvl(b.subs_id,'lool')
limit 10;

select count(*)
FROM CDR.SPARK_IT_CONT WHERE original_file_date ='2020-03-05'
AND imsi IS NULL and est_present_om='false' and est_present_zebra='false' and prod_state_name='Non-provisioning'


-- update zebra
create table tmp.tt_zebra_active_user as
SELECT '2020-02-25' event_date, sENDer_msisdn, sum(nbre_transaction) nbre_transaction, sum(nbre_jour)nbre_jour, sum(total_transfer_amount)total_transfer_amount, max(last_tranfer_date) last_tranfer_date, current_timestamp insert_date
FROM (
SELECT to_date(substr(event_date, 1, 10)) event_date, sENDer_msisdn, nbre_transaction, nbre_jour, total_transfer_amount, FROM_UNIXTIME(unix_timestamp(last_tranfer_date, 'yyyy-MM-dd HH:mm:ss')) last_transfer_date
FROM BACKUP_DWH.TT_ZEBRA_ACTIVE_USER2
union
SELECT max(transfer_date) event_date, sENDer_msisdn, count(distinct transfer_id) nbre_transaction, count(distinct transfer_date) nbre_jour, sum(transfer_amount) total_transfer_amount, max(FROM_UNIXTIME(unix_timestamp(transfer_date_time, 'yyyy-MM-dd HH:mm:ss'))) last_transfer_date
FROM cdr.spark_it_zebra_transac
WHERE transfer_date >='2020-02-05'
group by sENDer_msisdn
) a group by sENDer_msisdn
limit 50;

insert into TMP.TT_ZEBRA_ACTIVE_USER
SELECT '2020-03-05' event_date, sENDer_msisdn, sum(nbre_transaction) nbre_transaction, sum(nbre_jour)nbre_jour, sum(total_transfer_amount)total_transfer_amount, max(last_transfer_date) last_transfer_date, current_timestamp insert_date
FROM (
SELECT to_date(event_date) event_date, sENDer_msisdn, nbre_transaction, nbre_jour, total_transfer_amount, FROM_UNIXTIME(unix_timestamp(last_transfer_date, 'yyyy-MM-dd HH:mm:ss')) last_transfer_date
FROM TMP.TT_ZEBRA_ACTIVE_USER WHERE event_date='2020-02-25'
union
SELECT max(transfer_date) event_date, sENDer_msisdn, count(distinct transfer_id) nbre_transaction, count(distinct transfer_date) nbre_jour, sum(transfer_amount) total_transfer_amount, max(FROM_UNIXTIME(unix_timestamp(transfer_date_time, 'yyyy-MM-dd HH:mm:ss'))) last_transfer_date
FROM cdr.spark_it_zebra_transac
WHERE TRANSFER_DATE >='2020-02-26'
group by sENDer_msisdn
) a group by sENDer_msisdn

--- update om
insert /* + APPEND */  into  tt_msisdn_active_om
SELECT  MSISDN, sum(transaction_amount) transaction_amount, max(last_transaction_date) last_transaction_date, sum(nb_count) nb_count, TO_DATE(20200304, 'yyyymmdd') event_date
FROM (
    SELECT msisdn, transaction_amount, last_transaction_date, nb_count
    FROM TT_MSISDN_ACTIVE_OM WHERE event_date= TO_DATE('2020-02-05')
    union all
    SELECT SENDER_MSISDN, sum(transaction_amount) transaction_amount, max(transfer_datetime) last_transaction_date, count(*) nb_count--, 'RECEIVER' type
    FROM TANGO_CDR.IT_OMNY_TRANSACTIONS
    WHERE TRANSFER_DATETIME BETWEEN TO_DATE('2020-02-06') AND TO_DATE('2020-03-05')
    AND TRANSFER_STATUS='TS'
    group by SENDER_MSISDN--, SERVICE_TYPE
    union all
    SELECT RECEIVER_MSISDN, sum(transaction_amount) transaction_amount, max(transfer_datetime) last_transaction_date, count(*) nb_count--, 'RECEIVER' type
    FROM TANGO_CDR.IT_OMNY_TRANSACTIONS
    WHERE TRANSFER_DATETIME BETWEEN TO_DATE('2020-02-06') and TO_DATE('2020-03-05')
    AND TRANSFER_STATUS='TS'
    group by RECEIVER_MSISDN--, SERVICE_TYPE
)group by msisdn;


create table tmp.TT_MSISDN_ACTIVE_OM as
SELECT TO_DATE('2020-03-05') event_date, MSISDN, sum(transaction_amount) transaction_amount, max(last_transaction_date) last_transaction_date, sum(nb_count) nb_count, current_timestamp insert_date
FROM (
    SELECT msisdn, cast(transaction_amount as int) transaction_amount,
        FROM_UNIXTIME(unix_timestamp(last_transaction_date, 'yyyy-MM-dd HH:mm:ss')) last_transaction_date, nb_count, substr(event_date,1,10) event_date
    FROM BACKUP_DWH.TT_MSISDN_ACTIVE_OM2
    UNION ALL
    SELECT SENDER_MSISDN, sum(transaction_amount) transaction_amount, max(transfer_datetime) last_transaction_date, count(*) nb_count--, 'RECEIVER' type
    FROM CDR.spark_IT_OMNY_TRANSACTIONS
    WHERE TRANSFER_DATETIME BETWEEN TO_DATE('2020-03-05') AND TO_DATE('2020-03-05')
        AND TRANSFER_STATUS='TS'
    group by SENDER_MSISDN--, SERVICE_TYPE
    UNION ALL
    SELECT RECEIVER_MSISDN, sum(transaction_amount) transaction_amount, max(transfer_datetime) last_transaction_date, count(*) nb_count--, 'RECEIVER' type
    FROM CDR.spark_IT_OMNY_TRANSACTIONS
    WHERE TRANSFER_DATETIME BETWEEN TO_DATE('2020-03-05') and TO_DATE('2020-03-05')
        AND TRANSFER_STATUS='TS'
    GROUP BY RECEIVER_MSISDN
) a group by MSISDN;

create table TMP.TT_MSISDN_ACTIVE_OM as SELECT TO_DATE('2020-03-05') event_date, MSISDN, sum(transaction_amount) transaction_amount, max(last_transaction_date) last_transaction_date, sum(nb_count) nb_count, current_timestamp insert_date
FROM ( SELECT msisdn, cast(transaction_amount as int) transaction_amount, FROM_UNIXTIME(unix_timestamp(last_transaction_date, 'yyyy-MM-dd HH:mm:ss')) last_transaction_date, nb_count--, substr(event_date,1,10) event_date
FROM BACKUP_DWH.TT_MSISDN_ACTIVE_OM2
UNION ALL
SELECT SENDER_MSISDN, sum(transaction_amount) transaction_amount, max(FROM_UNIXTIME(unix_timestamp(transfer_datetime, 'yyyy-MM-dd HH:mm:ss'))) last_transaction_date, count(*) nb_count
FROM CDR.spark_IT_OMNY_TRANSACTIONS
WHERE TRANSFER_DATETIME BETWEEN TO_DATE('2020-03-05') AND TO_DATE('2020-03-05')
AND TRANSFER_STATUS='TS'
group by SENDER_MSISDN
UNION ALL
SELECT RECEIVER_MSISDN, sum(transaction_amount) transaction_amount, max(FROM_UNIXTIME(unix_timestamp(transfer_datetime, 'yyyy-MM-dd HH:mm:ss'))) last_transaction_date, count(*) nb_count
FROM CDR.spark_IT_OMNY_TRANSACTIONS
WHERE TRANSFER_DATETIME BETWEEN TO_DATE('2020-03-05') and TO_DATE('2020-03-05')
AND TRANSFER_STATUS='TS'
GROUP BY RECEIVER_MSISDN
) a group by MSISDN;

=================================


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

INSERT INTO tmp.tt_stat_recyclage_new
SELECT  EVENT_DATE, PROD_STATE_NAME, EST_PRESENT_OM, osp_account_type, CASE WHEN NVL(OM_BALANCE,0.0) >0 THEN 'FALSE' ELSE 'TRUE' END SOLDE_OM_NUL, nvl(duree_inactivite_om,'INACTIF_PLUS_24MOIS') DUREE_INACTIVITE_OM,
     CASE  WHEN DATEDIFF(TO_DATE(EVENT_DATE), prod_state_date) <90 THEN 'Moins_de_03Mois' WHEN DATEDIFF(TO_DATE(EVENT_DATE), prod_state_date) >= 180 THEN '06Mois_et_plus'
     WHEN DATEDIFF(TO_DATE(EVENT_DATE), prod_state_date) >= 90 and DATEDIFF(TO_DATE(EVENT_DATE), prod_state_date) < 180 THEN 'Entre_03_et_06Mois' END AGE_IN, count(*) nbre_msisdn,
     sum(CASE WHEN upper(OSP_ACCOUNT_TYPE) like '%PREPAID%' AND EST_PRESENT_ZEBRA='false' and NVL(C.CUSTOMER_TYPE,'UNK') !='C' THEN
          CASE WHEN EST_PRESENT_OM='true' AND (( AGE_IN in ('J180', 'J360', 'JPLUS360') AND PROD_STATE_NAME = 'One-Way Block') or (PROD_STATE_NAME = 'Two-Way Block' and recyclable=1))
             and nvl(duree_inactivite_om,'INACTIF_PLUS_24MOIS') in ('INACTIF_ENTRE_06_24MOIS', 'INACTIF_PLUS_24MOIS') and NVL(OM_BALANCE,0.0) = 0.0 THEN 1
           WHEN EST_PRESENT_OM='true' AND (( AGE_IN in ('J180', 'J360', 'JPLUS360') AND PROD_STATE_NAME = 'One-Way Block') or (PROD_STATE_NAME = 'Two-Way Block' and recyclable=1))
             and nvl(duree_inactivite_om,'INACTIF_PLUS_24MOIS')= 'INACTIF_PLUS_24MOIS' THEN 1
          WHEN EST_PRESENT_OM='false' AND (( AGE_IN in ('J180', 'J360', 'JPLUS360') AND PROD_STATE_NAME = 'One-Way Block') or (PROD_STATE_NAME = 'Two-Way Block' and recyclable=1)) THEN 1
          ELSE 0 END
        ELSE 0 END ) recyclable, sum(nvl(recyclable,0)) recyclable_old
FROM MON.SPARK_FT_MSISDN_RECYCLAGE A
LEFT JOIN (
    SELECT accnbr, prod_state, block_reason, order_reason, update_date, activation_date, iccid, imsi, customer_id, subscriber_type, default_price_plan_id, subs_id, row_number()over(partition by accnbr order by update_date desc, subs_id desc) rn_cont
    FROM CDR.SPARK_IT_CONT WHERE original_file_date ='###SLICE_VALUE###'
) b ON a.msisdn = b.accnbr and a.prod_state_name = b.prod_state and to_date(a.prod_state_date) = to_date(b.update_date) and rn_cont =1
LEFT JOIN(
    SELECT DISTINCT ORIGINAL_FILE_DATE, CUSTID CUSTOMER_ID, GUID, CUSTOMER_PARENT_ID CUSTOMER_TYPE, CUSTSEG PRGCODE
    FROM CDR.SPARK_IT_CUST_FULL WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
) C ON C.CUSTOMER_ID = B.CUSTOMER_ID
WHERE EVENT_DATE in ('###SLICE_VALUE###') AND EST_PRESENT_ZEBRA='false' AND FN_GET_NNP_MSISDN_SIMPLE_DESTN(MSISDN)='OCM' and osp_account_type in ('PrePaid', 'PURE PREPAID')
group by EVENT_DATE, PROD_STATE_NAME, EST_PRESENT_OM, osp_account_type, nvl(duree_inactivite_om,'INACTIF_PLUS_24MOIS') , CASE WHEN NVL(OM_BALANCE,0.0) >0 THEN 'FALSE' ELSE 'TRUE' END,
     CASE  WHEN DATEDIFF(TO_DATE(EVENT_DATE), prod_state_date) <90 THEN 'Moins_de_03Mois' WHEN DATEDIFF(TO_DATE(EVENT_DATE), prod_state_date) >= 180 THEN '06Mois_et_plus'
     WHEN DATEDIFF(TO_DATE(EVENT_DATE), prod_state_date) >= 90 and DATEDIFF(TO_DATE(EVENT_DATE), prod_state_date) < 180 THEN 'Entre_03_et_06Mois' END
order by EVENT_DATE
