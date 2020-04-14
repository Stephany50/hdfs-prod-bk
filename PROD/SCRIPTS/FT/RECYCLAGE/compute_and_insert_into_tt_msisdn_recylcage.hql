INSERT INTO TMP.TT_MSISDN_RECYCLAGE
SELECT distinct TO_DATE('###SLICE_VALUE###') EVENT_DATE, acc_nbr MSISDN, SUBS_ID, A.IMSI, ACCT_ID, PRICE_PLAN_ID, PROD_STATE_DATE, DATEDIFF(TO_DATE('###SLICE_VALUE###'), prod_state_date) AGE, BLOCK_REASON, PROD_STATE,
CASE WHEN b.prod_state = 'G' THEN 'Inactive' WHEN b.prod_state = 'A' THEN 'Active' WHEN b.prod_state = 'D' THEN 'One-Way Block'
        WHEN b.prod_state = 'E' THEN 'Two-Way Block' WHEN b.prod_state = 'B' THEN 'Termination' WHEN b.prod_state = 'H' THEN 'Non-provisioning'
        WHEN b.prod_state = 'Y' THEN 'UnPassIdentity' WHEN b.prod_state = 'L' THEN 'Subscriber Locked' END PROD_STATE_NAME, A.UPDATE_DATE, PROD_SPEC_ID, ACCESS_KEY,
    COMPLETED_DATE ACTIVATION_DATE, DEACTIVATION_DATE, COMMERCIAL_OFFER, h.profile OSP_ACCOUNT_TYPE, PROVISIONING_DATE, OSP_STATUS, MAIN_CREDIT,
    CASE WHEN d.msisdn is not null or LAST_TRANSAC_OM_DATE  is not null THEN 'true'  else 'false' END EST_PRESENT_OM, d.registered_date_om REGISTERED_DATE_OM, ID_NUMBER, d.USER_TYPE, creation_date CREATION_DATE_OM, account_balance OM_BALANCE, LAST_TRANSAC_OM_DATE,
    CASE WHEN e.primary_msisdn is not null or last_transfer_date is not null THEN 'true' else 'false' END EST_PRESENT_ZEBRA, user_status STATUT_ZEBRA, last_transfer_date LAST_TRANSAC_ZEBRA_DATE,
    CAST(null as String) ORDER_REASON, DATEDIFF(TO_DATE('###SLICE_VALUE###'), last_transfer_date) ANTERIORITE_ZEBRA, DATEDIFF(TO_DATE('###SLICE_VALUE###'), LAST_TRANSAC_OM_DATE) ANTERIORITE_OM,
    CASE WHEN DATEDIFF(TO_DATE('###SLICE_VALUE###'), LAST_TRANSAC_OM_DATE)<180 THEN 'INACTIF_MOINS_06MOIS'
         WHEN DATEDIFF(TO_DATE('###SLICE_VALUE###'), LAST_TRANSAC_OM_DATE)>=180 and DATEDIFF(TO_DATE('###SLICE_VALUE###'), LAST_TRANSAC_OM_DATE)<30*24 THEN 'INACTIF_ENTRE_06_24MOIS'
         WHEN DATEDIFF(TO_DATE('###SLICE_VALUE###'), LAST_TRANSAC_OM_DATE)>=30*24 THEN 'INACTIF_PLUS_24MOIS' END DUREE_INACTIVITE_OM,
    CASE WHEN DATEDIFF(TO_DATE('###SLICE_VALUE###'), prod_state_date) <30 THEN 'J30' WHEN DATEDIFF(TO_DATE('###SLICE_VALUE###'), prod_state_date) >= 30 and DATEDIFF(TO_DATE('###SLICE_VALUE###'), prod_state_date) <90 THEN 'J90'
        WHEN DATEDIFF(TO_DATE('###SLICE_VALUE###'), prod_state_date)>=90 and DATEDIFF(TO_DATE('###SLICE_VALUE###'), prod_state_date) <180 THEN 'J180' WHEN DATEDIFF(TO_DATE('###SLICE_VALUE###'), prod_state_date)>=180 and DATEDIFF('###SLICE_VALUE###', prod_state_date)<360 THEN 'J360'
        WHEN DATEDIFF(TO_DATE('###SLICE_VALUE###'), prod_state_date)>=360 THEN 'JPLUS360' END AGE_IN, current_timestamp INSERT_DATE
FROM ( SELECT a.*, row_number() OVER (PARTITION BY acc_nbr ORDER BY UPDATE_DATE DESC) rn FROM cdr.spark_it_zte_subs_extract a WHERE original_file_date = DATE_ADD('###SLICE_VALUE###', 1) )a
LEFT JOIN CDR.SPARK_IT_ZTE_PROD_EXTRACT b on b.original_file_date = DATE_ADD('###SLICE_VALUE###', 1) and b.prod_id = a.subs_id --and a.original_file_date = b.original_file_date
LEFT JOIN MON.SPARK_FT_CONTRACT_SNAPSHOT c on c.event_date = DATE_ADD('###SLICE_VALUE###', 1) and c.access_key = a.acc_nbr -- and a.original_file_date = c.event_date
LEFT JOIN BACKUP_DWH.TT_OFFER_PROFILE h on h.offer_name= c.commercial_offer
LEFT JOIN ( SELECT d.*, row_number() OVER (PARTITION BY msisdn ORDER BY modified_on desc, registered_on desc, account_balance DESC) rn_om, first_value(registered_on) OVER (PARTITION BY msisdn ORDER BY registered_on asc) registered_date_om
    FROM MON.spark_FT_OMNY_ACCOUNT_SNAPSHOT d WHERE d.event_date = DATE_SUB('###SLICE_VALUE###', 1)) d on rn_om=1 and a.acc_nbr = d.msisdn
LEFT JOIN ( SELECT e.*,  row_number() over (partition by primary_msisdn order by CASE WHEN USER_STATUS='Active' THEN 1
     WHEN USER_STATUS='Suspend Request' THEN 2  WHEN USER_STATUS='Suspended' THEN 3 WHEN USER_STATUS='Removed' THEN 5
     WHEN USER_STATUS='New' THEN 1.1 WHEN USER_STATUS='Delete Request' THEN 4 END asc, channel_user_id desc) status_order FROM CDR.SPARK_IT_ZEBRA_MASTER e WHERE e.transaction_date = TO_DATE('###SLICE_VALUE###') ) e on status_order=1 and e.primary_msisdn = a.acc_nbr
LEFT JOIN (
    select sender_msisdn, max(nbre_transaction) nbre_transaction, max(nbre_jour)nbre_jour, max(total_transfer_amount) total_transfer_amount, max(last_transfer_date) last_transfer_date
    from TMP.TT_ZEBRA_ACTIVE_USER where event_date = TO_DATE('###SLICE_VALUE###') group by sender_msisdn
)f on a.acc_nbr = f.sender_msisdn
LEFT JOIN (
    SELECT  MSISDN, max(transaction_amount) TRANSACTION_AMOUNT, max(last_transaction_date) LAST_TRANSACTION_DATE, sum(nb_count) NB_COUNT
    FROM  TMP.TT_MSISDN_ACTIVE_OM WHERE event_date = TO_DATE('###SLICE_VALUE###') group by MSISDN
) g on a.acc_nbr=g.msisdn
WHERE RN=1 AND length(acc_nbr)=9