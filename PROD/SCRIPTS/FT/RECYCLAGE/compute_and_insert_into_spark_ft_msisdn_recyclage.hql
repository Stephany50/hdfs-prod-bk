INSERT INTO MON.SPARK_FT_MSISDN_RECYCLAGE
SELECT MSISDN, A.SUBS_ID, A.IMSI, B.ICCID, ACCT_ID, PRICE_PLAN_ID, PROD_STATE_DATE, AGE, NVL(B.BLOCK_REASON, A.BLOCK_REASON) BLOCK_REASON, A.PROD_STATE, PROD_STATE_NAME,
    A.UPDATE_DATE, PROD_SPEC_ID, ACCESS_KEY, A.ACTIVATION_DATE, DEACTIVATION_DATE, COMMERCIAL_OFFER, OSP_ACCOUNT_TYPE, PROVISIONING_DATE, OSP_STATUS, MAIN_CREDIT,
    EST_PRESENT_OM, REGISTERED_DATE_OM, ID_NUMBER, USER_TYPE, CREATION_DATE_OM, OM_BALANCE, LAST_TRANSAC_OM_DATE, EST_PRESENT_ZEBRA, STATUT_ZEBRA, LAST_TRANSAC_ZEBRA_DATE,
    B.ORDER_REASON, ANTERIORITE_ZEBRA, ANTERIORITE_OM, DUREE_INACTIVITE_OM, AGE_IN,
    CASE WHEN prod_state_name in ('Termination', 'Two-Way Block') and est_present_zebra = 'false' and (osp_account_type is null or osp_account_type='PURE PREPAID')
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
WHERE EVENT_DATE='###SLICE_VALUE###'