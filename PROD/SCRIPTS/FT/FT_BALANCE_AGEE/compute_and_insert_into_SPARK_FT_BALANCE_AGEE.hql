INSERT INTO MON.SPARK_FT_BALANCE_AGEE
SELECT CODE_CLIENT, CATEGORIE, NOM, CASE WHEN count(case when prod_state='Active' then accnbr end) !=0 THEN 'ACTIF'
    WHEN count(case when prod_state in ('Termination', 'One-Way Block', 'Two-Way Block') then accnbr end) = 0 then 'DESACTIVE' ELSE 'SUSPENDU' END STATUT,
    BILLCYCLE_CODE, BALANCE, DERNIERE_FACTURE, balance_J, balance_J_30, balance_J_60, balance_J_90, balance_J_120, balance_J_150, balance_J_180,
    balance_J_360, balance_J_720, balance_J_720_Plus, a.CUSTOMER_ID, BALANCE_CURRENT, NBRE_FACTURE, NBRE_ENTREE, count(distinct accnbr) NBRE_FILS,
    count(case when prod_state='Active' then accnbr end) nbre_actif, count(case when prod_state in ('Non-provisioning', 'Inactive') or prod_state is null then accnbr end) nbre_inactif, count(case when prod_state in ('Termination', 'One-Way Block', 'Two-Way Block') then accnbr end) nbre_suspendu, CURRENT_TIMESTAMP INSERT_DATE, EVENT_DATE
FROM TMP.TT_TMP_BALANCE_AGEE A
LEFT JOIN (
    SELECT original_file_date, accnbr, RESP_PAYMENT, ACCOUNT_NUMBER, PARENT_ACCOUNT_NUMBER, prod_state, block_reason, order_reason, update_date, activation_date, iccid, imsi,
        customer_id, subscriber_type, default_price_plan_id, subs_id, row_number()over( partition by original_file_date, accnbr order by cast(subs_id as int) desc, update_date desc) rn_cont
    FROM CDR.SPARK_IT_CONT WHERE original_file_date = '###SLICE_VALUE###'
        and subscriber_type !='VPN(OCS)'
) b on a.CODE_CLIENT = b.RESP_PAYMENT and b.original_file_date= a.event_date and rn_cont=1
WHERE EVENT_DATE = '###SLICE_VALUE###'
GROUP BY EVENT_DATE, CODE_CLIENT, CATEGORIE, NOM, BALANCE, DERNIERE_FACTURE, balance_J, balance_J_30, balance_J_60, balance_J_90, balance_J_120, balance_J_150, balance_J_180,
          balance_J_360, balance_J_720, balance_J_720_Plus, BILLCYCLE_CODE, a.CUSTOMER_ID, BALANCE_CURRENT, NBRE_FACTURE, NBRE_ENTREE
