select  
    sender_msisdn ,
    receiver_msisdn ,
    receiver_user_id ,
    sender_user_id ,
    transaction_amount ,
    commissions_paid ,
    commissions_received ,
    commissions_others ,
    service_charge_received ,
    service_charge_paid ,
    taxes ,
    (
        case when transfer_status='TS' and substr(transfer_id,1,2)='PP' and
            sender_category_code='SUBS' and A.msisdn is null and B.msisdn is null then "P2P_OUT_OM"

        when transfer_status='TS' and  substr(transfer_id,1,2)='PN' then "TNO_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='CO' and C.msisdn is null and D.msisdn is null then "CASHOUT_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='CN' then "CASHOUT_OFFNET_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='PP' and sender_category_code<>'SUBS' 
            and receiver_category_code<>'SUBS' then "FEE_B2B_TRANSACTION_OM"
        
        when transfer_status='TS' and substr(transfer_id,1,2)='CI' and F.msisdn is not null
            then "B2W_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='CO' and E.msisdn is not null
            then "W2B_OM"

        when transfer_status='TS' and substr(transfer_id,1,2) in ('BP','MP','IN') and G.msisdn is not null and B.msisdn is null
            then "ENEO_OM"
        
        when transfer_status='TS' and substr(transfer_id,1,2)='MP' and I.msisdn is not null and B.msisdn is null then "CANAL_PLUS_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='MP' and receiver_msisdn='657763384' and B.msisdn is null then "STARTIME_OM" 

        when transfer_status='TS' and substr(transfer_id,1,2) in ('BP','MP','IN') and K.msisdn is not null and B.msisdn is null
            then "CAMWATER_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='BP' and receiver_msisdn not in ('699703939','AES')
                and L.msisdn is null and B.msisdn is null then "AUTRE_FACTURES_OM" 

        when transfer_status='TS' and ((substr(transfer_id,1,2)='PP' and upper(receiver_grade_name) like "%INFORMEL%" and 
                A.msisdn is null) or substr(transfer_id,1,2)='MP' and 
                receiver_msisdn<>'AES' and N.msisdn is null and B.msisdn is null and 
                O.msisdn is null) then "MERCHPAY_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='SM' and receiver_msisdn <>'AES' and 
                N.msisdn is null and B.msisdn is null then "OM_ASSO_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='MP' and sender_category_code='SUBS' and Q.msisdn is not null
                and N.msisdn is null and P.msisdn is null 
                then "WEB_PAIEMENT_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='ER' then "PAIEMENT_MASSE_OM"

        when transfer_status='TS' and  substr(transfer_id,1,2)='MP' and sender_category_code='SUBS' and R.msisdn is not null 
                and N.msisdn is null and P.msisdn is null
                then "REMONTEE_FOND_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='RC' then"TOP_UP_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='MP' and S.msisdn is not null
                and B.msisdn is null then "DUAL_PAIMENT_OM"

        when transfer_status='TS' and substr(transfer_id,1,2)='CI' and receiver_category_code='SUBS' 
            and C.msisdn is null and D.msisdn is null then "CASHIN_OM"

        when transfer_status='TS' and substr(transfer_id,1,2) in ('CO','MP') and T.msisdn is not null
                then "VISA_OM"

        when transfer_status='TS' and U.msisdn is not null and B.msisdn is null and 
            substr(transfer_id,1,2) in ('CI','CO','PN','RC','MP','BP','ER')  then "MICRO_ASSURANCE_OM"

        when transfer_status='TS' and service_type="CASHIN" and sender_category_code<>'SUBS' and 
                V.msisdn is not null then "GIMAC_IN_OM"

        when transfer_status='TS' and service_type in ('CASHOUT', 'P2P', 'MERCHPAY') and sender_category_code='SUBS' and 
                (W.msisdn is not null and X.msisdn is null) then "GIMACT_OUT_OM"

        when transfer_status='TS' and sender_category_code='IRTCAT' and sender_msisdn in ("691400678","692037846",
            "692037849","692049000") and A.msisdn is null then "IRT_OM"

        else service_type end
    ) service_type,
    transfer_status ,
    sender_pre_bal ,
    sender_post_bal ,
    receiver_pre_bal ,
    receiver_post_bal ,
    sender_acc_status ,
    receiver_acc_status ,
    error_code ,
    error_desc ,
    reference_number ,
    created_on ,
    created_by ,
    modified_on ,
    modified_by ,
    app_1_date ,
    app_2_date ,
    transfer_id ,
    transfer_datetime_nq ,
    sender_category_code ,
    sender_domain_code ,
    sender_grade_name ,
    sender_group_role ,
    sender_designation ,
    sender_state ,
    receiver_category_code ,
    receiver_domain_code ,
    receiver_grade_name ,
    receiver_group_role ,
    receiver_designation ,
    receiver_state ,
    sender_city,
    receiver_city,
    app_1_by ,
    app_2_by ,
    request_source ,
    gateway_type ,
    transfer_subtype ,
    payment_type ,
    payment_number ,
    payment_date ,
    remarks ,
    action_type ,
    transaction_tag ,
    reconciliation_by ,
    reconciliation_for ,
    ext_txn_number ,
    original_ref_number ,
    zebra_ambiguous ,
    attempt_status ,
    other_msisdn ,
    sender_wallet_number ,
    receiver_wallet_number ,
    sender_user_name ,
    receiver_user_name ,
    tno_msisdn ,
    tno_id ,
    unreg_first_name ,
    unreg_last_name ,
    unreg_dob ,
    unreg_id_number ,
    bulk_payout_batchid ,
    is_financial ,
    transfer_done ,
    initiator_msisdn ,
    validator_msisdn ,
    initiator_comments ,
    validator_comments ,
    sender_wallet_name ,
    reciever_wallet_name ,s
    sender_user_type ,
    receiver_user_type ,
    original_file_name ,
    original_file_size ,
    original_file_line_count ,
    original_file_date ,
    insert_date ,
    txnmode ,
    transfer_datetime ,
    file_date
FROM (select * from CDR.SPARK_IT_OMNY_TRANSACTIONS WHERE transfer_datetime = '###SLICE_VALUE###') T0
left join 
(select msisdn from DIM.OM_TEST_MSISDNS) A on receiver_msisdn= A.msisdn
left join 
(select msisdn from DIM.OM_TEST_MSISDNS) B on sender_msisdn= B.msisdn
left join 
(select msisdn from DIM.OM_TEST_MSISDNS union select msisdn from DIM.msisdn_VISA 
union select msisdn from DIM.msisdn_B2W union select msisdn from DIM.msisdn_GIMAC) C
on receiver_msisdn= C.msisdn
left join
(select msisdn from DIM.OM_TEST_MSISDNS union select msisdn from DIM.msisdn_VISA 
union select msisdn from DIM.msisdn_B2W union select msisdn from DIM.msisdn_GIMAC) D
on sender_msisdn= D.msisdn
left join
(select msisdn from DIM.msisdn_B2W) E on receiver_msisdn= E.msisdn
left join
(select msisdn from DIM.msisdn_B2W) F on sender_msisdn= F.msisdn
left join
(select msisdn from DIM.msisdn_ENEO) G on receiver_msisdn= G.msisdn
left join
(select msisdn from DIM.msisdn_ENEO) H on sender_msisdn= H.msisdn
left join
(select msisdn from DIM.msisdn_CANAL) I on receiver_msisdn= I.msisdn
left join 
(select msisdn from DIM.msisdn_EAU) K on receiver_msisdn= K.msisdn
left join 
(select msisdn from DIM.msisdn_EAU union select msisdn from DIM.ENEOPREPAID_MSISDNS
union select msisdn from DIM.OM_TEST_MSISDNS) L on receiver_msisdn= L.msisdn
left join 
(select msisdn from DIM.NON_MERCHANT_MSISDNS union select msisdn from DIM.OM_TEST_MSISDNS) N
on receiver_msisdn= N.msisdn
left join 
(select msisdn from DIM.msisdn_EAU union select msisdn from DIM.REF_OM_PRODUCTS 
where upper(technology) like "%WEB" or upper(technology) like "%FLAS%") O
on receiver_msisdn= O.msisdn
left join 
(select msisdn from DIM.NON_MERCHANT_MSISDNS union select msisdn from DIM.OM_TEST_MSISDNS) P
on sender_msisdn= P.msisdn
left join
(select distinct msisdn from DIM.REF_OM_PRODUCTS where ref_date in (select max(ref_date) 
from DIM.REF_OM_PRODUCTS where ref_date<='###SLICE_VALUE###') 
and upper(technology) like "%WEB%" or upper(technology) like "%FLAS%") Q
on receiver_msisdn= Q.msisdn
left join
(select distinct msisdn from DIM.REF_OM_PRODUCTS where ref_date in (select max(ref_date) 
from DIM.REF_OM_PRODUCTS 
where ref_date<='###SLICE_VALUE###') and upper(product) like "%FOND%") R
on receiver_msisdn= R.msisdn
left join 
(select msisdn from DIM.INTERNET_MSISDN_1) S
on receiver_msisdn= S.msisdn
left join
(select msisdn from DIM.msisdn_VISA) T
on receiver_msisdn= T.msisdn
left join
(select msisdn from DIM.MICROASSURANCE_MSISDNS) U
on receiver_msisdn= U.msisdn
left join
(select msisdn from DIM.msisdn_GIMAC union select msisdn from DIM.msisdn_GIMAC_REF) V
on sender_msisdn= V.msisdn
left join
(select msisdn from DIM.msisdn_GIMAC) W
on receiver_msisdn= W.msisdn
left join
(select msisdn from DIM.msisdn_GIMAC_REF) X
on receiver_msisdn= X.msisdn