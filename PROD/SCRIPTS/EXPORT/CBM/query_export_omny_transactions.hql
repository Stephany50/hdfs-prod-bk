SELECT
    sender_msisdn,
    receiver_msisdn,
    receiver_user_id,
    sender_user_id,
    transaction_amount,
    commissions_paid,
    commissions_received,
    commissions_others,
    service_charge_received,
    service_charge_paid,
    taxes,
    service_type,
    transfer_status,
    sender_pre_bal,
    sender_post_bal,
    receiver_pre_bal,
    receiver_post_bal,
    sender_acc_status,
    receiver_acc_status,
    error_code,
    error_desc,
    reference_number,
    created_on,
    created_by,
    modified_on,
    modified_by,
    app_1_date,
    app_2_date,
    transfer_id,
    transfer_datetime,
    sender_category_code,
    sender_domain_code,
    sender_grade_name,
    sender_group_role,
    sender_designation,
    sender_state,
    receiver_category_code,
    receiver_domain_code,
    receiver_grade_name,
    receiver_group_role,
    receiver_designation,
    receiver_state,
    sender_city,
    receiver_city,
    app_1_by,
    app_2_by,
    request_source,
    gateway_type,
    transfer_subtype,
    payment_type,
    payment_number,
    payment_date,
    remarks,
    action_type,
    transaction_tag,
    reconciliation_by,
    reconciliation_for,
    ext_txn_number,
    original_ref_number,
    zebra_ambiguous,
    attempt_status,
    other_msisdn,
    sender_wallet_number,
    receiver_wallet_number,
    sender_user_name,
    receiver_user_name,
    tno_msisdn,
    tno_id ,
    unreg_first_name,
    unreg_last_name,
    unreg_dob,
    unreg_id_number,
    bulk_payout_batchid,
    is_financial,
    transfer_done,
    initiator_msisdn,
    validator_msisdn,
    initiator_comments,
    validator_comments,
    sender_wallet_name,
    reciever_wallet_name,
    sender_user_type,
    receiver_user_type
FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
WHERE TRANSFER_DATETIME = '###SLICE_VALUE###'