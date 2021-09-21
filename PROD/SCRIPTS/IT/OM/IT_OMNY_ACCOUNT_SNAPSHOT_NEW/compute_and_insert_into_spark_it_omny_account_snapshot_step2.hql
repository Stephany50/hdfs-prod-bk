---Etape 2 : Mise à jour des soldes de chaque compte suivant le format (solde1 | solde 2 | solde 3)
INSERT INTO CDR.SPARK_IT_OMNY_ACCOUNT_SNAPSHOT_NEW
SELECT 
    a.user_name,a.last_name,
    a.created_on,a.modified_on,
    a.last_transfer_on,a.msisdn,
    a.user_id,concat_ws('|',collect_list(b.balance)) as balance,
    concat_ws('|',collect_list(b.payment_type_id)) as payment_type_id,
    a.external_code,a.is_active,
    a.birth_date,a.user_type,
    a.user_domain,a.user_category,
    a.original_file_name,a.original_file_size,
    a.original_file_line_count,
    current_timestamp insert_date,
    '###SLICE_VALUE###' original_file_date
FROM TMP.TT_SPARK_IT_OMNY_ACCOUNT_SNAPSHOT a
LEFT JOIN (select * from cdr.spark_it_om_all_balance where original_file_date=DATE_SUB("###SLICE_VALUE###",1)) b 
on upper(trim(a.user_id)) = upper(trim(b.account_name)) and trim(a.msisdn) = trim(b.account_id)
GROUP BY 
    a.user_name,a.last_name,a.created_on,
    a.modified_on,a.last_transfer_on,
    a.external_code,a.is_active,a.birth_date,a.user_type,
    a.user_domain,a.user_category,a.original_file_name,
    a.original_file_size,a.original_file_line_count,
    a.msisdn,a.user_id
    