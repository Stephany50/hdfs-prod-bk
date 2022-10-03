select msisdn,creation_date from (
    select a.*,b.*
    from (
        select msisdn msisdn3,upper(trim(concat_ws(' ',nvl(user_last_name,''),nvl(user_first_name,'')))) nom_prenom_om,upper(trim(id_number)) numero_piece_om, birth_date date_naissance_om,to_date(creation_date) creation_date
        from mon.spark_ft_omny_account_snapshot_new where event_date='###SLICE_VALUE###' and to_date(creation_date)='###SLICE_VALUE###'
    ) b
    left join (select msisdn, nom_prenom nom_prenom_telco, numero_piece numero_piece_telco,date_naissance date_naissance_telco,to_date(date_activation) date_activation from cdr.spark_it_kyc_bdi_full where original_file_date=DATE_ADD('###SLICE_VALUE###',1)) a 
    on a.msisdn = b.msisdn3
) a
where not(nom_prenom_telco = nom_prenom_om and substr(upper(trim(numero_piece_om)),1,9) = substr(upper(trim(numero_piece_telco)),1,9) and date_naissance_telco=date_naissance_om)
