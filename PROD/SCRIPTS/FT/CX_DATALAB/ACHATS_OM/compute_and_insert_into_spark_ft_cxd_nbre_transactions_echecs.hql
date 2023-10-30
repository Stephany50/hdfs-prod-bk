INSERT INTO MON.SPARK_FT_CXD_NBRE_TRANSACTIONS_ECHECS
select 
    numero,
    nbre_echecs_transactions_om,
    (CASE WHEN erreur_echec is null and  nbre_echecs_transactions_om=0 THEN 'NO' ELSE erreur_echec END) erreur_echec,
    current_timestamp insert_date,
    event_date
from
    (select
        P2.sender_msisdn numero, 
        P2.error_desc erreur_echec,
        IF(P2.nbre_trans_om_echec is null,0,P2.nbre_trans_om_echec) nbre_echecs_transactions_om, 
        current_timestamp insert_date,
        P2.event_date
    from 
        (select 
            sender_msisdn,
            count(transfer_id) nbre_trans_om_echec,
            error_desc,
            transfer_datetime event_date 
        from cdr.spark_it_omny_transactions
        where transfer_datetime="###SLICE_VALUE###" and upper(trim(transfer_status))='TF' 
        group by sender_msisdn,error_desc,event_date)P2)KK 
where trim(upper(numero)) <> 'NA'