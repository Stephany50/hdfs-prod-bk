INSERT INTO MON.SPARK_FT_CXD_NBRE_ACHATS_FORFAITS
SELECT 
    numero,
    nbre_transactions_om,
    nbre_achats_om ,
    nbre_achats_forfaits_om,
    nbre_echecs_achats_forfaits_om,
    duree_min_depot_fortait,
    duree_moyenne_depot_fortait,
    duree_mediane_depot_fortait,
    duree_max_depot_fortait,
    heure_min_echec_achat_forfait,
    heure_moyenne_echec_achat_forfait,
    heure_mediane_echec_achat_forfait,
    heure_max_echec_achat_forfait,
    current_timestamp insert_date,
    event_date
from
    (select 
        P1.sender_msisdn numero, 
        P1.nbre_trans_om nbre_transactions_om,
        IF(P3.nbre_achat_om is null,0 ,P3.nbre_achat_om) nbre_achats_om ,
        IF(P2.nbre_achats_forfaits_om is null,0 ,P2.nbre_achats_forfaits_om) nbre_achats_forfaits_om,
        IF(P4.nbre_achat_om_echec is null, 0, P4.nbre_achat_om_echec) nbre_echecs_achats_forfaits_om,
        PF.duree_min_depot_fortait duree_min_depot_fortait ,
        PF.duree_max_depot_fortait duree_max_depot_fortait,
        PF.duree_moyenne_depot_fortait duree_moyenne_depot_fortait,
        PF.duree_mediane_depot_fortait duree_mediane_depot_fortait,
        F5.heure_min_echec_achat_forfait,
        F5.heure_moyenne_echec_achat_forfait,
        F5.heure_mediane_echec_achat_forfait,
        F5.heure_max_echec_achat_forfait,
        P1.event_date
    from 
        (select 
            sender_msisdn,
            count(transfer_id) nbre_trans_om,
            transfer_datetime event_date 
        from cdr.spark_it_omny_transactions
        where transfer_datetime="###SLICE_VALUE###"  
        group by sender_msisdn,event_date )P1
    LEFT JOIN
    (select 
        count(*) nbre_achats_forfaits_om, 
        numero 
    from
        (select 
            trim(transfer_id) id_transfer_1,
            sender_msisdn numero ,
            transfer_datetime_nq 
        from cdr.spark_it_omny_transactions 
        where transfer_datetime="###SLICE_VALUE###" and upper(trim(transfer_status))='TS' and fn_format_msisdn_to_9digits(receiver_msisdn) in (select trim(msisdn) from dim.ref_compte_afd))P5
        group by numero)P2
    ON P1.sender_msisdn=P2.numero
    LEFT JOIN
    (select 
        sender_msisdn,
        count(transfer_id) nbre_achat_om 
    from cdr.spark_it_omny_transactions
    where transfer_datetime="###SLICE_VALUE###" and trim(upper(service_type)) in ('MERCHPAY','SUBMERPAY') 
    group by sender_msisdn)P3
    ON P1.sender_msisdn=P3.sender_msisdn
    LEFT JOIN
    (select 
    sender_msisdn,
    count(*) nbre_achat_om_echec
    from 
        (select 
            sender_msisdn,
            transfer_id
        from cdr.spark_it_omny_transactions
        where transfer_datetime="###SLICE_VALUE###"
        and transfer_id in
        (select trim(transfer_id) from cdr.spark_it_omny_transactions where transfer_datetime="###SLICE_VALUE###" and upper(trim(transfer_status))='TS' 
        and fn_format_msisdn_to_9digits(receiver_msisdn) in (select trim(msisdn) from dim.ref_compte_afd)
        minus
        (select trim(transactionsn) from CDR.SPARK_IT_ZTE_SUBSCRIPTION WHERE CREATEDDATE between '###SLICE_VALUE###' and date_add('###SLICE_VALUE###', 1))
        )
        )p group by sender_msisdn)P4
    ON P1.sender_msisdn=P4.sender_msisdn
    LEFT JOIN
    (select 
        F1.numero, 
        F1.duree_min_depot_fortait, 
        F1.duree_max_depot_fortait,
        F1.duree_moyenne_depot_fortait,
        F2.duree_mediane_depot_fortait 
    from
        (select 
            numero,
            min(duree)duree_min_depot_fortait , 
            max(duree) duree_max_depot_fortait, 
            avg(duree) duree_moyenne_depot_fortait 
        from
            (select 
            P5.sender_msisdn numero ,
            abs(round(unix_timestamp(P5.transfer_datetime_nq, 'yyyy-MM-dd HH:mm:ss')-unix_timestamp(P4.nq_createddate, 'yyyy-MM-dd HH:mm:ss'))) duree 
            from
                (select 
                    trim(transfer_id) id_transfer_1,
                    sender_msisdn,
                    transfer_datetime_nq 
                from cdr.spark_it_omny_transactions 
                where transfer_datetime="###SLICE_VALUE###" and upper(trim(transfer_status))='TS' and fn_format_msisdn_to_9digits(receiver_msisdn) in (select trim(msisdn) from dim.ref_compte_afd))P5
                inner join 
                (select 
                trim(transactionsn) id_transfer_2,
                active_date,
                nq_createddate 
                from CDR.SPARK_IT_ZTE_SUBSCRIPTION 
                WHERE CREATEDDATE between '###SLICE_VALUE###' and date_add('###SLICE_VALUE###', 1))P4
                on P4.id_transfer_2 = P5.id_transfer_1)
        group by numero)F1

    INNER JOIN 
    (SELECT 
    numero, 
    avg(duree) as duree_mediane_depot_fortait
    FROM ( SELECT numero, duree, rN, (CASE WHEN cN % 2 = 0 then (cN DIV 2) ELSE (cN DIV 2) + 1 end) as m1, (cN DIV 2) + 1 as m2 
            FROM ( 
                SELECT numero, duree, row_number() OVER (PARTITION BY numero ORDER BY duree ) as rN, count(duree) OVER (PARTITION BY numero ) as cN
                FROM (select P5.sender_msisdn numero ,abs(round(unix_timestamp(P5.transfer_datetime_nq, 'yyyy-MM-dd HH:mm:ss')-unix_timestamp(P4.nq_createddate, 'yyyy-MM-dd HH:mm:ss'))) duree from
                        (select trim(transfer_id) id_transfer_1,sender_msisdn,transfer_datetime_nq from cdr.spark_it_omny_transactions where transfer_datetime="###SLICE_VALUE###" and upper(trim(transfer_status))='TS' 
                        and fn_format_msisdn_to_9digits(receiver_msisdn) in (select trim(msisdn) from dim.ref_compte_afd))P5
                        inner join (select trim(transactionsn) id_transfer_2,active_date,nq_createddate from CDR.SPARK_IT_ZTE_SUBSCRIPTION WHERE CREATEDDATE between '###SLICE_VALUE###' and date_add('###SLICE_VALUE###', 1))P4
                        on P4.id_transfer_2 = P5.id_transfer_1)
            ) s
        ) r
    WHERE rN BETWEEN m1 and m2
    GROUP BY numero )F2
    ON F1.numero=F2.numero)PF
    ON P1.sender_msisdn=PF.numero 
    LEFT JOIN
    (select 
        F1.sender_msisdn,
        F1.heure_min_echec_achat_forfait,
        F1.heure_moyenne_echec_achat_forfait,
        F1.heure_max_echec_achat_forfait,
        F2.heure_mediane_echec_achat_forfait 
    from
        (select 
            sender_msisdn,
            substr(min(transfer_datetime_nq),12,9)heure_min_echec_achat_forfait , 
            substr(max(transfer_datetime_nq),12,9) heure_max_echec_achat_forfait, 
            substr(FROM_UNIXTIME(AVG(UNIX_TIMESTAMP(transfer_datetime_nq, 'yyyy-MM-dd HH:mm:ss')),'yyyy-MM-dd HH:mm:ss'),12,9)heure_moyenne_echec_achat_forfait
        from
            (select 
                sender_msisdn,
                transfer_datetime_nq 
            from cdr.spark_it_omny_transactions
            where transfer_datetime="###SLICE_VALUE###"
            and transfer_id in
            (select trim(transfer_id) from cdr.spark_it_omny_transactions where transfer_datetime="###SLICE_VALUE###" and upper(trim(transfer_status))='TS' 
            and fn_format_msisdn_to_9digits(receiver_msisdn) in (select trim(msisdn) from dim.ref_compte_afd)
            minus
            (select trim(transactionsn) from CDR.SPARK_IT_ZTE_SUBSCRIPTION WHERE CREATEDDATE between '###SLICE_VALUE###' and date_add('###SLICE_VALUE###', 1))
            )
            )FF
        group by sender_msisdn)F1

    INNER JOIN 
    (SELECT 
        sender_msisdn,  
        substr(FROM_UNIXTIME(AVG(UNIX_TIMESTAMP(transfer_datetime_nq, 'yyyy-MM-dd HH:mm:ss')),'yyyy-MM-dd HH:mm:ss'),12,9) as heure_mediane_echec_achat_forfait
        FROM ( SELECT sender_msisdn, transfer_datetime_nq, rN, (CASE WHEN cN % 2 = 0 then (cN DIV 2) ELSE (cN DIV 2) + 1 end) as m1, (cN DIV 2) + 1 as m2 
                FROM ( 
                    SELECT sender_msisdn, transfer_datetime_nq, row_number() OVER (PARTITION BY sender_msisdn ORDER BY transfer_datetime_nq ) as rN, count(transfer_datetime_nq) OVER (PARTITION BY sender_msisdn ) as cN
                    FROM (select sender_msisdn,transfer_datetime_nq 
                            from cdr.spark_it_omny_transactions
                            where transfer_datetime="###SLICE_VALUE###"
                            and transfer_id in
                            (select trim(transfer_id) from cdr.spark_it_omny_transactions where transfer_datetime="###SLICE_VALUE###" and upper(trim(transfer_status))='TS' 
                            and fn_format_msisdn_to_9digits(receiver_msisdn) in (select trim(msisdn) from dim.ref_compte_afd)
                            minus
                            (select trim(transactionsn) from CDR.SPARK_IT_ZTE_SUBSCRIPTION WHERE CREATEDDATE between '###SLICE_VALUE###' and date_add('###SLICE_VALUE###', 1))
                            )
                            )F1
                ) s
            ) r
    WHERE rN BETWEEN m1 and m2
    GROUP BY sender_msisdn )F2
    ON F1.sender_msisdn=F2.sender_msisdn)F5
    ON P1.sender_msisdn=F5.sender_msisdn
    )PP
