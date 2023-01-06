insert into mon.spark_ft_cbm_quickeval
select msisdn, -- ok
    temoin, -- ok
    ipp1, -- ok
    ipp2, -- ok
    ipp3, -- ok
    problematique, -- ok
    takers1, -- ok
    takers2, -- ok
    takers3, -- ok
    takers, --max(taker1, taker2, taker3) -- ok
    nb_souscriptions_periode1, -- ok
    nb_souscriptions_periode2, -- ok
    ca_forfait_periode1, -- chiffre d affaire genere sur la periode 1 venant de soit ipp1, ipp2 ou ipp3
    ca_forfait_periode2,
    arpu_periode1, -- arpu total sur toute les transanctions dans toutes la periode 1 -- ok
    arpu_periode2, -- ok
    arpu_data_periode1, -- ok
    arpu_data_periode2, -- ok
    arpu_voix_periode1, -- ok
    arpu_voix_periode2, -- ok
    volume_data_periode1, -- ok
    volume_data_periode2, -- ok
    mou_periode1, -- ok
    mou_periode2, -- ok
    activite_data, -- ok
    activite_voix, -- ok
    activite_globale, --max(activite_data,activite_voix) -- ok
    (ca_forfait_periode2/ca_forfait_periode2)-1 evol_ca_forfait, -- ok
    (arpu_periode2/arpu_periode1)-1 evol_revenu, -- ok
    (arpu_data_periode2/arpu_data_periode1)-1 evol_data, -- ok
    (arpu_voix_periode2/arpu_voix_periode1)-1 evol_voix, -- ok
    (volume_data_periode2/volume_data_periode1)-1 evol_volume_data, -- ok
    (mou_periode2/mou_periode1)-1 evol_mou, -- ok
    code_campagne, -- colonne de partition -- ok
    current_timestamp event_time -- ok
from
    (
        select msisdn,
            temoin,
            ipp1,
            ipp2,
            ipp3,
            problematique,
            if(ipp1<>null, 1, 0) taker1,
            if(ipp2<>null, 1, 0) taker2,
            if(ipp3<>null, 1, 0) taker3,
            greatest(if(ipp1<>null, 1, 0), if(ipp2<>null, 1, 0), if(ipp3<>null, 1, 0)) takers,
            periode1,
            periode2,
            code_campagne
        from tt.cbm_input_quickeval
    ) input_data 
    left join
    (
        select served_party_msisdn msisdn,
            count(served_party_msisdn) nb_souscriptions_periode1,
            sum(rated_amount) ca_forfait_periode1
        from mon.spark_ft_subscription
        left join tt.cbm_input_quickeval on input_data.msisdn=subs1.msisdn
        where original_file_name not like '%in_postpaid%' and transaction_date between to_date(split(tt.cbm_input_quickeval.periode1, '/')[0]) and to_date(split(tt.cbm_input_quickeval.periode1, '/')[1])
        and upper(trim(subscription_service_details)) in (select distinct upper(trim(nvl(ipp1, ''))), upper(trim(nvl(ipp2, ''))), upper(trim(nvl(ipp3, ''))) from tt.cbm_input_quickeval limit 1)
        group by msisdn
    ) subs1 on input_data.msisdn=subs1.msisdn
    left join
    (
        select served_party_msisdn msisdn,
            count(served_party_msisdn) nb_souscriptions_periode2,
            sum(rated_amount) ca_forfait_periode2
        from mon.spark_ft_subscription
        where original_file_name not like '%in_postpaid%' and transaction_date between to_date(split(tt.cbm_input_quickeval.periode2, '/')[0]) and to_date(split(tt.cbm_input_quickeval.periode2, '/')[1])
        and upper(trim(subscription_service_details)) in (select distinct upper(trim(nvl(ipp1, ''))), upper(trim(nvl(ipp2, ''))), upper(trim(nvl(ipp3, ''))) from tt.cbm_input_quickeval limit 1)
        group by msisdn
    ) subs2 on input_data.msisdn=subs2.msisdn
    left join
    (
        select msisdn,
            sum(arpu) arpu_periode1,
            sum(arpu_voix) arpu_voix_periode1,
            sum(arpu_data) arpu_data_periode1,
            sum(volume_data) volume_data_periode1,
            sum(mou) mou_periode1
        from mon.spark_ft_cbm_arpu_mou
        group by msisdn
    ) arpu1 on input_data.msisdn=arpu1.msisdn
    left join
    (
        select msisdn,
            arpu_periode2,
            arpu_voix_periode2,
            arpu_data_periode2,
            volume_data_periode2,
            mou_periode2,
            if(volume_data_periode2>1, 1, 0) activite_data,
            if(mou_periode2>0, 1, 0) activite_voix,
            greatest(if(volume_data_periode2>1, 1, 0), if(mou_periode2>0, 1, 0)) activite_globale
        from 
        (
            select msisdn,
                sum(arpu) arpu_periode2,
                sum(arpu_voix) arpu_voix_periode2,
                sum(arpu_data) arpu_data_periode2,
                sum(volume_data) volume_data_periode2,
                sum(mou) mou_periode2
            from mon.spark_ft_cbm_arpu_mou
            group by msisdn
        ) z
    ) arpu2 on input_data.msisdn=arpu2.msisdn
    limit 10;