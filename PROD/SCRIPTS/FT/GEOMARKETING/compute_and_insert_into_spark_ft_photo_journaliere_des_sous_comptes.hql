insert into mon.spark_ft_photo_journaliere_des_sous_comptes
select
    nvl(photo_a_j.msisdn, photo_a_j_moins_un.msisdn) msisdn
    , nvl(photo_a_j.bal_id, photo_a_j_moins_un.bal_id) bal_id
    , nvl(max(photo_a_j.transaction_time), max(photo_a_j_moins_un.transaction_time)) transaction_time
    , nvl(max(photo_a_j.acct_res_id), max(photo_a_j_moins_un.acct_res_id)) acct_res_id
    , nvl(max(photo_a_j.acct_res_rating_unit), max(photo_a_j_moins_un.acct_res_rating_unit)) acct_res_rating_unit
    , nvl(max(photo_a_j.politic), max(photo_a_j_moins_un.politic)) politic
    , nvl(max(photo_a_j.bdle_name), max(photo_a_j_moins_un.bdle_name)) bdle_name
    , (
        case             
            when upper(nvl(max(photo_a_j.politic),max( photo_a_j_moins_un.politic))) in ('ECRASE') then 
                case
                    when(upper(max(photo_a_j.politic)) in ('ECRASE')) then nvl(nvl(sum(photo_a_j.prix_total), 0), 0) + nvl(sum(photo_a_j_moins_un.prix_total), 0)
                    else nvl(sum(photo_a_j_moins_un.prix_total), 0)
                end
            when upper(nvl(max(photo_a_j.politic), max(photo_a_j_moins_un.politic))) in ('CUMUL') then nvl(nvl(sum(photo_a_j.prix_total), 0), 0) + nvl(sum(photo_a_j_moins_un.prix_total), 0)
            else nvl(sum(photo_a_j.prix_total), 0) + nvl(sum(photo_a_j_moins_un.prix_total), 0)
        end
    ) prix_total
    , (
        case 
            when upper(nvl(max(photo_a_j.politic),max( photo_a_j_moins_un.politic))) in ('ECRASE') then 
                case
                    when(upper(max(photo_a_j.politic)) in ('ECRASE')) then nvl(nvl(sum(photo_a_j.volume_total), 0), 0) + nvl(sum(photo_a_j_moins_un.cumul_usage), 0)
                    else nvl(sum(photo_a_j_moins_un.volume_total), 0)
                end
            when upper(nvl(max(photo_a_j.politic), max(photo_a_j_moins_un.politic))) in ('CUMUL') then nvl(nvl(sum(photo_a_j.volume_total), 0), 0) + nvl(sum(photo_a_j_moins_un.volume_total), 0)
            else nvl(nvl(sum(photo_a_j.volume_total), 0), 0) + nvl(sum(photo_a_j_moins_un.volume_total), 0)
        end
    ) volume_total
    ,(
        case
            when upper(nvl(max(photo_a_j.politic),max( photo_a_j_moins_un.politic))) in ('ECRASE') then if(
                date_format(nvl(max(bal_extract.expiration_date), max(photo_a_j_moins_un.expiration_date)), 'HH:mm:ss')='00:00:00', 
                ceil((unix_timestamp(to_timestamp(nvl(max(bal_extract.expiration_date), max(photo_a_j_moins_un.expiration_date))))-unix_timestamp(to_timestamp(nvl(max(bal_extract.initial_date), max(photo_a_j_moins_un.initial_date)))))/86400), 
                ceil((unix_timestamp(to_timestamp(date_add(nvl(max(bal_extract.expiration_date), max(photo_a_j_moins_un.expiration_date)), 1)))-unix_timestamp(to_timestamp(nvl(max(bal_extract.initial_date), max(photo_a_j_moins_un.initial_date)))))/86400))
            when upper(nvl(max(photo_a_j.politic), max(photo_a_j_moins_un.politic))) in ('CUMUL') then if(
                date_format(nvl(max(bal_extract.expiration_date), max(photo_a_j_moins_un.expiration_date)), 'HH:mm:ss')='00:00:00', 
                ceil((unix_timestamp(to_timestamp(nvl(max(bal_extract.expiration_date), max(photo_a_j_moins_un.expiration_date))))-unix_timestamp(to_timestamp(nvl(max(bal_extract.initial_date), max(photo_a_j_moins_un.initial_date)))))/86400), 
                ceil((unix_timestamp(to_timestamp(date_add(nvl(max(bal_extract.expiration_date), max(photo_a_j_moins_un.expiration_date)), 1)))-unix_timestamp(to_timestamp(nvl(max(bal_extract.initial_date), max(photo_a_j_moins_un.initial_date)))))/86400))
            else if(
                date_format(nvl(max(bal_extract.expiration_date), max(photo_a_j_moins_un.expiration_date)), 'HH:mm:ss')='00:00:00', 
                ceil((unix_timestamp(to_timestamp(nvl(max(bal_extract.expiration_date), max(photo_a_j_moins_un.expiration_date))))-unix_timestamp(to_timestamp(nvl(max(bal_extract.initial_date), max(photo_a_j_moins_un.initial_date)))))/86400), 
                ceil((unix_timestamp(to_timestamp(date_add(nvl(max(bal_extract.expiration_date), max(photo_a_j_moins_un.expiration_date)), 1)))-unix_timestamp(to_timestamp(nvl(max(bal_extract.initial_date), max(photo_a_j_moins_un.initial_date)))))/86400))
        end
    ) duree_bundle
    , (
        case
            when upper(nvl(max(photo_a_j.politic), max(photo_a_j_moins_un.politic))) in ('ECRASE') then nvl(min(photo_a_j_moins_un.initial_date), min(bal_extract.initial_date))
            when upper(nvl(max(photo_a_j.politic), max(photo_a_j_moins_un.politic))) in ('CUMUL') then nvl(min(photo_a_j_moins_un.initial_date), min(bal_extract.initial_date))
            else nvl(min(photo_a_j_moins_un.initial_date), min(bal_extract.initial_date))
        end
    ) initial_date
    , nvl(nvl(max(bal_extract.expiration_date), max(photo_a_j_moins_un.expiration_date)), '###SLICE_VALUE###')  expiration_date
    , (
        case 
            when upper(nvl(max(photo_a_j.politic), max(photo_a_j_moins_un.politic))) in ('ECRASE') then nvl(max(photo_a_j_moins_un.cumul_usage), 0) + nvl(max(table_des_usages.usage_a_j), 0)
            when upper(nvl(max(photo_a_j.politic), max(photo_a_j_moins_un.politic))) in ('CUMUL') then nvl(max(photo_a_j_moins_un.cumul_usage), 0) + nvl(max(table_des_usages.usage_a_j), 0)
            else nvl(max(photo_a_j_moins_un.cumul_usage), 0) + nvl(max(table_des_usages.usage_a_j), 0)
        end
    ) cumul_usage
    ,(
        case 
            when upper(nvl(max(photo_a_j.politic),max( photo_a_j_moins_un.politic))) in ('ECRASE') then 
                case
                    when(upper(max(photo_a_j.politic)) in ('ECRASE')) then (nvl(nvl(sum(photo_a_j.volume_total), 0), 0) + nvl(sum(photo_a_j_moins_un.cumul_usage), 0)) - (nvl(max(photo_a_j_moins_un.cumul_usage), 0) + nvl(max(table_des_usages.usage_a_j), 0))
                    else (nvl(sum(photo_a_j_moins_un.volume_total), 0)) - (nvl(max(photo_a_j_moins_un.cumul_usage), 0) + nvl(max(table_des_usages.usage_a_j), 0))
                end
            when upper(nvl(max(photo_a_j.politic), max(photo_a_j_moins_un.politic))) in ('CUMUL') then (nvl(nvl(sum(photo_a_j.volume_total), 0), 0) + nvl(sum(photo_a_j_moins_un.volume_total), 0)) - (nvl(max(photo_a_j_moins_un.cumul_usage), 0) + nvl(max(table_des_usages.usage_a_j), 0))
            else (nvl(nvl(sum(photo_a_j.volume_total), 0), 0) + nvl(sum(photo_a_j_moins_un.volume_total), 0)) - (nvl(max(photo_a_j_moins_un.cumul_usage), 0) + nvl(max(table_des_usages.usage_a_j), 0))
        end
    ) volume_restant
    , (
        case 
            when upper(nvl(max(photo_a_j.politic),max( photo_a_j_moins_un.politic))) in ('ECRASE') then
                case
                    when(upper(max(photo_a_j.politic)) in ('ECRASE'))  then nvl(max(table_des_valorisations.valorisation_a_j), 0) + nvl(max(photo_a_j_moins_un.cumul_valorisation), 0)
                    else nvl(max(table_des_valorisations.valorisation_a_j), 0) + nvl(max(photo_a_j_moins_un.cumul_valorisation), 0)
                end
            when upper(nvl(max(photo_a_j.politic), max(photo_a_j_moins_un.politic))) in ('CUMUL') then nvl(max(table_des_valorisations.valorisation_a_j), 0) + nvl(max(photo_a_j_moins_un.cumul_valorisation), 0)
            else nvl(max(table_des_valorisations.valorisation_a_j), 0) + nvl(max(photo_a_j_moins_un.cumul_valorisation), 0)
        end
    ) cumul_valorisation
    , (
        case             
            when upper(nvl(max(photo_a_j.politic),max( photo_a_j_moins_un.politic))) in ('ECRASE') then 
                case
                    when(upper(max(photo_a_j.politic)) in ('ECRASE')) then (nvl(nvl(sum(photo_a_j.prix_total), 0), 0) + nvl(sum(photo_a_j_moins_un.prix_total), 0)) - (nvl(max(table_des_valorisations.valorisation_a_j), 0) + nvl(max(photo_a_j_moins_un.cumul_valorisation), 0))
                    else (nvl(nvl(sum(photo_a_j.prix_total), 0), 0) + nvl(sum(photo_a_j_moins_un.prix_total), 0)) - (nvl(max(table_des_valorisations.valorisation_a_j), 0) + nvl(max(photo_a_j_moins_un.cumul_valorisation), 0))
                end
            when upper(nvl(max(photo_a_j.politic), max(photo_a_j_moins_un.politic))) in ('CUMUL') then (nvl(nvl(sum(photo_a_j.prix_total), 0), 0) + nvl(sum(photo_a_j_moins_un.prix_total), 0)) - (nvl(max(table_des_valorisations.valorisation_a_j), 0) + nvl(max(photo_a_j_moins_un.cumul_valorisation), 0))
            else (nvl(nvl(sum(photo_a_j.prix_total), 0), 0) + nvl(sum(photo_a_j_moins_un.prix_total), 0)) - (nvl(max(table_des_valorisations.valorisation_a_j), 0) + nvl(max(photo_a_j_moins_un.cumul_valorisation), 0)) 
        end
    ) valorisation_restante
    , (
        case
            when upper(nvl(max(photo_a_j.politic),max( photo_a_j_moins_un.politic))) in ('ECRASE')  then if(
                date_format(nvl(max(bal_extract.expiration_date), max(photo_a_j_moins_un.expiration_date)), 'HH:mm:ss')='00:00:00', 
                ceil((unix_timestamp(to_timestamp('###SLICE_VALUE###'))-unix_timestamp(to_timestamp(nvl(min(photo_a_j_moins_un.initial_date), min(bal_extract.initial_date)))))/86400), 
                ceil((unix_timestamp(to_timestamp('###SLICE_VALUE###'))-unix_timestamp(to_timestamp(nvl(min(photo_a_j_moins_un.initial_date), min(bal_extract.initial_date)))))/86400)+1)
            when upper(nvl(max(photo_a_j.politic), max(photo_a_j_moins_un.politic))) in ('CUMUL') then if(
                date_format(nvl(max(bal_extract.expiration_date), max(photo_a_j_moins_un.expiration_date)), 'HH:mm:ss')='00:00:00', 
                ceil((unix_timestamp(to_timestamp('###SLICE_VALUE###'))-unix_timestamp(to_timestamp(nvl(min(photo_a_j_moins_un.initial_date), min(bal_extract.initial_date)))))/86400), 
                ceil((unix_timestamp(to_timestamp('###SLICE_VALUE###'))-unix_timestamp(to_timestamp(nvl(min(photo_a_j_moins_un.initial_date), min(bal_extract.initial_date)))))/86400)+1)
            else if(
                date_format(nvl(max(bal_extract.expiration_date), max(photo_a_j_moins_un.expiration_date)), 'HH:mm:ss')='00:00:00', 
                ceil((unix_timestamp(to_timestamp('###SLICE_VALUE###'))-unix_timestamp(to_timestamp(nvl(min(photo_a_j_moins_un.initial_date), min(bal_extract.initial_date)))))/86400), 
                ceil((unix_timestamp(to_timestamp('###SLICE_VALUE###'))-unix_timestamp(to_timestamp(nvl(min(photo_a_j_moins_un.initial_date), min(bal_extract.initial_date)))))/86400)+1)
        end
    ) nb_jours_ecoules
    , current_timestamp insert_date
    , '###SLICE_VALUE###' event_date
from
(
    select a.msisdn msisdn
        , a.bal_id bal_id
        , max(a.bdle_name) bdle_name
        , max(transaction_time) transaction_time
        , max(a.acct_res_id) acct_res_id
        , max(acct_res_rating_unit) acct_res_rating_unit
        , max(politic) politic
        ,sum(bdle_cost) prix_total
        , sum(volume_total)/1000 volume_total -- pour reduire les volumes
        -- , usage_a_j
        -- , valorisation_a_j
        , sum(number_updates_of_bal_in_day) number_updates_of_bal_in_day
    from
    (
        select
            subs_of_today.msisdn msisdn
            , nvl(price_plan.price_plan_name, cast(subs_of_today.price_plan_code as string)) bdle_name
            , subs_of_today.bal_id_col bal_id
            -- , table_des_usages.tricky_bal_id tricky_bal_id
            , transaction_time
            , ben_acct_id acct_res_id
            -- , initial_date initial_date
            -- , expiration_date expiration_date
            , acct_res_rating_unit acct_res_rating_unit
            , volume_total volume_total
            -- , usage_a_j usage_a_j
            -- , valorisation_a_j valorisation_a_j
            , max(
                case
                    when hybipp.offer_name is not null then 0
                    when services_dynamique.msisdn is not null then nvl(cast(services_dynamique.bdle_cost as int),0)
                    when nvl(price_plan.price_plan_name,cast(subs_of_today.price_plan_code as string)) = services_default.bdle_name and services_dynamique.msisdn  is null and subs_of_today.channel_id in ('32', '111') then nvl(cast(services_default.bdle_cost as INT), 0)
                    when subs_of_today.channel_id in ('32', '111') and services_dynamique.bdle_name is null and services_default.bdle_name is null then nvl(amount_via_om_vas, 0)
                    else event_cost / 100
                end
            ) * sum(number_updates_of_bal_in_day) bdle_cost ------------- En attente du reférentiel décrit ci-bas
            , sum(number_updates_of_bal_in_day) number_updates_of_bal_in_day
        from
        (
            select
                substring(acc_nbr, -9) msisdn
                , bal_id_col
                , nq_createddate transaction_time
                , cast(event_cost as DECIMAL(30, 8)) event_cost
                , cast(split(ben_bal, '&')[0] as bigint) ben_acct_id --- va servir pour joindre au reférentiel ci bas du moins soit ca, soit le acct_res_name soit le acct_res_std_code
                , cast(abs(cast(split(ben_bal, '&')[1] as DECIMAL(30, 8))) as DECIMAL(30, 8)) volume_total
                , subs_event_id subs_event_id
                , channel_id channel_id
                , price_plan_code price_plan_code
                , old_price_plan_code old_price_plan_code
                , prod_spec_code prod_spec_code
                , old_prod_spec_code old_prod_spec_code
                , related_prod_code related_prod_code
                , sum(number_updates_of_bal_in_day) number_updates_of_bal_in_day
            from
            (
                select
                    acc_nbr
                    , channel_id
                    , subs_event_id
                    , nq_createddate
                    , old_subs_state
                    , new_subs_state
                    , event_cost
                    , benefit_name
                    , benefit_bal_list
                    , old_prod_spec_code
                    , prod_spec_code
                    , old_price_plan_code
                    , price_plan_code
                    , old_related_prod_code
                    , related_prod_code
                    , active_date
                    , expired_date
                    , provider_id
                    , prepay_flag
                    , payment_number
                    , subscription_cost
                    , transactionsn
                    , original_file_name
                    , original_file_date
                    , original_file_size
                    , original_file_line_count
                    , insert_date
                    , exp_lac
                    , exp_cell_id
                    , bal_id
                    , createddate
                    , file_date
                    , count(*) number_updates_of_bal_in_day 
                from
                (
                    select acc_nbr
                        , channel_id
                        , subs_event_id
                        , nq_createddate
                        , old_subs_state
                        , new_subs_state
                        , event_cost
                        , benefit_name
                        , benefit_bal_list
                        , old_prod_spec_code
                        , prod_spec_code
                        , old_price_plan_code
                        , price_plan_code
                        , old_related_prod_code
                        , related_prod_code
                        , active_date
                        , expired_date
                        , provider_id
                        , prepay_flag
                        , payment_number
                        , subscription_cost
                        , transactionsn
                        , original_file_name
                        , original_file_date
                        , original_file_size
                        , original_file_line_count
                        , insert_date
                        , exp_lac
                        , exp_cell_id
                        , bal_id
                        , createddate
                        , file_date
                    from cdr.spark_it_zte_subscription
                    where createddate = '###SLICE_VALUE###' and original_file_name not like '%in_postpaid%' -- and benefit_bal_list is not null
                    group by acc_nbr
                        , channel_id
                        , subs_event_id
                        , nq_createddate
                        , old_subs_state
                        , new_subs_state
                        , event_cost
                        , benefit_name
                        , benefit_bal_list
                        , old_prod_spec_code
                        , prod_spec_code
                        , old_price_plan_code
                        , price_plan_code
                        , old_related_prod_code
                        , related_prod_code
                        , active_date
                        , expired_date
                        , provider_id
                        , prepay_flag
                        , payment_number
                        , subscription_cost
                        , transactionsn
                        , original_file_name
                        , original_file_date
                        , original_file_size
                        , original_file_line_count
                        , insert_date
                        , exp_lac
                        , exp_cell_id
                        , bal_id
                        , createddate
                        , file_date
                ) a00
                group by acc_nbr
                    , channel_id
                    , subs_event_id
                    , nq_createddate
                    , old_subs_state
                    , new_subs_state
                    , event_cost
                    , benefit_name
                    , benefit_bal_list
                    , old_prod_spec_code
                    , prod_spec_code
                    , old_price_plan_code
                    , price_plan_code
                    , old_related_prod_code
                    , related_prod_code
                    , active_date
                    , expired_date
                    , provider_id
                    , prepay_flag
                    , payment_number
                    , subscription_cost
                    , transactionsn
                    , original_file_name
                    , original_file_date
                    , original_file_size
                    , original_file_line_count
                    , insert_date
                    , exp_lac
                    , exp_cell_id
                    , bal_id
                    , createddate
                    , file_date
            ) a0
            lateral view posexplode(split(nvl(bal_id, ''), ',')) tmp1 as pos_bal_id_col, bal_id_col
            lateral view posexplode(split(nvl(benefit_bal_list, ''), '#')) tmp2 as pos_ben_bal, ben_bal 
            -- where pos_bal_id_col = pos_BEN_BAL and bal_id_col is not null and bal_id_col != ''
            where pos_bal_id_col='0' and pos_bal_id_col = pos_BEN_BAL and bal_id_col is not null --and bal_id_col != ''
            group by acc_nbr
                , bal_id_col
                , nq_createddate
                , event_cost
                , ben_bal
                , subs_event_id
                , channel_id
                , price_plan_code
                , old_price_plan_code
                , prod_spec_code
                , old_prod_spec_code
                , related_prod_code
        ) subs_of_today -- recuperer les gars qui ont fait les souscriptions aujourd'hui
        left join
        (
            select 
                acct_res_id
                , max(acct_res_name) acct_res_name
                , max(acct_res_rating_service_code) acct_res_rating_service_code
                , max(acct_res_rating_unit) acct_res_rating_unit
            from dim.dt_balance_type_item
            group by acct_res_id
        ) balance_type_item on subs_of_today.ben_acct_id = cast(balance_type_item.acct_res_id as bigint)
        left join dim.dt_subscription_service servsubsc on nvl(subs_of_today.subs_event_id, 1000000) = servsubsc.subscription_service_id
        left join dim.dt_subscription_channel chansubsc on nvl(subs_of_today.channel_id, 1000000) = chansubsc.channel_id
        left join
        (
            select
                nvl(price_plan_code, 'UNKNOWN') price_plan_code
                , min( price_plan_name) price_plan_name
            from cdr.spark_it_zte_price_plan_extract
            where original_file_date = '###SLICE_VALUE###'
            group by nvl(price_plan_code, 'UNKNOWN')
        ) price_plan on nvl(subs_of_today.price_plan_code, '1000000') = price_plan.price_plan_code
        left join 
        (
            select
                nvl(price_plan_code, 'UNKNOWN') price_plan_code
                , min(price_plan_name) price_plan_name
            from cdr.spark_it_zte_price_plan_extract
            where original_file_date = '###SLICE_VALUE###'
            group by nvl(price_plan_code, 'UNKNOWN')
        ) old_price_plan on nvl(subs_of_today.old_price_plan_code, '1000000') = old_price_plan.price_plan_code
        left join 
        (
            select
                std_code
                , min(prod_spec_name) prod_spec_name
            from cdr.spark_it_zte_prod_spec_extract
            where original_file_date = '###SLICE_VALUE###'
            group by std_code
        ) prod_spec on nvl(subs_of_today.prod_spec_code, '1000000') =  prod_spec.std_code
        left join 
        (
            select
                std_code
                , min(prod_spec_name) prod_spec_name
            from cdr.spark_it_zte_prod_spec_extract
            where original_file_date = '###SLICE_VALUE###'
            group by std_code
        ) rel_prod on nvl(subs_of_today.related_prod_code, '1000000') = rel_prod.std_code
        left join 
        (
            select
                std_code
                , min(prod_spec_name) prod_spec_name
            from cdr.spark_it_zte_prod_spec_extract
            where original_file_date = '###SLICE_VALUE###'
            group by std_code
        ) old_prod_spec on nvl(subs_of_today.old_prod_spec_code, '1000000') =  old_prod_spec.std_code
        left join 
        (
            select
                event
                , max(service_code) service_code
                , min(voix_onnet) voix_onnet
                , min(voix_offnet) voix_offnet
                , min(voix_inter)voix_inter
                , min(voix_roaming)voix_roaming
                , min(sms_onnet) sms_onnet
                , min(sms_offnet) sms_offnet
                , min(sms_inter) sms_inter
                , min(sms_roaming)sms_roaming
                , min(data_bundle) data_bundle
                , min(sva) sva
                , min(prix) prix
                , min(combo) combo
            from dim.dt_services dtsvs group by event 
        ) dtsvs on nvl(price_plan.price_plan_name, servsubsc.subscription_service_name) = dtsvs.event
        LEFT JOIN -- ce left join donne le prix des souscriptions qui sont effectuées via OM parceque ce prix ne remonte pas dans les cdr de souscriptions quand on paye par OM
        (
            SELECT
                MAX(IPP_AMOUNT) AMOUNT_VIA_OM_VAS
                , IPP_NAME
            FROM CDR.SPARK_IT_ZTE_IPP_EXTRACT IPP_EXTRACT
            WHERE IPP_EXTRACT.ORIGINAL_FILE_DATE IN (SELECT MAX(ORIGINAL_FILE_DATE) ORIGINAL_FILE_DATE FROM CDR.SPARK_IT_ZTE_IPP_EXTRACT)
            GROUP BY IPP_NAME
        ) IPP_EXTRACT ON NVL(PRICE_PLAN.PRICE_PLAN_NAME, SERVSUBSC.SUBSCRIPTION_SERVICE_NAME) = IPP_EXTRACT.IPP_NAME
        LEFT JOIN -- cette table donne tous les ipp/forfaits auxquels un msisdn peut souscrire ainsi que les prix de ces forfaits
        (
            select 
                msisdn
                , bdle_name
                , bdle_cost
            from cdr.dt_Services_dynamique
            
        ) Services_dynamique on Services_dynamique.bdle_name = NVL(PRICE_PLAN.PRICE_PLAN_NAME,CAST(subs_of_today.PRICE_PLAN_CODE AS STRING)) and subs_of_today.msisdn = Services_dynamique.MSISDN and NVL(CHANSUBSC.CHANNEL_NAME, CAST(subs_of_today.CHANNEL_ID AS STRING)) IN ('32', '111')
        LEFT JOIN -- obtenir les informations sur les services par defaut (bdle_name, bdle_cost)
        (
            select *
            from cdr.dt_services_default
        ) services_default on services_default.bdle_name = NVL(PRICE_PLAN.PRICE_PLAN_NAME,CAST(subs_of_today.PRICE_PLAN_CODE AS STRING)) and  NVL(CHANSUBSC.CHANNEL_NAME, CAST(subs_of_today.CHANNEL_ID AS STRING)) IN ('32', '111')
        LEFT JOIN -- obtenir toutes les informations sur les offres (les ipp)
        (
            SELECT
                OFFER_NAME
                , MAX(OFFER_ID) OFFER_ID
                , MAX(OFFER_CODE) OFFER_CODE
            FROM DIM.DT_IPP_HYBRID GROUP BY OFFER_NAME 
        ) hybipp on nvl(price_plan.price_plan_name, servsubsc.subscription_service_name) = hybipp.offer_name
        group by subs_of_today.msisdn
            , nvl(price_plan.price_plan_name,cast(subs_of_today.price_plan_code as string))
            , subs_of_today.bal_id_col
            , transaction_time
            , subs_of_today.msisdn 
            -- , table_des_valorisations.msisdn
            , price_plan.price_plan_name
            , subs_of_today.price_plan_code
            , subs_of_today.bal_id_col
            -- , table_des_usages.bal_id
            -- , table_des_usages.tricky_bal_id
            , transaction_time
            , ben_acct_id
            -- , initial_date
            -- , expiration_date
            , acct_res_rating_unit
            , volume_total
            -- , usage_a_j
            -- , valorisation_a_j
    ) a
    left join dim.dt_balance_usage dt_balance_usage on a.acct_res_id = cast(dt_balance_usage.acct_res_id as bigint)
    left join dim.dt_cbm_ref_souscription_price ref_souscription on trim(upper(a.bdle_name)) = trim(upper(ref_souscription.bdle_name))
    left join (select acct_res_id, max(politic) politic from dim.dt_politique_forfaits group by acct_res_id) dim_politique_forfaits on a.acct_res_id=dim_politique_forfaits.acct_res_id
    group by a.msisdn
        , a.bal_id
    -- where (ref_souscription.validite is not null and volume_total > 0.0) or tricky_bal_id is not null -- tricky_bal_id permet de se rassurer que dans cette condition, on prend en compte les gars qui ont trafiqué aujourd'hui mais dont on a aucune information sur la validité du forfait ou sur le volume (et c'est normal parce que la table des usages ne donne pas ces infos s'il avait souscrit hier et a trafiqué aujourd'hui)
    
    -- where ref_souscription.validite is not null or tricky_bal_id is not null 
    
    -- tricky_bal_id permet de se rassurer que dans cette condition, on prend en compte les gars qui ont trafiqué aujourd'hui mais dont on a aucune information sur la validité du forfait ou sur le volume (et c'est normal parce que la table des usages ne donne pas ces infos s'il avait souscrit hier et a trafiqué aujourd'hui)
) photo_a_j
full join
(
    select msisdn
        , bal_id
        , transaction_time
        , acct_res_id
        , acct_res_rating_unit
        , politic
        , bdle_name
        , prix_total
        , volume_total
        , duree_bundle
        , initial_date
        , expiration_date
        , cumul_usage
        , volume_restant
        , cumul_valorisation
        , valorisation_restante
        , nb_jours_ecoules
    from mon.spark_ft_photo_journaliere_des_sous_comptes
    where event_date=date_sub('###SLICE_VALUE###', 1)
) photo_a_j_moins_un
on photo_a_j.bal_id=photo_a_j_moins_un.bal_id and photo_a_j.msisdn=photo_a_j_moins_un.msisdn
left join -- recuperer initial_date et expiration_date dans bal_extract
(
    select bal_id
        , eff_date initial_date
        , exp_date expiration_date
    from 
    (
        select 
            bal_id
            , eff_date
            , exp_date
            , row_number() over(partition by bal_id order by update_date desc) rang
        from cdr.spark_it_zte_bal_extract
        where original_file_date=date_add('###SLICE_VALUE###', 1)
    ) xxx where rang = 1
) bal_extract on nvl(photo_a_j.bal_id, photo_a_j_moins_un.bal_id)=bal_extract.bal_id
left join -- recuperer le cumul des usages dans la table des usages pour se rassurer qu'on prenne les gars qui n'ont pas fait de souscriptions aujourd'hui mais qui avaient déja des balances actives et qui ont trafiqué
(
    select max(msisdn) msisdn
        , bal_id
        , bal_id tricky_bal_id
        , sum(usage_horaire) usage_a_j
    from mon.spark_ft_usage_horaire_localise
    where event_date='###SLICE_VALUE###'
    group by msisdn
        , bal_id
) table_des_usages on nvl(photo_a_j.bal_id, photo_a_j_moins_un.bal_id)=table_des_usages.bal_id and nvl(photo_a_j.msisdn, photo_a_j_moins_un.msisdn)=table_des_usages.msisdn
left join -- pour avoir le cumul des valorisations jusqu'a j-1
(
    select msisdn msisdn
        , bal_id
        , nvl(sum(valorisation_conso_horaire), 0) valorisation_a_j
    from mon.spark_ft_valorisation_horaire_dans_sous_compte
    where event_date=date_sub('###SLICE_VALUE###', 1)
    group by msisdn, bal_id
) table_des_valorisations on table_des_valorisations.bal_id=nvl(photo_a_j.bal_id, photo_a_j_moins_un.bal_id) 
    and table_des_valorisations.msisdn=nvl(photo_a_j.msisdn, photo_a_j_moins_un.msisdn)
where --to_date(nvl(bal_extract.expiration_date, photo_a_j_moins_un.expiration_date)) >= '###SLICE_VALUE###' and 
    nvl(photo_a_j.prix_total, photo_a_j_moins_un.prix_total) is not null and (nvl(photo_a_j.prix_total, 0) +  nvl(photo_a_j_moins_un.prix_total, 0)) <> 0
group by nvl(photo_a_j.msisdn, photo_a_j_moins_un.msisdn)
    , nvl( photo_a_j.bal_id, photo_a_j_moins_un.bal_id)
having nvl(nvl(max(bal_extract.expiration_date), max(photo_a_j_moins_un.expiration_date)), '###SLICE_VALUE###') >= '###SLICE_VALUE###'