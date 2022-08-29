select 
    T.loan_date loan_date,
    T.loan_amount loan_amount,
    fn_format_msisdn_to_9digits(T.MSISDN) msisdn,
    T.activation_date activation_date,
    T.profil profil,
    T.main_credit main_credit,
    B.avg_recharge_last_three_month avg_recharge_last_three_month,
    B.monthly_avg_recharge monthly_avg_recharge,
    B.plafond plafond,
	U1.max_bundle_data_day last_date_bundle_data,
	F.session_datetime session_datetime_data,
	mo_remaining_data,
    C.last_loan_date_sos_data last_loan_date_sos_data,
	C.last_payback_list_sos_data last_payback_list_sos_data,
    C.last_loan_amount_sos_data last_loan_amount_sos_data,
    C.avg_last_payback_amount_sos_data avg_last_payback_amount_sos_data,
    C.second_last_loan_date_sos_data second_last_loan_date_sos_data,
    C.second_last_loan_amount_sos_data second_last_loan_amount_sos_credit,
    C.avg_second_last_payback_amount_sos_data avg_second_last_payback_amount_sos_data,
	C.second_last_payback_list_sos_data second_last_payback_list_sos_data,
    D.last_loan_date_sos_credit last_loan_date_sos_credit,
    D.last_payback_list_sos_credit last_payback_list_sos_credit,
    D.last_loan_amount_sos_credit last_loan_amount_sos_credit,
    D.avg_last_payback_amount_sos_credit avg_last_payback_amount_sos_credit,
    E.last_loan_date_sos_voix last_loan_date_sos_voix,
    E.last_payback_list_sos_voix last_payback_list_sos_voix,
    E.last_loan_amount_sos_voix last_loan_amount_sos_voix,
    E.avg_last_payback_amount_sos_voix avg_last_payback_amount_sos_voix,
    current_timestamp() insert_date,
    '###SLICE_VALUE###' event_date
from
(
    -- Client prépayé ou hybride
    -- Ancienneté > 6 mois
    -- Main balance < 100F
    -- CONFORMITE DES SOUSCRIPTIONS AU SOS CREDIT : extraction des souscriptions et des infromations sur les souscripteurs
    SELECT 
        nvl(t1.msisdn, t2.ACCESS_KEY) MSISDN, 
        t1.amount loan_amount, 
        cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) loan_date,  
        t2.ACTIVATION_DATE activation_date, 
        t2.MAIN_CREDIT main_credit, 
        t2.OSP_CONTRACT_TYPE profil
        FROM
    (
		select *
		from cdr.spark_it_zte_emergency_data
		where TRANSACTION_DATE = '###SLICE_VALUE###' AND trim(LOWER(transaction_type))='loan'
    ) t1
    left JOIN 
    (
        select *
        from MON.spark_FT_CONTRACT_SNAPSHOT
        where EVENT_DATE = '###SLICE_VALUE###'
    ) t2 
    ON fn_format_msisdn_to_9digits(t1.msisdn) = fn_format_msisdn_to_9digits(t2.ACCESS_KEY)
) T
left join
(
    -- Recharge moyenne mensuelle des 3 derniers mois >= 250
    -- Plafond d emprunt non atteint sur SOS Credit : 40% de la recharge moyenne des 3 derniers mois.
    -- CONFORMITE DES SOUSCRIPTIONS AU SOS CREDIT : extraction de la moyenne des recharges des trois dernier mois et calcul du plafond         
    SELECT 
        ACCT_ID_MSISDN msisdn, 
        sum(main_credit)/count(event_time) avg_recharge_last_three_month, 
        sum(main_credit)/3 as monthly_avg_recharge,
        (0.4*avg(main_credit)) plafond
    FROM MON.spark_FT_EDR_PRPD_EQT
    WHERE EVENT_DATE BETWEEN add_months('###SLICE_VALUE###', -3) AND '###SLICE_VALUE###' AND
        (UPPER(TYPE) LIKE 'RECURRING%' OR UPPER(TYPE) in ('ADJUSTMENT CHARGE POINT OF SAL', 'TRANSFER P2P RECV CREDIT', 'SUBSCRIPTION DEPOT FLEX', 'RECHARGE VC RECHARGE 4', 'SUBSCRIPTION DEPOT HYBRID', 'RECHARGE E-TOPUP 11', 'SUBSCRIPTION DEPOT BENEFIT'))         
    GROUP BY ACCT_ID_MSISDN
    
) B
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(B.msisdn)
left join
(
	-- Avoir souscrit au moins une fois à un forfait data 
	select 
		served_party_msisdn as msisdn, 
		max(transaction_date) as max_bundle_data_day
	from mon.spark_ft_subscription 
	where transaction_date BETWEEN add_months('###SLICE_VALUE###', -3) AND '###SLICE_VALUE###' 
	and lower(service_list) like '%data%' 
	group by served_party_msisdn

) U1
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(U1.msisdn)
left join
(
    -- Pas d emprunt en cours sur SOS Data ou SOS Voix
    -- ICI nous aimerons avoir le dernier SOS DATA et le dernier SOS DATA avant la souscription SOS CREDIT ainsi que le montant remboursé pour chacun d’entre eux
    -- Maximum 1 emprunt en cours sur SOS Credit (ce que nous essayons de faire avec ses deux requêtes) 
    -- CONFORMITE DES SOUSCRIPTIONS AU SOS CREDIT : extraction d du dernier emprunt SOS CREDIT sur les 4 derniers mois
    select
        A3.msisdn,
        last_loan_date_sos_data,
        last_loan_amount_sos_data,
        second_last_loan_date_sos_data,
        second_last_loan_amount_sos_data,
		sum(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_data then nvl(A4.loan_amount, 0) else null end) avg_last_payback_amount_sos_data,
		sum(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= second_last_loan_date_sos_data and nvl(A4.transaction_time, '0000-00-00 00:00:00') < last_loan_date_sos_data then nvl(A4.loan_amount, 0) else null end) avg_second_last_payback_amount_sos_data,
		CONCAT_WS('#',COLLECT_LIST(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_data then transaction_id_time_amount end)) last_payback_list_sos_data,
		CONCAT_WS('#', COLLECT_LIST(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= second_last_loan_date_sos_data and nvl(A4.transaction_time, '0000-00-00 00:00:00') < last_loan_date_sos_data then transaction_id_time_amount end)) second_last_payback_list_sos_data
    from
    (
        select
            msisdn,
            transaction_time last_loan_date_sos_data,
            loan_amount last_loan_amount_sos_data,
            case when transaction_time_prev = transaction_time then null else transaction_time_prev end second_last_loan_date_sos_data,
            case when transaction_time_prev = transaction_time then null else loan_amount_prev end second_last_loan_amount_sos_data
        from 
        (
            select
                msisdn,
                max(transaction_time) over(partition by msisdn order by transaction_time desc) transaction_time,
                first_value(loan_amount) over(partition by msisdn order by transaction_time desc) loan_amount,
                min(transaction_time) over(partition by msisdn order by transaction_time asc) transaction_time_prev,
                first_value(loan_amount) over(partition by msisdn order by transaction_time asc) loan_amount_prev,
                row_number() over(partition by msisdn order by transaction_time desc) rang
            from
            (
                SELECT
                    msisdn,
                    transaction_time,
                    loan_amount
                from
                (

					select 
						A.msisdn,  
                        cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) transaction_time,
                        amount loan_amount,
                        row_number() over(partition by A.msisdn order by cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) desc) rang
					from
					(
						select *
						from cdr.spark_it_zte_emergency_data 
						where TRANSACTION_DATE >= add_months('###SLICE_VALUE###', -3) AND TRANSACTION_DATE <= '###SLICE_VALUE###' AND 
						trim(LOWER(transaction_type))='loan'
					) A
					left join
					(
						select 
							msisdn, 
							max(cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP)) last_loan_datetime
						from cdr.spark_it_zte_emergency_data 
						where transaction_date = '###SLICE_VALUE###' AND trim(LOWER(transaction_type))='loan'
						group by msisdn
					) B 
					on A.msisdn = B.msisdn and cast(concat(A.transaction_date, ' ', concat(substr(A.transaction_time, 1, 2), ':', substr(A.transaction_time, 3, 2), ':', substr(A.transaction_time, 5, 2))) as TIMESTAMP) < last_loan_datetime
					
                    
                ) T
                where rang <= 2
            ) Y
        ) U
        where rang <= 1
    ) A3
    left join
    (
	
		select 
			A.msisdn,  
			cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) transaction_time,
			amount loan_amount,
			concat(concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2)), '@', amount) transaction_id_time_amount
		from
		(
			select *
			from cdr.spark_it_zte_emergency_data 
			where TRANSACTION_DATE >= add_months('###SLICE_VALUE###', -3) AND TRANSACTION_DATE <= '###SLICE_VALUE###' AND 
			trim(LOWER(transaction_type))='payback'
		) A
		left join
		(
			select 
				msisdn, 
				max(cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP)) last_loan_datetime
			from cdr.spark_it_zte_emergency_data 
			where transaction_date = '###SLICE_VALUE###' AND trim(LOWER(transaction_type))='loan'
			group by msisdn
		) B 
		on A.msisdn = B.msisdn and cast(concat(A.transaction_date, ' ', concat(substr(A.transaction_time, 1, 2), ':', substr(A.transaction_time, 3, 2), ':', substr(A.transaction_time, 5, 2))) as TIMESTAMP) < last_loan_datetime
		
    ) A4
    on fn_format_msisdn_to_9digits(A3.msisdn) = fn_format_msisdn_to_9digits(A4.msisdn) 
	group by A3.msisdn,
        last_loan_date_sos_data,
        last_loan_amount_sos_data,
        second_last_loan_date_sos_data,
        second_last_loan_amount_sos_data
) C
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(C.msisdn)
left join
(

    select
        A.msisdn,
        last_loan_date_sos_credit,
        last_loan_amount_sos_credit,
		sum(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_credit then nvl(B.loan_amount, 0) else null end) avg_last_payback_amount_sos_credit,
		CONCAT_WS('#',COLLECT_LIST(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_credit then transaction_id_time_amount end)) last_payback_list_sos_credit
    from
    (
        select
            msisdn,
            transaction_time last_loan_date_sos_credit,
            amount last_loan_amount_sos_credit
        from
        (
            select
                msisdn,
                max(transaction_time) over(partition by msisdn order by transaction_time desc) transaction_time,
                first_value(amount) over(partition by msisdn order by transaction_time desc) amount,
                min(transaction_time) over(partition by msisdn order by transaction_time asc) transaction_time_prev,
                first_value(amount) over(partition by msisdn order by transaction_time asc) amount_prev,
                row_number() over(partition by msisdn order by transaction_time desc) rang
            from
            (
                select
                    msisdn,
                    transaction_time,
                    amount
                from
                (
					
					select 
						A.msisdn msisdn,
						amount,
						cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) transaction_time,
						row_number() over(partition by A.msisdn order by cast(concat(transaction_date, ' ', substr(transaction_time,1,2),':',substr(transaction_time,3,2),':',substr(transaction_time,5,2)) as timestamp) desc) rang
					from
					(
						select *
						from cdr.spark_it_zte_emergency_credit
						where TRANSACTION_DATE >= add_months('###SLICE_VALUE###', -3) AND TRANSACTION_DATE <= '###SLICE_VALUE###' AND 
						trim(LOWER(transaction_type))='loan'
					) A
					left join
					(
						select 
							msisdn, 
							max(cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP)) last_loan_datetime
						from cdr.spark_it_zte_emergency_data
						where transaction_date = '###SLICE_VALUE###' AND trim(LOWER(transaction_type))='loan'
						group by msisdn
					) B 
					on A.msisdn = B.msisdn and cast(concat(A.transaction_date, ' ', concat(substr(A.transaction_time, 1, 2), ':', substr(A.transaction_time, 3, 2), ':', substr(A.transaction_time, 5, 2))) as TIMESTAMP) < last_loan_datetime					
					
                ) T
                where rang <= 2
            ) Y
        ) U
        where rang <= 1
    ) A
    left join
    (
		
		select 
			A.msisdn msisdn,
			amount loan_amount,
			cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) transaction_time,
			concat(cast(transaction_date as timestamp), '@', amount) transaction_id_time_amount
		from
		(
			select *
			from cdr.spark_it_zte_emergency_credit
			where TRANSACTION_DATE >= add_months('###SLICE_VALUE###', -3) AND TRANSACTION_DATE <= '###SLICE_VALUE###' AND 
			trim(LOWER(transaction_type))='payback'
		) A
		left join
		(
			select 
				msisdn, 
				max(cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP)) last_loan_datetime
			from cdr.spark_it_zte_emergency_data
			where transaction_date = '###SLICE_VALUE###' AND trim(LOWER(transaction_type))='loan'
			group by msisdn
		) B 
		on A.msisdn = B.msisdn and cast(concat(A.transaction_date, ' ', concat(substr(A.transaction_time, 1, 2), ':', substr(A.transaction_time, 3, 2), ':', substr(A.transaction_time, 5, 2))) as TIMESTAMP) < last_loan_datetime	
		
    ) B
    on fn_format_msisdn_to_9digits(A.msisdn) = fn_format_msisdn_to_9digits(B.msisdn)
    group by A.msisdn,
        last_loan_date_sos_credit,
        last_loan_amount_sos_credit
) D
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(D.msisdn)
left join
(
    select 
        A.msisdn,
        last_loan_date_sos_voix,
        last_loan_amount_sos_voix,
		sum(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_voix then nvl(B.loan_amount, 0) else null end) avg_last_payback_amount_sos_voix,
		CONCAT_WS('#',COLLECT_LIST(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_voix then transaction_id_time_amount end)) last_payback_list_sos_voix
    from
    (
        select
            msisdn,
            transaction_time last_loan_date_sos_voix,
            amount last_loan_amount_sos_voix
        from
        (
            select
                msisdn,
                max(transaction_time) over(partition by msisdn order by transaction_time desc) transaction_time,
                first_value(amount) over(partition by msisdn order by transaction_time desc) amount,
                min(transaction_time) over(partition by msisdn order by transaction_time asc) transaction_time_prev,
                first_value(amount) over(partition by msisdn order by transaction_time asc) amount_prev,
                row_number() over(partition by msisdn order by transaction_time desc) rang
            from
            (
                select
                    msisdn,
                    transaction_time,
                    amount
                from
                (
					
					select 
						A.msisdn msisdn,
						amount,
						cast(transaction_date as timestamp) transaction_time,
						row_number() over(partition by A.msisdn order by cast(transaction_date as timestamp) desc) rang
					from
					(
						select *
						from cdr.spark_it_zte_loan_cdr
						where original_file_date >= add_months('###SLICE_VALUE###', -3) AND TRANSACTION_DATE <= '###SLICE_VALUE###' AND
						lower(transaction_type) = 'loan'
					) A
					left join
					(
						select 
							msisdn, 
							max(cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP)) last_loan_datetime
						from cdr.spark_it_zte_emergency_data
						where transaction_date = '###SLICE_VALUE###' AND trim(LOWER(transaction_type))='loan'
						group by msisdn
					) B 
					on A.msisdn = B.msisdn and cast(transaction_date as timestamp) < last_loan_datetime	
					
                ) T
                where rang <= 2
            ) Y
        ) U 
        where rang <= 1
    ) A
    left join
    (

		select 
			A.msisdn msisdn,
			amount loan_amount,
			cast(transaction_date as timestamp) transaction_time,
			concat(cast(transaction_date as timestamp), '@', amount) transaction_id_time_amount
		from
		(
			select *
			from cdr.spark_it_zte_loan_cdr
			where original_file_date >= add_months('###SLICE_VALUE###', -3) AND TRANSACTION_DATE <= '###SLICE_VALUE###' AND
			lower(transaction_type) = 'payback'
		) A
		left join
		(
			select 
				msisdn, 
				max(cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP)) last_loan_datetime
			from cdr.spark_it_zte_emergency_data
			where transaction_date = '###SLICE_VALUE###' AND trim(LOWER(transaction_type))='loan'
			group by msisdn
		) B 
		on A.msisdn = B.msisdn and cast(transaction_date as timestamp) < last_loan_datetime	
		
    ) B
    on fn_format_msisdn_to_9digits(A.msisdn) = fn_format_msisdn_to_9digits(B.msisdn) 
    group by A.msisdn,
        last_loan_date_sos_voix,
        last_loan_amount_sos_voix
) E
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(E.msisdn)
left join
(
	-- ICI nous aimerons avoir le volume data restant (en Mo) après la dernière session data avant l’emprunt

	select 
		msisdn,
		session_datetime,
		nb_Mo_data mo_remaining_data
	from
	(
		select 
			A.msisdn msisdn,  
			session_datetime,
			nb_Mo_data,
			row_number() over(partition by A.msisdn order by session_datetime desc) rang
		from
		(
			select 
				served_party_msisdn msisdn,
				cast(concat(session_date, ' ', concat(substr(session_time, 1, 2), ':', substr(session_time, 3, 2), ':', substr(session_time, 5, 2))) as TIMESTAMP) session_datetime,
				nvl(bundle_bytes_remaining_volume, 0)/1024/1024 as nb_Mo_data
			from mon.spark_ft_cra_gprs
			where session_date between add_months('###SLICE_VALUE###', -3) and '###SLICE_VALUE###'
		) A
		left join
		(
			select 
				msisdn, 
				max(cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP)) last_loan_datetime
			from cdr.spark_it_zte_emergency_data 
			where transaction_date = '###SLICE_VALUE###' AND trim(LOWER(transaction_type))='loan'
			group by msisdn
		) B 
		on fn_format_msisdn_to_9digits(A.msisdn) = fn_format_msisdn_to_9digits(B.msisdn) and session_datetime < last_loan_datetime
	) A 
	where rang <= 1
) F
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(F.msisdn)
