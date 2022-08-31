select 
    T.loan_date loan_date,
    T.loan_amount loan_amount,
    fn_format_msisdn_to_9digits(T.MSISDN) msisdn,
    T.activation_date activation_date,
    T.profil profil,
    T.main_credit main_credit,
    B.monthly_avg_recharge monthly_avg_recharge,
    C.last_loan_date_sos_voix last_loan_date_sos_voix,
	C.last_payback_list_sos_voix last_payback_list_sos_voix,
    C.last_loan_amount_sos_voix last_loan_amount_sos_voix,
    C.avg_last_payback_amount_sos_voix avg_last_payback_amount_sos_voix,
    C.second_last_loan_date_sos_voix second_last_loan_date_sos_voix,
    C.second_last_loan_amount_sos_voix second_last_loan_amount_sos_voix,
    C.avg_second_last_payback_amount_sos_voix avg_second_last_payback_amount_sos_voix,
	C.second_last_payback_list_sos_voix second_last_payback_list_sos_voix,
    D.last_loan_date_sos_data last_loan_date_sos_data,
    D.last_payback_list_sos_data last_payback_list_sos_data,
    D.last_loan_amount_sos_data last_loan_amount_sos_data,
    D.avg_last_payback_amount_sos_data avg_last_payback_amount_sos_data,
    F.last_loan_date_sos_credit last_loan_date_sos_credit,
    F.last_payback_list_sos_credit last_payback_list_sos_credit,
    F.last_loan_amount_sos_credit last_loan_amount_sos_credit,
    F.avg_last_payback_amount_sos_credit avg_last_payback_amount_sos_credit,
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
        cast(transaction_date as timestamp) loan_date,  
        t2.ACTIVATION_DATE activation_date, 
        t2.MAIN_CREDIT main_credit, 
        t2.OSP_CONTRACT_TYPE profil
    FROM 
    (
        select *
        from CDR.spark_it_zte_loan_cdr
        where original_file_date  = '###SLICE_VALUE###' AND trim(LOWER(transaction_type))='loan'
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
    -- CONFORMITE DES SOUSCRIPTIONS AU SOS VOIX : extraction de la moyenne des recharges des trois dernier mois et calcul du plafond         
    SELECT 
        ACCT_ID_MSISDN msisdn, 
        sum(main_credit)/3 as monthly_avg_recharge
    FROM MON.spark_FT_EDR_PRPD_EQT
    WHERE EVENT_DATE BETWEEN add_months('###SLICE_VALUE###', -3) AND '###SLICE_VALUE###' AND
        (UPPER(TYPE) LIKE 'RECURRING%' OR UPPER(TYPE) in ('ADJUSTMENT CHARGE POINT OF SAL', 'TRANSFER P2P RECV CREDIT', 'SUBSCRIPTION DEPOT FLEX', 'RECHARGE VC RECHARGE 4', 'SUBSCRIPTION DEPOT HYBRID', 'RECHARGE E-TOPUP 11', 'SUBSCRIPTION DEPOT BENEFIT'))         
    GROUP BY ACCT_ID_MSISDN

) B
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(B.msisdn)
left join
(
    -- Pas d emprunt en cours sur SOS Data ou SOS Voix ou SOS crédit
    -- ICI nous aimerons avoir le dernier SOS DATA, le dernier SOS Voix et le dernier SOS crédit avant la souscription SOS CREDIT ainsi que le montant remboursé pour chacun d’entre eux
    -- CONFORMITE DES SOUSCRIPTIONS AU SOS Voix : extraction d du dernier emprunt SOS CREDIT sur les 4 derniers mois
    select
        A3.msisdn,
        last_loan_date_sos_voix,
        last_loan_amount_sos_voix,
        second_last_loan_date_sos_voix,
        second_last_loan_amount_sos_voix,
        sum(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_voix then nvl(A4.loan_amount, 0) else null end) avg_last_payback_amount_sos_voix,
        sum(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= second_last_loan_date_sos_voix and nvl(A4.transaction_time, '0000-00-00 00:00:00') < last_loan_date_sos_voix then nvl(A4.loan_amount, 0) else null end) avg_second_last_payback_amount_sos_voix,
        CONCAT_WS('#',COLLECT_LIST(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_voix then transaction_id_time_amount end)) last_payback_list_sos_voix,
        CONCAT_WS('#', COLLECT_LIST(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= second_last_loan_date_sos_voix and nvl(A4.transaction_time, '0000-00-00 00:00:00') < last_loan_date_sos_voix then transaction_id_time_amount end)) second_last_payback_list_sos_voix
    from
    (
        select
            msisdn,
            transaction_time last_loan_date_sos_voix,
            loan_amount last_loan_amount_sos_voix,
            case when transaction_time_prev = transaction_time then null else transaction_time_prev end second_last_loan_date_sos_voix,
            case when transaction_time_prev = transaction_time then null else loan_amount_prev end second_last_loan_amount_sos_voix
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
                        cast(transaction_date as timestamp) transaction_time,
                        amount loan_amount,
                        row_number() over(partition by A.msisdn order by cast(transaction_date as timestamp) desc) rang
					from
					(
						select *
						from cdr.spark_it_zte_loan_cdr
						where original_file_date >= add_months('###SLICE_VALUE###', -3) AND original_file_date < '###SLICE_VALUE###' AND 
						trim(LOWER(transaction_type))='loan'
					) A
					left join
					(
						select 
							msisdn,
							max(cast(transaction_date as timestamp)) last_loan_datetime
						from CDR.spark_it_zte_loan_cdr
						where original_file_date  = '###SLICE_VALUE###' AND trim(LOWER(transaction_type))='loan'
						group by msisdn
					) B 
					on A.msisdn = B.msisdn and cast(transaction_date as timestamp) < last_loan_datetime
                    
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
			cast(transaction_date as timestamp) transaction_time,
			amount loan_amount,
			concat(cast(transaction_date as timestamp), '@', amount) transaction_id_time_amount
		from
		(
			select *
			from cdr.spark_it_zte_loan_cdr
			where original_file_date >= add_months('###SLICE_VALUE###', -3) AND original_file_date < '###SLICE_VALUE###' AND 
			trim(LOWER(transaction_type))='payback'
		) A
		left join
		(
			select 
				msisdn,
				max(cast(transaction_date as timestamp)) last_loan_datetime
			from CDR.spark_it_zte_loan_cdr
			where original_file_date  = '###SLICE_VALUE###' AND trim(LOWER(transaction_type))='loan'
			group by msisdn
		) B 
		on A.msisdn = B.msisdn and cast(transaction_date as timestamp) < last_loan_datetime
		
    ) A4
    on fn_format_msisdn_to_9digits(A3.msisdn) = fn_format_msisdn_to_9digits(A4.msisdn) 
    group by A3.msisdn,
        last_loan_date_sos_voix,
        last_loan_amount_sos_voix,
        second_last_loan_date_sos_voix,
        second_last_loan_amount_sos_voix

) C
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(C.msisdn)
left join
(
    select
        A.msisdn,
        last_loan_date_sos_data,
        last_loan_amount_sos_data,
		sum(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_data then nvl(B.loan_amount, 0) else null end) avg_last_payback_amount_sos_data,
		CONCAT_WS('#',COLLECT_LIST(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_data then transaction_id_time_amount end)) last_payback_list_sos_data
    from
    (
        select
            msisdn,
            transaction_time last_loan_date_sos_data,
            amount last_loan_amount_sos_data
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
						from cdr.spark_it_zte_emergency_data
						where TRANSACTION_DATE >= add_months('###SLICE_VALUE###', -3) AND TRANSACTION_DATE <= '###SLICE_VALUE###' AND 
						trim(LOWER(transaction_type))='loan'
					) A
					left join
					(
						select 
							msisdn,
							max(cast(transaction_date as timestamp)) last_loan_datetime
						from CDR.spark_it_zte_loan_cdr
						where original_file_date  = '###SLICE_VALUE###' AND trim(LOWER(transaction_type))='loan'
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
			cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) transaction_time,
			concat(cast(transaction_date as timestamp), '@', amount) transaction_id_time_amount
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
				max(cast(transaction_date as timestamp)) last_loan_datetime
			from CDR.spark_it_zte_loan_cdr
			where original_file_date  = '###SLICE_VALUE###' AND trim(LOWER(transaction_type))='loan'
			group by msisdn
		) B 
		on A.msisdn = B.msisdn and cast(transaction_date as timestamp) < last_loan_datetime
		
    ) B
    on fn_format_msisdn_to_9digits(A.msisdn) = fn_format_msisdn_to_9digits(B.msisdn)
    group by A.msisdn,
        last_loan_date_sos_data,
        last_loan_amount_sos_data
) D
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(D.msisdn)
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
						cast(concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2)) as timestamp) transaction_time,
						row_number() over(partition by A.msisdn order by cast(concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2)) as timestamp) desc) rang
					from
					(
						select *
						from cdr.spark_it_zte_emergency_credit
						where transaction_date >= add_months('###SLICE_VALUE###', -3) AND TRANSACTION_DATE <= '###SLICE_VALUE###' AND
						lower(transaction_type) = 'loan'
					) A
					left join
					(
						select 
							msisdn,
							max(cast(transaction_date as timestamp)) last_loan_datetime
						from CDR.spark_it_zte_loan_cdr
						where original_file_date  = '###SLICE_VALUE###' AND trim(LOWER(transaction_type))='loan'
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
			cast(concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2)) as timestamp) transaction_time,
            concat(cast(concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2)) as timestamp), '@', amount) transaction_id_time_amount
		from
		(
			select *
			from cdr.spark_it_zte_emergency_credit
			where transaction_date >= add_months('###SLICE_VALUE###', -3) AND TRANSACTION_DATE <= '###SLICE_VALUE###' AND
			lower(transaction_type) = 'payback'
		) A
		left join
		(
			select 
				msisdn,
				max(cast(transaction_date as timestamp)) last_loan_datetime
			from CDR.spark_it_zte_loan_cdr
			where original_file_date  = '###SLICE_VALUE###' AND trim(LOWER(transaction_type))='loan'
			group by msisdn
		) B 
		on A.msisdn = B.msisdn and cast(transaction_date as timestamp) < last_loan_datetime
		
    ) B
    on fn_format_msisdn_to_9digits(A.msisdn) = fn_format_msisdn_to_9digits(B.msisdn) 
    group by A.msisdn,
        last_loan_date_sos_credit,
        last_loan_amount_sos_credit
) F 
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(F.msisdn)

