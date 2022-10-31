insert into mon.spark_ft_sos_credit_non_conformes
select 
    T.loan_id,
    T.loan_date loan_date,
    T.loan_amount loan_amount,
    fn_format_msisdn_to_9digits(T.MSISDN) msisdn,
    T.activation_date activation_date,
    T.profil profil,
    T.main_credit main_credit,
    B.avg_recharge_last_three_month avg_recharge_last_three_month,
    B.monthly_avg_recharge monthly_avg_recharge,
    B.plafond plafond,
    C.last_loan_date_sos_credit last_loan_date_sos_credit,
	C.last_payback_list_sos_credit last_payback_list_sos_credit,
    C.last_loan_amount_sos_credit last_loan_amount_sos_credit,
    C.avg_last_payback_amount_sos_credit avg_last_payback_amount_sos_credit,
    C.last_loan_id_sos_credit last_loan_id_sos_credit,
    C.second_last_loan_date_sos_credit second_last_loan_date_sos_credit,
    C.second_last_loan_amount_sos_credit second_last_loan_amount_sos_credit,
    C.avg_second_last_payback_amount_sos_credit avg_second_last_payback_amount_sos_credit,
    C.second_last_loan_id_sos_credit second_last_loan_id_sos_credit,
	C.second_last_payback_list_sos_credit second_last_payback_list_sos_credit,
    D.last_loan_date_sos_data last_loan_date_sos_data,
    D.last_payback_list_sos_data last_payback_list_sos_data,
    D.last_loan_amount_sos_data last_loan_amount_sos_data,
    D.avg_last_payback_amount_sos_data avg_last_payback_amount_sos_data,
    null last_loan_id_sos_data,
    D.second_last_loan_date_sos_data second_last_loan_date_sos_data,
    D.second_last_loan_amount_sos_data second_last_loan_amount_sos_data,
    --D.avg_second_last_payback_amount_sos_data avg_second_last_payback_amount_sos_data,
    null second_last_loan_id_sos_data,
	D.second_last_payback_list_sos_data second_last_payback_list_sos_data,
    E.last_loan_date_sos_voix last_loan_date_sos_voix,
    E.last_payback_list_sos_voix last_payback_list_sos_voix,
    E.last_loan_amount_sos_voix last_loan_amount_sos_voix,
    E.avg_last_payback_amount_sos_voix avg_last_payback_amount_sos_voix,
    null last_loan_id_sos_voix,
    E.second_last_loan_date_sos_voix second_last_loan_date_sos_voix,
    E.second_last_loan_amount_sos_voix second_last_loan_amount_sos_voix,
    --E.avg_second_last_payback_amount_sos_voix avg_second_last_payback_amount_sos_voix,
    null second_last_loan_id_sos_voix,
	E.second_last_payback_list_sos_voix second_last_payback_list_sos_voix,
    current_timestamp() insert_date,
    '###SLICE_VALUE###' event_date
from
(
    -- Client prépayé ou hybride
    -- Ancienneté > 6 mois
    -- Main balance < 100F
    -- CONFORMITE DES SOUSCRIPTIONS AU SOS CREDIT : extraction des souscriptions et des infromations sur les souscripteurs
    SELECT 
        t1.TRANSACTION_ID loan_id, 
        nvl(t1.SERVED_PARTY_MSISDN, t2.ACCESS_KEY) MSISDN, 
        t1.LOAN_AMOUNT loan_amount, 
        cast(t1.transaction_time as timestamp) loan_date,  
        t2.ACTIVATION_DATE activation_date, 
        t2.MAIN_CREDIT main_credit, 
        t2.OSP_CONTRACT_TYPE profil
    FROM 
    (
        select *
        from MON.SPARK_FT_OVERDRAFT 
        where TRANSACTION_DATE = '###SLICE_VALUE###' AND LOWER(OPERATION_TYPE)='loan' 
    ) t1
    left JOIN 
    (
        select *
        from MON.spark_FT_CONTRACT_SNAPSHOT
        where EVENT_DATE = '###SLICE_VALUE###'
    ) t2 
    ON fn_format_msisdn_to_9digits(t1.SERVED_PARTY_MSISDN) = fn_format_msisdn_to_9digits(t2.ACCESS_KEY)
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
    -- Pas d emprunt en cours sur SOS Data ou SOS Voix
    -- ICI nous aimerons avoir le dernier SOS DATA et le dernier SOS DATA avant la souscription SOS CREDIT ainsi que le montant remboursé pour chacun d’entre eux
    -- Maximum 1 emprunt en cours sur SOS Credit (ce que nous essayons de faire avec ses deux requêtes) 
    -- CONFORMITE DES SOUSCRIPTIONS AU SOS CREDIT : extraction d du dernier emprunt SOS CREDIT sur les 4 derniers mois
    select
        A3.msisdn,
        last_loan_date_sos_credit,
        last_loan_amount_sos_credit,
        last_loan_id_sos_credit,
        second_last_loan_date_sos_credit,
        second_last_loan_amount_sos_credit,
        second_last_loan_id_sos_credit,
		sum(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_credit then nvl(A4.loan_amount, 0) else null end) avg_last_payback_amount_sos_credit,
		sum(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= second_last_loan_date_sos_credit and nvl(A4.transaction_time, '0000-00-00 00:00:00') < last_loan_date_sos_credit then nvl(A4.loan_amount, 0) else null end) avg_second_last_payback_amount_sos_credit,
		CONCAT_WS('#',COLLECT_LIST(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_credit then transaction_id_time_amount end)) last_payback_list_sos_credit,
		CONCAT_WS('#', COLLECT_LIST(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= second_last_loan_date_sos_credit and nvl(A4.transaction_time, '0000-00-00 00:00:00') < last_loan_date_sos_credit then transaction_id_time_amount end)) second_last_payback_list_sos_credit
    from
    (
        select
            msisdn,
            transaction_time last_loan_date_sos_credit,
            loan_amount last_loan_amount_sos_credit,
            loan_id last_loan_id_sos_credit,
            case when transaction_time_prev = transaction_time then null else transaction_time_prev end second_last_loan_date_sos_credit,
            case when transaction_time_prev = transaction_time then null else loan_amount_prev end second_last_loan_amount_sos_credit,
            case when transaction_time_prev = transaction_time then null else loan_id_prev end second_last_loan_id_sos_credit
        from
        (
            select
                msisdn,
                max(transaction_time) over(partition by msisdn order by transaction_time desc) transaction_time,
                first_value(loan_amount) over(partition by msisdn order by transaction_time desc) loan_amount,
                first_value(loan_id) over(partition by msisdn order by transaction_time desc) loan_id,
                min(transaction_time) over(partition by msisdn order by transaction_time asc) transaction_time_prev,
                first_value(loan_amount) over(partition by msisdn order by transaction_time asc) loan_amount_prev,
                first_value(loan_id) over(partition by msisdn order by transaction_time asc) loan_id_prev,
                row_number() over(partition by msisdn order by transaction_time desc) rang
            from
            (
                SELECT
                    msisdn,
                    transaction_time,
                    loan_amount,
                    loan_id
                from
                (
                    SELECT  
                        SERVED_PARTY_MSISDN msisdn,  
                        cast(transaction_time as timestamp) transaction_time,
                        loan_amount loan_amount,
                        transaction_id loan_id,
                        row_number() over(partition by SERVED_PARTY_MSISDN order by cast(transaction_time as timestamp) desc) rang
                    FROM MON.SPARK_FT_OVERDRAFT
                    WHERE TRANSACTION_DATE >= add_months('###SLICE_VALUE###', -3) AND TRANSACTION_DATE < '###SLICE_VALUE###' AND 
                    LOWER(OPERATION_TYPE)='loan'
                ) T
                where rang <= 2
            ) Y
        ) U
        where rang <= 1
    ) A3
    left join
    (
		SELECT  
			SERVED_PARTY_MSISDN msisdn,  
			cast(transaction_time as timestamp) transaction_time,
			loan_amount loan_amount,
			transaction_id loan_id,
			concat(transaction_id, '@', cast(transaction_time as timestamp), '@', loan_amount) transaction_id_time_amount
		FROM MON.SPARK_FT_OVERDRAFT
		WHERE TRANSACTION_DATE >= add_months('###SLICE_VALUE###', -3) AND TRANSACTION_DATE < '###SLICE_VALUE###' AND LOWER(OPERATION_TYPE)='reimbursment'
    ) A4
    on fn_format_msisdn_to_9digits(A3.msisdn) = fn_format_msisdn_to_9digits(A4.msisdn) 
	group by A3.msisdn,
        last_loan_date_sos_credit,
        last_loan_amount_sos_credit,
        last_loan_id_sos_credit,
        second_last_loan_date_sos_credit,
        second_last_loan_amount_sos_credit,
        second_last_loan_id_sos_credit
) C
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(C.msisdn)
left join
(
    select
        A.msisdn,
        last_loan_date_sos_data,
        last_loan_amount_sos_data,
        second_last_loan_date_sos_data,
        second_last_loan_amount_sos_data,
		sum(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_data then nvl(B.loan_amount, 0) else null end) avg_last_payback_amount_sos_data,
		sum(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= second_last_loan_date_sos_data and nvl(B.transaction_time, '0000-00-00 00:00:00') < last_loan_date_sos_data then nvl(B.loan_amount, 0) else null end) avg_second_last_payback_amount_sos_data,
		CONCAT_WS('#',COLLECT_LIST(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_data then transaction_id_time_amount end)) last_payback_list_sos_data,
		CONCAT_WS('#', COLLECT_LIST(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= second_last_loan_date_sos_data and nvl(B.transaction_time, '0000-00-00 00:00:00') < last_loan_date_sos_data then transaction_id_time_amount end)) second_last_payback_list_sos_data
    from
    (
        select
            msisdn,
            transaction_time last_loan_date_sos_data,
            amount last_loan_amount_sos_data,
            case when transaction_time_prev = transaction_time then null else transaction_time_prev end second_last_loan_date_sos_data,
            case when transaction_time_prev = transaction_time then null else amount_prev end second_last_loan_amount_sos_data
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
                        msisdn,
                        amount,
                        cast(concat(transaction_date, ' ', substr(transaction_time,1,2),':',substr(transaction_time,3,2),':',substr(transaction_time,5,2)) as timestamp) transaction_time,
                        row_number() over(partition by msisdn order by cast(concat(transaction_date, ' ', substr(transaction_time,1,2),':',substr(transaction_time,3,2),':',substr(transaction_time,5,2)) as timestamp) desc) rang
                    from cdr.spark_it_zte_emergency_data 
                    where transaction_date >= add_months('###SLICE_VALUE###', -3) AND TRANSACTION_DATE <= '###SLICE_VALUE###' AND
                    lower(transaction_type) = 'loan'
                ) T
                where rang <= 2
            ) Y
        ) U
        where rang <= 1
    ) A
    left join
    (
        select
            msisdn,
            amount loan_amount,
            cast(concat(transaction_date, ' ', substr(transaction_time,1,2),':',substr(transaction_time,3,2),':',substr(transaction_time,5,2)) as timestamp) transaction_time,
            concat(cast(transaction_date as timestamp), '@', amount) transaction_id_time_amount
        from cdr.spark_it_zte_emergency_data 
        where transaction_date >= add_months('###SLICE_VALUE###', -3) AND TRANSACTION_DATE <= '###SLICE_VALUE###' AND
        lower(transaction_type) = 'payback'
    ) B
    on fn_format_msisdn_to_9digits(A.msisdn) = fn_format_msisdn_to_9digits(B.msisdn)
    group by A.msisdn,
        last_loan_date_sos_data,
        last_loan_amount_sos_data,
        second_last_loan_date_sos_data,
        second_last_loan_amount_sos_data
) D
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(D.msisdn)
left join
(
    select 
        A.msisdn,
        last_loan_date_sos_voix,
        last_loan_amount_sos_voix,
        second_last_loan_date_sos_voix,
        second_last_loan_amount_sos_voix,
		sum(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_voix then nvl(B.loan_amount, 0) else null end) avg_last_payback_amount_sos_voix,
		sum(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= second_last_loan_date_sos_voix and nvl(B.transaction_time, '0000-00-00 00:00:00') < last_loan_date_sos_voix then nvl(B.loan_amount, 0) else null end) avg_second_last_payback_amount_sos_voix,
		CONCAT_WS('#',COLLECT_LIST(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_voix then transaction_id_time_amount end)) last_payback_list_sos_voix,
		CONCAT_WS('#', COLLECT_LIST(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= second_last_loan_date_sos_voix and nvl(B.transaction_time, '0000-00-00 00:00:00') < last_loan_date_sos_voix then transaction_id_time_amount end)) second_last_payback_list_sos_voix
    from
    (
        select
            msisdn,
            transaction_time last_loan_date_sos_voix,
            amount last_loan_amount_sos_voix,
            case when transaction_time_prev = transaction_time then null else transaction_time_prev end second_last_loan_date_sos_voix,
            case when transaction_time_prev = transaction_time then null else amount_prev end second_last_loan_amount_sos_voix
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
                        msisdn,
                        amount,
                        cast(transaction_date as timestamp) transaction_time,
                        row_number() over(partition by msisdn order by cast(transaction_date as timestamp) desc) rang
                    from cdr.spark_it_zte_loan_cdr
                    where original_file_date >= add_months('###SLICE_VALUE###', -3) AND TRANSACTION_DATE <= '###SLICE_VALUE###' AND
                    lower(transaction_type) = 'loan'
                ) T
                where rang <= 2
            ) Y
        ) U 
        where rang <= 1
    ) A
    left join
    (
        select
            msisdn,
            amount loan_amount,
            cast(transaction_date as timestamp) transaction_time,
            concat(cast(transaction_date as timestamp), '@', amount) transaction_id_time_amount
        from cdr.spark_it_zte_loan_cdr
        where original_file_date >= add_months('###SLICE_VALUE###', -3) AND TRANSACTION_DATE <= '###SLICE_VALUE###' AND
        lower(transaction_type) = 'payback'
    ) B
    on fn_format_msisdn_to_9digits(A.msisdn) = fn_format_msisdn_to_9digits(B.msisdn) 
    group by A.msisdn,
        last_loan_date_sos_voix,
        last_loan_amount_sos_voix,
        second_last_loan_date_sos_voix,
        second_last_loan_amount_sos_voix
) E
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(E.msisdn)

