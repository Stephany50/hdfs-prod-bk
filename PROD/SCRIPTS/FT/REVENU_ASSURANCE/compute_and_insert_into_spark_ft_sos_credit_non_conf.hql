TRUE
____________________________________________________

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
    C.last_loan_date_sos_credit last_loan_date_sos_credit,
	C.last_payback_list_sos_credit last_payback_list_sos_credit,
    C.last_loan_amount_sos_credit last_loan_amount_sos_credit,
    C.avg_last_payback_amount_sos_credit avg_last_payback_amount_sos_credit,
    C.second_last_loan_date_sos_credit second_last_loan_date_sos_credit,
    C.second_last_loan_amount_sos_credit second_last_loan_amount_sos_credit,
    C.avg_second_last_payback_amount_sos_credit avg_second_last_payback_amount_sos_credit,
	C.second_last_payback_list_sos_credit second_last_payback_list_sos_credit,
    D.last_loan_date_sos_data last_loan_date_sos_data,
    D.last_payback_list_sos_data last_payback_list_sos_data,
    D.last_loan_amount_sos_data last_loan_amount_sos_data,
    D.avg_last_payback_amount_sos_data avg_last_payback_amount_sos_data,
    E.last_loan_date_sos_voix last_loan_date_sos_voix,
    E.last_payback_list_sos_voix last_payback_list_sos_voix,
    E.last_loan_amount_sos_voix last_loan_amount_sos_voix,
    E.avg_last_payback_amount_sos_voix avg_last_payback_amount_sos_voix,
    current_timestamp() insert_date,
    '2022-07-20' event_date
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
        from cdr.spark_it_zte_emergency_credit
        where transaction_date  = '2022-07-20' AND trim(LOWER(transaction_type))='loan'
    ) t1
    left JOIN 
    (
        select *
        from MON.spark_FT_CONTRACT_SNAPSHOT
        where EVENT_DATE = '2022-07-20'
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
    WHERE EVENT_DATE BETWEEN add_months('2022-07-20', -3) AND '2022-07-20' AND
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
        last_loan_datetime,
        last_loan_date_sos_credit,
        last_loan_amount_sos_credit,
        second_last_loan_date_sos_credit,
        second_last_loan_amount_sos_credit,
		sum(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_credit then nvl(A4.loan_amount, 0) else null end) avg_last_payback_amount_sos_credit,
        sum(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= second_last_loan_date_sos_credit and nvl(A4.transaction_time, '0000-00-00 00:00:00') < last_loan_date_sos_credit then nvl(A4.loan_amount, 0) else null end) avg_second_last_payback_amount_sos_credit,
		CONCAT_WS('#',COLLECT_LIST(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_credit then transaction_id_time_amount end)) last_payback_list_sos_credit,
        CONCAT_WS('#', COLLECT_LIST(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= second_last_loan_date_sos_credit and nvl(A4.transaction_time, '0000-00-00 00:00:00') < last_loan_date_sos_credit then transaction_id_time_amount end)) second_last_payback_list_sos_credit
    from
    (
        select
            msisdn,
            last_loan_datetime,
            transaction_time last_loan_date_sos_credit,
            loan_amount last_loan_amount_sos_credit,
            case when transaction_time_prev = transaction_time then null else transaction_time_prev end second_last_loan_date_sos_credit,
            case when transaction_time_prev = transaction_time then null else loan_amount_prev end second_last_loan_amount_sos_credit
        from 
        (
            select
                msisdn,
                last_loan_datetime,
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
                    last_loan_datetime,
                    loan_amount
                from
                (

					select 
						A.msisdn, 
                        last_loan_datetime, 
                        cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) transaction_time,
                        amount loan_amount,
                        row_number() over(partition by A.msisdn order by cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) desc) rang
					from
					(
						select *
						from cdr.spark_it_zte_emergency_credit
						where TRANSACTION_DATE >= add_months('2022-07-20', -3) AND TRANSACTION_DATE <= '2022-07-20' AND 
						trim(LOWER(transaction_type))='loan'
					) A
					right join
					(
						select 
							msisdn, 
							cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) last_loan_datetime
						from cdr.spark_it_zte_emergency_credit
						where transaction_date = '2022-07-20' AND trim(LOWER(transaction_type))='loan'
					) B 
					on A.msisdn = B.msisdn 
                    where A.msisdn is not null and cast(concat(A.transaction_date, ' ', concat(substr(A.transaction_time, 1, 2), ':', substr(A.transaction_time, 3, 2), ':', substr(A.transaction_time, 5, 2))) as TIMESTAMP) < last_loan_datetime
					
                    
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
			from cdr.spark_it_zte_emergency_credit
			where TRANSACTION_DATE >= add_months('2022-07-20', -3) AND TRANSACTION_DATE <= '2022-07-20' AND 
			trim(LOWER(transaction_type))='payback'
		) A
		right join
		(
			select 
				msisdn, 
				cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) last_loan_datetime
			from cdr.spark_it_zte_emergency_credit
			where transaction_date = '2022-07-20' AND trim(LOWER(transaction_type))='loan'
		) B 
		on A.msisdn = B.msisdn 
        where A.msisdn is not null and cast(concat(A.transaction_date, ' ', concat(substr(A.transaction_time, 1, 2), ':', substr(A.transaction_time, 3, 2), ':', substr(A.transaction_time, 5, 2))) as TIMESTAMP) < last_loan_datetime
		
    ) A4
    on fn_format_msisdn_to_9digits(A3.msisdn) = fn_format_msisdn_to_9digits(A4.msisdn) 
	group by A3.msisdn,
        last_loan_date_sos_credit,
        last_loan_amount_sos_credit,
        second_last_loan_date_sos_credit,
        last_loan_datetime,
        second_last_loan_amount_sos_credit
) C
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(C.msisdn) and loan_date=C.last_loan_datetime
left join
(

    select
        A.msisdn,
        last_loan_datetime,
        last_loan_date_sos_data,
        last_loan_amount_sos_data,
		sum(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_data then nvl(B.loan_amount, 0) else null end) avg_last_payback_amount_sos_data,
		CONCAT_WS('#',COLLECT_LIST(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_data then transaction_id_time_amount end)) last_payback_list_sos_data
    from
    (
        select
            msisdn,
            last_loan_datetime,
            transaction_time last_loan_date_sos_data,
            amount last_loan_amount_sos_data
        from
        (
            select
                msisdn,
                last_loan_datetime,
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
                    last_loan_datetime,
                    amount
                from
                (
					
					select 
						A.msisdn msisdn,
						amount,
                        last_loan_datetime,
						cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) transaction_time,
						row_number() over(partition by A.msisdn order by cast(concat(transaction_date, ' ', substr(transaction_time,1,2),':',substr(transaction_time,3,2),':',substr(transaction_time,5,2)) as timestamp) desc) rang
					from
					(
						select *
						from cdr.spark_it_zte_emergency_data
						where TRANSACTION_DATE >= add_months('2022-07-20', -3) AND TRANSACTION_DATE <= '2022-07-20' AND 
						trim(LOWER(transaction_type))='loan'
					) A
					right join
					(
						select 
							msisdn, 
							cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) last_loan_datetime
						from cdr.spark_it_zte_emergency_credit
						where transaction_date = '2022-07-20' AND trim(LOWER(transaction_type))='loan'
					) B 
					on A.msisdn = B.msisdn 					
					where A.msisdn is not null and cast(concat(A.transaction_date, ' ', concat(substr(A.transaction_time, 1, 2), ':', substr(A.transaction_time, 3, 2), ':', substr(A.transaction_time, 5, 2))) as TIMESTAMP) < last_loan_datetime

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
			where TRANSACTION_DATE >= add_months('2022-07-20', -3) AND TRANSACTION_DATE <= '2022-07-20' AND 
			trim(LOWER(transaction_type))='payback'
		) A
		right join
		(
			select 
				msisdn, 
				cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) last_loan_datetime
			from cdr.spark_it_zte_emergency_credit
			where transaction_date = '2022-07-20' AND trim(LOWER(transaction_type))='loan'
		) B 
		on A.msisdn = B.msisdn 	
		where A.msisdn is not null and cast(concat(A.transaction_date, ' ', concat(substr(A.transaction_time, 1, 2), ':', substr(A.transaction_time, 3, 2), ':', substr(A.transaction_time, 5, 2))) as TIMESTAMP) < last_loan_datetime
    ) B
    on fn_format_msisdn_to_9digits(A.msisdn) = fn_format_msisdn_to_9digits(B.msisdn)
    group by A.msisdn,
        last_loan_date_sos_data,
        last_loan_amount_sos_data,
        last_loan_datetime
) D
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(D.msisdn) and loan_date=D.last_loan_datetime
left join
(
    select 
        A.msisdn,
        last_loan_datetime,
        last_loan_date_sos_voix,
        last_loan_amount_sos_voix,
		sum(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_voix then nvl(B.loan_amount, 0) else null end) avg_last_payback_amount_sos_voix,
		CONCAT_WS('#',COLLECT_LIST(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_voix then transaction_id_time_amount end)) last_payback_list_sos_voix
    from
    (
        select
            msisdn,
            last_loan_datetime,
            transaction_time last_loan_date_sos_voix,
            amount last_loan_amount_sos_voix
        from
        (
            select
                msisdn,
                last_loan_datetime,
                max(transaction_time) over(partition by msisdn order by transaction_time desc) transaction_time,
                first_value(amount) over(partition by msisdn order by transaction_time desc) amount,
                min(transaction_time) over(partition by msisdn order by transaction_time asc) transaction_time_prev,
                first_value(amount) over(partition by msisdn order by transaction_time asc) amount_prev,
                row_number() over(partition by msisdn order by transaction_time desc) rang
            from
            (
                select
                    msisdn,
                    last_loan_datetime,
                    transaction_time,
                    amount
                from
                (
					
					select 
						A.msisdn msisdn,
						amount,
                        last_loan_datetime,
						cast(transaction_date as timestamp) transaction_time,
						row_number() over(partition by A.msisdn order by cast(transaction_date as timestamp) desc) rang
					from
					(
						select *
						from cdr.spark_it_zte_loan_cdr
						where original_file_date >= add_months('2022-07-20', -3) AND TRANSACTION_DATE <= '2022-07-20' AND
						lower(transaction_type) = 'loan'
					) A
					right join
					(
						select 
							msisdn, 
							cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) last_loan_datetime
						from cdr.spark_it_zte_emergency_credit
						where transaction_date = '2022-07-20' AND trim(LOWER(transaction_type))='loan'
					) B 
					on A.msisdn = B.msisdn 
                    where A.msisdn is not null and cast(transaction_date as timestamp) < last_loan_datetime	
					
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
			where original_file_date >= add_months('2022-07-20', -3) AND TRANSACTION_DATE <= '2022-07-20' AND
			lower(transaction_type) = 'payback'
		) A
		right join
		(
			select 
				msisdn, 
				cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) last_loan_datetime
			from cdr.spark_it_zte_emergency_credit
			where transaction_date = '2022-07-20' AND trim(LOWER(transaction_type))='loan'

		) B 
		on A.msisdn = B.msisdn 
        where A.msisdn is not null and cast(transaction_date as timestamp) < last_loan_datetime	
		
    ) B
    on fn_format_msisdn_to_9digits(A.msisdn) = fn_format_msisdn_to_9digits(B.msisdn) 
    group by A.msisdn,
        last_loan_date_sos_voix,
        last_loan_amount_sos_voix,
        last_loan_datetime
) E
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(E.msisdn) and loan_date=E.last_loan_datetime

Optimisation _new_
________________________________________________________________

create table tmp.ccrredit as
select *
from cdr.spark_it_zte_emergency_credit
where transaction_date  = '2022-07-20'

create table tmp.llooan_cdr as
select *
from cdr.spark_it_zte_loan_cdr
where original_file_date >= add_months('2022-07-20', -3) AND TRANSACTION_DATE <= '2022-07-20'

create table tmp.emergenncy_data as 
select *
from cdr.spark_it_zte_emergency_data
where TRANSACTION_DATE >= add_months('2022-07-20', -3) AND TRANSACTION_DATE <= '2022-07-20'

create table tmp.emergenncy_credit as 
select *
from cdr.spark_it_zte_emergency_credit
where TRANSACTION_DATE >= add_months('2022-07-20', -3) AND TRANSACTION_DATE <= '2022-07-20'

create table tmp.eqt as 
SELECT 
ACCT_ID_MSISDN msisdn, 
sum(main_credit)/count(event_time) avg_recharge_last_three_month, 
sum(main_credit)/3 as monthly_avg_recharge,
(0.4*avg(main_credit)) plafond
FROM MON.spark_FT_EDR_PRPD_EQT
WHERE EVENT_DATE BETWEEN add_months('2022-07-20', -3) AND '2022-07-20' AND
(UPPER(TYPE) LIKE 'RECURRING%' OR UPPER(TYPE) in ('ADJUSTMENT CHARGE POINT OF SAL', 'TRANSFER P2P RECV CREDIT', 'SUBSCRIPTION DEPOT FLEX', 'RECHARGE VC RECHARGE 4', 'SUBSCRIPTION DEPOT HYBRID', 'RECHARGE E-TOPUP 11', 'SUBSCRIPTION DEPOT BENEFIT'))         
GROUP BY ACCT_ID_MSISDN


create table tmp.sos_emer_credit as 
-- Pas d emprunt en cours sur SOS Data ou SOS Voix
-- ICI nous aimerons avoir le dernier SOS DATA et le dernier SOS DATA avant la souscription SOS CREDIT ainsi que le montant remboursé pour chacun d’entre eux
-- Maximum 1 emprunt en cours sur SOS Credit (ce que nous essayons de faire avec ses deux requêtes) 
-- CONFORMITE DES SOUSCRIPTIONS AU SOS CREDIT : extraction d du dernier emprunt SOS CREDIT sur les 4 derniers mois
select
A3.msisdn,
last_loan_datetime,
last_loan_date_sos_credit,
last_loan_amount_sos_credit,
second_last_loan_date_sos_credit,
second_last_loan_amount_sos_credit,
sum(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_credit then nvl(A4.loan_amount, 0) else null end) avg_last_payback_amount_sos_credit,
sum(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= second_last_loan_date_sos_credit and nvl(A4.transaction_time, '0000-00-00 00:00:00') < last_loan_date_sos_credit then nvl(A4.loan_amount, 0) else null end) avg_second_last_payback_amount_sos_credit,
CONCAT_WS('#',COLLECT_LIST(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_credit then transaction_id_time_amount end)) last_payback_list_sos_credit,
CONCAT_WS('#', COLLECT_LIST(case when nvl(A4.transaction_time, '0000-00-00 00:00:00') >= second_last_loan_date_sos_credit and nvl(A4.transaction_time, '0000-00-00 00:00:00') < last_loan_date_sos_credit then transaction_id_time_amount end)) second_last_payback_list_sos_credit
from
(
select
msisdn,
last_loan_datetime,
transaction_time last_loan_date_sos_credit,
loan_amount last_loan_amount_sos_credit,
case when transaction_time_prev = transaction_time then null else transaction_time_prev end second_last_loan_date_sos_credit,
case when transaction_time_prev = transaction_time then null else loan_amount_prev end second_last_loan_amount_sos_credit
from 
(
select
msisdn,
last_loan_datetime,
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
last_loan_datetime,
loan_amount
from
(

select 
A.msisdn, 
last_loan_datetime, 
cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) transaction_time,
amount loan_amount,
row_number() over(partition by A.msisdn order by cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) desc) rang
from
(
select *
from tmp.emergenncy_credit
where trim(LOWER(transaction_type))='loan'
) A
right join
(
select 
msisdn, 
cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) last_loan_datetime
from tmp.ccrredit
where trim(LOWER(transaction_type))='loan'
) B 
on A.msisdn = B.msisdn 
where A.msisdn is not null and cast(concat(A.transaction_date, ' ', concat(substr(A.transaction_time, 1, 2), ':', substr(A.transaction_time, 3, 2), ':', substr(A.transaction_time, 5, 2))) as TIMESTAMP) < last_loan_datetime


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
concat(cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP), '@', amount) transaction_id_time_amount
from
(
select *
from tmp.emergenncy_credit
where trim(LOWER(transaction_type))='payback'
) A
right join
(
select 
msisdn, 
cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) last_loan_datetime
from tmp.ccrredit
where trim(LOWER(transaction_type))='loan'
) B 
on A.msisdn = B.msisdn 
where A.msisdn is not null and cast(concat(A.transaction_date, ' ', concat(substr(A.transaction_time, 1, 2), ':', substr(A.transaction_time, 3, 2), ':', substr(A.transaction_time, 5, 2))) as TIMESTAMP) < last_loan_datetime

) A4
on fn_format_msisdn_to_9digits(A3.msisdn) = fn_format_msisdn_to_9digits(A4.msisdn) 
group by A3.msisdn,
last_loan_date_sos_credit,
last_loan_amount_sos_credit,
second_last_loan_date_sos_credit,
last_loan_datetime,
second_last_loan_amount_sos_credit

create table tmp.sos_data_lot1 as 
select 
A.msisdn msisdn,
amount,
last_loan_datetime,
cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) transaction_time,
row_number() over(partition by A.msisdn order by cast(concat(transaction_date, ' ', substr(transaction_time,1,2),':',substr(transaction_time,3,2),':',substr(transaction_time,5,2)) as timestamp) desc) rang
from
(
select *
from tmp.emergenncy_data
where trim(LOWER(transaction_type))='loan'
) A
right join
(
select 
msisdn, 
cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) last_loan_datetime
from tmp.ccrredit
where trim(LOWER(transaction_type))='loan'
) B 
on A.msisdn = B.msisdn 
where A.msisdn is not null and cast(concat(A.transaction_date, ' ', concat(substr(A.transaction_time, 1, 2), ':', substr(A.transaction_time, 3, 2), ':', substr(A.transaction_time, 5, 2))) as TIMESTAMP) < last_loan_datetime

create table tmp.sos_data_lot2_0 as 
select 
A.msisdn msisdn,
amount loan_amount,
last_loan_datetime,
transaction_time,
A.transaction_date transaction_date
from
(
select *
from tmp.emergenncy_data
where trim(LOWER(transaction_type))='payback'
) A
right join
(
select 
msisdn, 
cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) last_loan_datetime
from tmp.ccrredit
where trim(LOWER(transaction_type))='loan'
) B 
on A.msisdn = B.msisdn

create table tmp.sos_data_lot2 as 
select 
A.msisdn msisdn,
loan_amount,
cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) transaction_time,
concat(cast(transaction_date as timestamp), '@', loan_amount) transaction_id_time_amount
from tmp.sos_data_lot2_0 A
where A.msisdn is not null and cast(concat(A.transaction_date, ' ', concat(substr(A.transaction_time, 1, 2), ':', substr(A.transaction_time, 3, 2), ':', substr(A.transaction_time, 5, 2))) as TIMESTAMP) < last_loan_datetime

create table tmp.sos_emergency_data as
select
A.msisdn,
last_loan_datetime,
last_loan_date_sos_data,
last_loan_amount_sos_data,
sum(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_data then nvl(B.loan_amount, 0) else null end) avg_last_payback_amount_sos_data,
CONCAT_WS('#',COLLECT_LIST(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_data then transaction_id_time_amount end)) last_payback_list_sos_data
from
(
select
msisdn,
last_loan_datetime,
transaction_time last_loan_date_sos_data,
amount last_loan_amount_sos_data
from
(
select
msisdn,
last_loan_datetime,
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
last_loan_datetime,
amount
from
(
select * from tmp.sos_data_lot1
) T
where rang <= 2
) Y
) U
where rang <= 1
) A
left join
(
select * from tmp.sos_data_lot2
) B
on fn_format_msisdn_to_9digits(A.msisdn) = fn_format_msisdn_to_9digits(B.msisdn)
group by A.msisdn,
last_loan_date_sos_data,
last_loan_amount_sos_data,
last_loan_datetime



create table tmp.sos_loan as
select 
A.msisdn,
last_loan_datetime,
last_loan_date_sos_voix,
last_loan_amount_sos_voix,
sum(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_voix then nvl(B.loan_amount, 0) else null end) avg_last_payback_amount_sos_voix,
CONCAT_WS('#',COLLECT_LIST(case when nvl(B.transaction_time, '0000-00-00 00:00:00') >= last_loan_date_sos_voix then transaction_id_time_amount end)) last_payback_list_sos_voix
from
(
select
msisdn,
last_loan_datetime,
transaction_time last_loan_date_sos_voix,
amount last_loan_amount_sos_voix
from
(
select
msisdn,
last_loan_datetime,
max(transaction_time) over(partition by msisdn order by transaction_time desc) transaction_time,
first_value(amount) over(partition by msisdn order by transaction_time desc) amount,
min(transaction_time) over(partition by msisdn order by transaction_time asc) transaction_time_prev,
first_value(amount) over(partition by msisdn order by transaction_time asc) amount_prev,
row_number() over(partition by msisdn order by transaction_time desc) rang
from
(
select
msisdn,
last_loan_datetime,
transaction_time,
amount
from
(

select 
A.msisdn msisdn,
amount,
last_loan_datetime,
cast(transaction_date as timestamp) transaction_time,
row_number() over(partition by A.msisdn order by cast(transaction_date as timestamp) desc) rang
from
(
select *
from tmp.llooan_cdr
where lower(transaction_type) = 'loan'
) A
right join
(
select 
msisdn, 
cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) last_loan_datetime
from tmp.ccrredit
where trim(LOWER(transaction_type))='loan'
) B 
on A.msisdn = B.msisdn 
where A.msisdn is not null and cast(transaction_date as timestamp) < last_loan_datetime	

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
from tmp.llooan_cdr
where lower(transaction_type) = 'payback'
) A
right join
(
select 
msisdn, 
cast(concat(transaction_date, ' ', concat(substr(transaction_time, 1, 2), ':', substr(transaction_time, 3, 2), ':', substr(transaction_time, 5, 2))) as TIMESTAMP) last_loan_datetime
from tmp.ccrredit
where trim(LOWER(transaction_type))='loan'

) B 
on A.msisdn = B.msisdn 
where A.msisdn is not null and cast(transaction_date as timestamp) < last_loan_datetime	

) B
on fn_format_msisdn_to_9digits(A.msisdn) = fn_format_msisdn_to_9digits(B.msisdn) 
group by A.msisdn,
last_loan_date_sos_voix,
last_loan_amount_sos_voix,
last_loan_datetime

-- Final

create table tmp.sos_credit_final_stage as
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
C.last_loan_date_sos_credit last_loan_date_sos_credit,
C.last_payback_list_sos_credit last_payback_list_sos_credit,
C.last_loan_amount_sos_credit last_loan_amount_sos_credit,
C.avg_last_payback_amount_sos_credit avg_last_payback_amount_sos_credit,
C.second_last_loan_date_sos_credit second_last_loan_date_sos_credit,
C.second_last_loan_amount_sos_credit second_last_loan_amount_sos_credit,
C.avg_second_last_payback_amount_sos_credit avg_second_last_payback_amount_sos_credit,
C.second_last_payback_list_sos_credit second_last_payback_list_sos_credit,
D.last_loan_date_sos_data last_loan_date_sos_data,
D.last_payback_list_sos_data last_payback_list_sos_data,
D.last_loan_amount_sos_data last_loan_amount_sos_data,
D.avg_last_payback_amount_sos_data avg_last_payback_amount_sos_data,
E.last_loan_date_sos_voix last_loan_date_sos_voix,
E.last_payback_list_sos_voix last_payback_list_sos_voix,
E.last_loan_amount_sos_voix last_loan_amount_sos_voix,
E.avg_last_payback_amount_sos_voix avg_last_payback_amount_sos_voix,
current_timestamp() insert_date,
'2022-07-20' event_date
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
from tmp.ccrredit
where trim(LOWER(transaction_type))='loan'
) t1
left JOIN 
(
select *
from MON.spark_FT_CONTRACT_SNAPSHOT
where EVENT_DATE = '2022-07-20'
) t2 
ON fn_format_msisdn_to_9digits(t1.msisdn) = fn_format_msisdn_to_9digits(t2.ACCESS_KEY)
) T
left join
(
-- Recharge moyenne mensuelle des 3 derniers mois >= 250
-- Plafond d emprunt non atteint sur SOS Credit : 40% de la recharge moyenne des 3 derniers mois.
-- CONFORMITE DES SOUSCRIPTIONS AU SOS CREDIT : extraction de la moyenne des recharges des trois dernier mois et calcul du plafond         
SELECT * FROM tmp.eqt

) B
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(B.msisdn)
left join
(
select * from tmp.sos_emer_credit
) C
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(C.msisdn) and loan_date=C.last_loan_datetime
left join
(
select * from tmp.sos_emergency_data
) D
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(D.msisdn) and loan_date=D.last_loan_datetime
left join
(
select * from tmp.sos_loan
) E
on fn_format_msisdn_to_9digits(T.msisdn) = fn_format_msisdn_to_9digits(E.msisdn) and loan_date=E.last_loan_datetime

