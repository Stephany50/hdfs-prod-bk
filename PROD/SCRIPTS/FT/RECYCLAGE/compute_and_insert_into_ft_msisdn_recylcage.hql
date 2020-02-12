cdr.it_account
mon.ft_omny_account_snapshot
cdr.it_zebra_master

select msisdn, count(distinct prod_state)
from tmp.tt_msisdn_recyclage
where event_date = date_add('2020-01-19', 1) and length(msisdn)=9
group by msisdn
having count( prod_state)>1 limit 10

 select event_date, case when prod_state = 'G' then 'Inactive' when prod_state = 'A' then 'Active' when prod_state= 'D' then 'One-Way Block'
    when prod_state = 'E' then 'Two-Way Block' when prod_state = 'B' then 'Termination' when prod_state = 'H' then 'Non-provisioning'
    when prod_state = 'Y' then 'UnPassIdentity' when prod_state = 'L' then 'Subscriber Locked' end prod_state_name,
    osp_status, count(*) nbre_total, count(distinct msisdn) nbre_msisdn
from tmp.tt_msisdn_recyclage
where event_date = '2020-01-20' and length(msisdn)=9
group by event_date, prod_state, osp_status
limit 5000



insert into tmp.tt_msisdn_recyclage

create table tmp.tt_msisdn_recyclage2 as
select distinct '2020-01-19' event_date, acc_nbr msisdn, subs_id, acct_id, price_plan_id, prod_state_date, block_reason, prod_state,
    case when prod_state = 'G' then 'Inactive' when prod_state = 'A' then 'Active' when prod_state = 'D' then 'One-Way Block'
        when prod_state = 'E' then 'Two-Way Block' when prod_state = 'B' then 'Termination' when prod_state = 'H' then 'Non-provisioning'
        when prod_state = 'Y' then 'UnPassIdentity' when prod_state = 'L' then 'Subscriber Locked' end prod_state_name,
    a.update_date, prod_spec_id, access_key, COMPLETED_DATE activation_date, deactivation_date, commercial_offer, osp_account_type, provisioning_date, osp_status, main_credit,
    case when d.msisdn is not null then true else false end est_present_om, d.registered_date_om registered_date_om, id_number, d.user_type, creation_date creation_date_om, account_balance om_balance,
    case when e.primary_msisdn is not null then true else false end est_present_zebra, USER_STATUS statut_zebra, current_timestamp insert_date
from(select *, row_number() OVER (PARTITION BY acc_nbr ORDER BY UPDATE_DATE DESC) rn from cdr.spark_it_zte_subs_extract where original_file_date = date_add('2020-01-19', 1) )a
left join cdr.spark_it_zte_prod_extract b on b.original_file_date >= date_add('2020-01-19', 1) and a.original_file_date = b.original_file_date  and b.prod_id = a.subs_id
left join mon.spark_ft_contract_snapshot c on c.event_date >= date_add('2020-01-19', 1) and a.original_file_date = c.event_date and c.access_key = a.acc_nbr
left join ( select d.*, row_number() OVER (PARTITION BY msisdn ORDER BY modified_on desc, registered_on desc, account_balance DESC) rn_om, first_value(registered_on) OVER (PARTITION BY msisdn ORDER BY registered_on asc) registered_date_om
    from mon.ft_omny_account_snapshot d where d.event_date >= '2020-01-19' ) d on date_sub(a.original_file_date,1)=d.event_date and a.acc_nbr = d.msisdn and rn_om=1
left join (select e.*,  row_number() over (partition by primary_msisdn order by case when USER_STATUS='Active' then 1
                when USER_STATUS='Suspend Request' then 2  when USER_STATUS='Suspended' then 3 when USER_STATUS='Removed' then 5
                when USER_STATUS='New'	then 1.1 when USER_STATUS='Delete Request' then 4 end asc, channel_user_id desc) status_order from cdr.spark_it_zebra_master e where e.transaction_date ='2020-01-19' ) on status_order=1 and date_sub(a.original_file_date, 1) = e.transaction_date and e.primary_msisdn = a.acc_nbr
where rn=1 and length(acc_nbr)=9



select distinct to_date('25/01/2020') event_date, acc_nbr msisdn, subs_id, acct_id, price_plan_id, prod_state_date, block_reason, prod_state, a.update_date, prod_spec_id, access_key,
    COMPLETED_DATE activation_date, deactivation_date, commercial_offer, osp_account_type, provisioning_date, osp_status, main_credit,
    case when d.msisdn is not null then 'true' else 'false' end est_present_om, d.registered_date_om registered_date_om, id_number, d.user_type, creation_date creation_date_om, account_balance om_balance, last_transac_om_date,
    case when e.primary_msisdn is not null then 'true' else 'false' end est_present_zebra, user_status statut_zebra, last_tranfer_date last_transac_zebra_date, current_timestamp insert_date
from ( select a.*, row_number() OVER (PARTITION BY acc_nbr ORDER BY UPDATE_DATE DESC) rn from cdr.it_zte_subs_extract a where original_file_date = to_date('25/01/2020')+1 )a
left join cdr.it_zte_prod_extract b on b.original_file_date = to_date('25/01/2020')+1 and a.original_file_date = b.original_file_date  and b.prod_id = a.subs_id
left join mon.ft_contract_snapshot c on c.event_date = to_date('25/01/2020')+1 and a.original_file_date = c.event_date and c.access_key = a.acc_nbr
left join ( select d.*, row_number() OVER (PARTITION BY msisdn ORDER BY modified_on desc, registered_on desc, account_balance DESC) rn_om, first_value(registered_on) OVER (PARTITION BY msisdn ORDER BY registered_on asc) registered_date_om
    from tango_mon.ft_omny_account_snapshot d where d.event_date = to_date('25/01/2020') ) d on rn_om=1 and a.original_file_date-1 = d.event_date  and a.acc_nbr = d.msisdn
left join ( select e.*,  row_number() over (partition by primary_msisdn order by case when USER_STATUS='Active' then 1
     when USER_STATUS='Suspend Request' then 2  when USER_STATUS='Suspended' then 3 when USER_STATUS='Removed' then 5
     when USER_STATUS='New'	then 1.1 when USER_STATUS='Delete Request' then 4 end asc, channel_user_id desc) status_order from cdr.it_zebra_master e where e.GENERATED_DATE = to_date('25/01/2020') ) e on  status_order=1 and a.original_file_date-1 = GENERATED_DATE and e.primary_msisdn = a.acc_nbr
left join tt_zebra_active_user2 f on a.acc_nbr = f.sender_msisdn and f.event_date = '27/01/2020' -- and  a.original_file_date-1 = f.event_date
left join ( select to_date('25/01/2020') event_date, msisdn, max(last_transaction_date_time) last_transac_om_date
from tango_mon.ft_om_active_user where event_date in (to_date('25/01/2020')-59,to_date('25/01/2020')-29, to_date('25/01/2020'))
group by msisdn ) g on a.acc_nbr=g.msisdn
where rn=1 and length(acc_nbr)=9

select main_msisdn, contract_id, main_imsi, contract_type, status_date, a.account_status, provisionning_date, activation_date, deactivation_date, event_date date_photo_om, msisdn, registered_on, id_number, b.account_status statut_oom, user_type, creation_date, account_balance
from cdr.it_account a
left join mon.ft_omny_account_snapshot b on b.event_date = '2020-01-02' and a.main_msisdn = b.msisdn
where a.original_file_date = '2020-01-02'


select *
from cdr.it_account a
where original_file_date = '2020-01-04' --and activation_date is not null
limit 5;
select distinct account_status -- event_date, msisdn, registered_on, id_number, account_status, user_type, creation_date, account_balance
from mon.ft_omny_account_snapshot b
where event_date = date_add('2020-01-19', 1)
    and msisdn = '693936553'

select *
from cdr.it_zebra_master
where  original_file_date=date_add('2020-01-19', 1)
limit 5;

select *
from cdr.it_zebra_master b
limit 5;