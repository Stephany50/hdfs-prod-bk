create table junk.ft_contract_snpashot_localise2 as
SELECT
contract_id,
customer_id,
access_key,
account_id,
activation_date,
deactivation_date,
inactivity_begin_date,
blocked,
exhausted,
periodic_fee,
scratch_reload_susp,
commercial_offer_assign_date,
commercial_offer,
current_status,
status_date,
login,
lang,
location,
main_imsi,
msid_type,
profile,
bad_reload_attempts,
last_topup_date,
last_credit_update_date,
bad_pin_attempts,
bad_pwd_attempts,
osp_account_type,
inactivity_credit_loss,
dealer_id,
provisioning_date,
main_credit,
promo_credit,
sms_credit,
data_credit,
used_credit_month,
used_credit_life,
bundle_list,
bundle_unit_list,
promo_and_discount_list,
insert_date,
src_table,
osp_status,
bscs_comm_offer_id,
bscs_comm_offer,
initial_selection_done,
nomore_credit,
pwd_blocked,
first_event_done,
cust_ext_id,
cust_group,
cust_category,
cust_billcycle,
cust_segment,
osp_contract_type,
osp_cust_commercial_offer,
osp_customer_cglist,
osp_customer_formule,
bscs_activation_date,
bscs_deactivation_date,
operator_code,
balance_list,
previous_status,
current_status_1,
state_datetime,
nvl(location_ci_b,location_ci_a) location_ci,
a.event_date
FROM (select * from mon.spark_ft_contract_snapshot where event_date between '2021-02-03' and '2021-02-03' ) a
left join (
    select
        a.event_date,
        a.msisdn,
        max(a.location_ci) location_ci_a,
        max(b.location_ci)location_ci_b
    from (select * from mon.spark_ft_client_last_site_day where event_date between '2021-02-03' and '2021-02-03') a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where  event_date between '2021-02-03' and '2021-02-03'
    ) b on a.msisdn = b.msisdn and a.event_date=b.event_date
    group by a.event_date, a.msisdn
) site on  site.msisdn =a.access_key and site.event_date=a.event_date



alter table   mon.spark_ft_contract_snapshot drop partition (event_date='2021-02-03');
insert into mon.spark_ft_contract_snapshot select * from junk.ft_contract_snpashot_localise2;

create table junk.spark_ft_account_activity_localise as
SELECT
a.msisdn,
og_call,
ic_call_1,
ic_call_2,
ic_call_3,
ic_call_4,
status,
gp_status,
gp_status_date,
gp_first_active_date,
activation_date,
resiliation_date,
provision_date,
formule,
platform_status,
remain_credit_main,
remain_credit_promo,
language_acc,
src_table,
contract_id,
customer_id,
account_id,
login,
icc_comm_offer,
bscs_comm_offer,
bscs_status,
osp_account_type,
cust_group,
cust_billcycle,
bscs_status_date,
inactivity_begin_date,
comgp_status,
comgp_status_date,
comgp_first_active_date,
insert_date,
location_ci,
nvl(location_ci_b,location_ci_a) location_ci,
a.event_date
FROM (select * from mon.spark_ft_account_activity where event_date between '2021-02-03' and '2021-02-03' ) a
left join (
    select
        a.event_date,
        a.msisdn,
        max(a.location_ci) location_ci_a,
        max(b.location_ci)location_ci_b
    from (select * from mon.spark_ft_client_last_site_day where event_date between '2021-02-03' and '2021-02-03') a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where  event_date between '2021-02-03' and '2021-02-03'
    ) b on a.msisdn = b.msisdn and a.event_date=b.event_date
    group by a.event_date, a.msisdn
) site on  site.msisdn =a.msisdn and site.event_date=a.event_date

insert into mon.spark_ft_account_activity select
msisdn,
og_call,
ic_call_1,
ic_call_2,
ic_call_3,
ic_call_4,
status,
gp_status,
gp_status_date,
gp_first_active_date,
activation_date,
resiliation_date,
provision_date,
formule,
platform_status,
remain_credit_main,
remain_credit_promo,
language_acc,
src_table,
contract_id,
customer_id,
account_id,
login,
icc_comm_offer,
bscs_comm_offer,
bscs_status,
osp_account_type,
cust_group,
cust_billcycle,
bscs_status_date,
inactivity_begin_date,
comgp_status,
comgp_status_date,
comgp_first_active_date,
insert_date,
location_ci_1,
event_date from junk.spark_ft_account_activity_localise