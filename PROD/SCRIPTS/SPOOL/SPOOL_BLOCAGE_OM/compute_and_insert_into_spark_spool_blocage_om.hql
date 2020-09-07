insert into SPOOL.SPARK_SPOOL_BLOCAGE_OM
select
trim(replace(A.msisdn,';',' ')) as msisdn,
trim(replace(B.nom,';',' ')) as  nom,
trim(replace(B.prenom,';',' ')) as  prenom,
trim(replace(C.user_domain,';',' ')) as  user_domain,
B.registered_on,
B.account_balance as balance,
trim(replace(C.account_type,';',' ')) as account_type,
trim(replace(C.account_name,';',' ')) as account_name,
trim(replace(C.account_id,';',' ')) as account_id,
trim(replace(B.barring_reason,';',' ')) as barring_reason,
current_timestamp() as insert_date,
'###SLICE_VALUE###' as event_date
from (
         select a.* from (
         select FN_FORMAT_MSISDN_TO_9DIGITS(trim(msisdn)) as msisdn,
         trim(barring_reason) as barring_reason,
         row_number() over(partition by FN_FORMAT_MSISDN_TO_9DIGITS(trim(msisdn))
             order by sys_timestamp desc) as rang3
         from CDR.SPARK_IT_OMNY_BARRED_ACCOUNTS
         where upper(trim(action)) = 'BAR'
         and upper(trim(barring_reason)) like 'I%') a where rang3 = 1
     ) A
left join ( select a.* from(
    select FN_FORMAT_MSISDN_TO_9DIGITS(trim(msisdn)) as msisdn,
    trim(user_first_name) as prenom,
    trim(user_last_name) as nom,
    trim(user_domain) user_domain,
    registered_on,
    account_balance,
    row_number() over(partition by FN_FORMAT_MSISDN_TO_9DIGITS(trim(msisdn))
        order by modified_on desc) as rang1
    from MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT
    where event_date='###SLICE_VALUE###') a where rang1 = 1
) B on trim(A.msisdn) = trim(B.msisdn)
left join (
    select a.*
    from (
    select FN_FORMAT_MSISDN_TO_9DIGITS(trim(account_id)) as msisdn,
    trim(account_type)  account_type,
    trim(account_id) account_id,
    trim(account_name) account_name,
    trim(user_domain) user_domain,
    balance,
    row_number() over(partition by FN_FORMAT_MSISDN_TO_9DIGITS(trim(account_id))
    order by original_file_date desc) as rang2
    from cdr.spark_it_om_all_balance
    where original_file_date='###SLICE_VALUE###') a where rang2 = 1
) C on trim(A.msisdn) = trim(C.msisdn)