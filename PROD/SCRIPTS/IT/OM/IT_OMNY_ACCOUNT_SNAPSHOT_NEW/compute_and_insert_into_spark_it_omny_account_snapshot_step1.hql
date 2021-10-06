--Etape 1: Recuperation de toutes les MAJ sur les comptes en dehors du solde.
INSERT INTO TMP.TT_SPARK_IT_OMNY_ACCOUNT_SNAPSHOT
select
    nvl(subscribers.user_first_name, account_snapshot.user_name ) user_name,
    nvl(subscribers.user_last_name, account_snapshot.last_name ) last_name,
    nvl(subscribers.creation_date, account_snapshot.created_on ) created_on,
    nvl(subscribers.modified_on, account_snapshot.modified_on ) modified_on,
    nvl(subscribers.dob, account_snapshot.last_transfer_on ) last_transfer_on,
    nvl(subscribers.msisdn, account_snapshot.msisdn ) msisdn,
    nvl(subscribers.user_id, account_snapshot.user_id ) user_id,
    account_snapshot.balance  balance,
    account_snapshot.payment_type_id payment_type_id,
    nvl(subscribers.id_number, account_snapshot.external_code ) external_code,
    nvl(subscribers.account_status, account_snapshot.is_active ) is_active,
    nvl(subscribers.dob, account_snapshot.birth_date ) birth_date,
    nvl(subscribers.user_type, account_snapshot.user_type ) user_type,
    nvl(domain_details.domain_name, nvl(subscribers.user_domain_code, account_snapshot.user_domain))  user_domain,
    nvl(category_details.category_name, nvl(subscribers.user_category_code, account_snapshot.user_category)) user_category,
    '' original_file_name,
    0 original_file_size,
    0 original_file_line_count
from
    (---Remonter des comptes sans doublons et possédant un user_id non null, qui servira de clé de jointure
        select * from (select user_name,last_name,created_on,modified_on,last_transfer_on, msisdn,user_id,balance,payment_type_id,external_code,is_active,birth_date,user_type,
        user_domain,user_category,original_file_name,original_file_size,original_file_line_count,row_number() over(partition by user_name,last_name,created_on,modified_on,last_transfer_on,
        msisdn,user_id,external_code,is_active,birth_date,user_type,user_domain,user_category order by user_name,last_name,created_on DESC nulls last) as RANG
        from CDR.SPARK_IT_OMNY_ACCOUNT_SNAPSHOT_NEW where original_file_date=DATE_SUB("###SLICE_VALUE###",1)) where rang=1 and user_id is not null
    ) account_snapshot
    full outer join (
        select user_first_name, user_last_name, creation_date, modified_on, dob, msisdn, trim(user_id) user_id, id_number, account_status, user_type, user_domain_code, user_category_code from cdr.spark_it_om_subscribers where file_date=DATE_SUB("###SLICE_VALUE###",1)
        union
        select user_first_name, user_last_name, creation_date, modified_on, dob, msisdn, trim(user_id) user_id, id_number, account_status, user_type, user_domain_code, user_category_code from cdr.spark_it_om_all_users where original_file_date=DATE_SUB("###SLICE_VALUE###",1)
    ) subscribers on (trim(account_snapshot.user_id) = trim(subscribers.user_id))
    left join (select * from CDR.SPARK_IT_OMNY_DOMAIN_DETAILS where original_file_date = (select max(original_file_date) from CDR.SPARK_IT_OMNY_DOMAIN_DETAILS)) domain_details on (trim(subscribers.user_domain_code) = trim(domain_details.domain_code))
    left join (select * from CDR.SPARK_IT_OMNY_CATEGORY_DETAILS where original_file_date = (select max(original_file_date) from CDR.SPARK_IT_OMNY_CATEGORY_DETAILS)) category_details on (trim(subscribers.user_category_code) = trim(category_details.category_code))
group by
    nvl(subscribers.user_first_name, account_snapshot.user_name ) ,
    nvl(subscribers.user_last_name, account_snapshot.last_name ) ,
    nvl(subscribers.creation_date, account_snapshot.created_on ) ,
    nvl(subscribers.modified_on, account_snapshot.modified_on ) ,
    nvl(subscribers.dob, account_snapshot.last_transfer_on ) ,
    nvl(subscribers.msisdn, account_snapshot.msisdn ) ,
    nvl(subscribers.user_id, account_snapshot.user_id ) ,
    account_snapshot.balance ,
    account_snapshot.payment_type_id,
    nvl(subscribers.id_number, account_snapshot.external_code ),
    nvl(subscribers.account_status, account_snapshot.is_active),
    nvl(subscribers.dob, account_snapshot.birth_date),
    nvl(subscribers.user_type, account_snapshot.user_type ) ,
    nvl(domain_details.domain_name, nvl(subscribers.user_domain_code, account_snapshot.user_domain)) ,
    nvl(category_details.category_name, nvl(subscribers.user_category_code, account_snapshot.user_category))
