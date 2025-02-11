SELECT
    original_file_name,
    original_file_size,
    original_file_line_count,
    user_id,
    profile_id,
    parent_user_id,
    parent_user_msisdn,
    msisdn,
    user_name_prefix,
    user_first_name,
    user_last_name,
    user_short_name,
    dob,
    registered_on,
    address1,
    address2,
    state,
    city,
    country,
    ssn,
    designation,
    division,
    contact_person,
    contact_no,
    employee_code,
    sex,
    id_number,
    e_mail,
    web_login,
    account_status,
    creation_date,
    created_by,
    created_by_msisdn,
    nomade_created_by,
    level1_app_date,
    level1_app_by,
    level2_app_date,
    level2_app_by,
    owner_id,
    owner_msisdn,
    user_domain_code,
    user_category_code,
    user_grade_name,
    modified_by,
    modified_on,
    modified_approved_by,
    modified_approved_on,
    deleted_on,
    deactivation_by,
    department,
    registration_form_number,
    remarks,
    geographical_domain,
    group_role,
    first_transaction_on,
    company_code,
    user_type,
    action_type,
    agent_code,
    creation_type,
    bulk_id,
    identity_proof_type,
    address_proof_type,
    photo_proof_type,
    id_type,
    id_no,
    id_issue_place,
    id_issue_date,
    id_issue_country,
    id_expiry_date,
    residence_country,
    nationality,
    employer_name,
    postal_code,
    souscription_type,
    mobile_group_role,
    last_login_on,
    user_grade_code,
    parent_first_name,
    parent_last_name,
    owner_first_name,
    owner_last_name,
    cast(insert_date as string) insert_date,
    original_file_date
FROM CDR.SPARK_IT_OM_ALL_USERS 
WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###' 

