INSERT INTO MON.SPARK_FT_ORANGEPRO_PU_ACTE_GESTION
SELECT id, 
    username, 
    name, 
    account_numbers, 
    '' enterprise_name, 
    email, 
    social_reason, 
    last_connection,
    action, 
    details,
    management_act,
    CURRENT_TIMESTAMP insert_date,
    ORIGINAL_FILE_DATE event_date 
from 
    (select id, 
        username, 
        name, 
        account_numbers, 
        email, 
        social_reason, 
        last_connection,action, 
        details,
        'N/A' management_act, 
        ORIGINAL_FILE_DATE 
    from CDR.SPARK_IT_ORANGEPRO_PORTAIL_UNIFIE 
    where action like 'logged in' and ORIGINAL_FILE_DATE = '###SLICE_VALUE###'

    union

    select 
        id, 
        username, 
        name, 
        account_numbers, 
        email, 
        social_reason, 
        last_connection,
        action, 
        details, 
        'YES' management_act, 
        ORIGINAL_FILE_DATE 
    from CDR.SPARK_IT_ORANGEPRO_PORTAIL_UNIFIE 
    where (action like 'suspend a line' or action like 'sim change request') and ORIGINAL_FILE_DATE = '###SLICE_VALUE###'

    union

    select id, 
        username, 
        name, 
        account_numbers, 
        email, 
        social_reason, 
        last_connection,
        action, 
        details, 
        'NO' management_act, 
        ORIGINAL_FILE_DATE 
    from CDR.SPARK_IT_ORANGEPRO_PORTAIL_UNIFIE 
    where (action not like 'suspend a line' and action not like 'sim change request') and ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
    )