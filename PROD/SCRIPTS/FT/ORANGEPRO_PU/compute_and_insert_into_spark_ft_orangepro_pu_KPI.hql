INSERT INTO MON.SPARK_FT_ORANGEPRO_PU_KPI
SELECT
    social_reason,
    action,
    mgmt_act_ticket_count,
    kpi_type,
    CURRENT_TIMESTAMP insert_date,
    ORIGINAL_FILE_DATE event_date 
from 
    (select 
        social_reason, 
        action, 
        count(*) mgmt_act_ticket_count,
        'management acts' 
        kpi_type, 
        ORIGINAL_FILE_DATE 
    from CDR.SPARK_IT_ORANGEPRO_PORTAIL_UNIFIE 
    where (action like 'suspend a line' or action like 'sim change request') and ORIGINAL_FILE_DATE = '###SLICE_VALUE###' group by ORIGINAL_FILE_DATE, social_reason, action

    union

    select 
        social_reason,
        '' action, 
        count(*) mgmt_act_ticket_count,
        'tickets' kpi_type, 
        ORIGINAL_FILE_DATE 
    from CDR.SPARK_IT_ORANGEPRO_PORTAIL_UNIFIE 
    where action ='ticket assistance' and ORIGINAL_FILE_DATE = '###SLICE_VALUE###' group by ORIGINAL_FILE_DATE, social_reason
    )