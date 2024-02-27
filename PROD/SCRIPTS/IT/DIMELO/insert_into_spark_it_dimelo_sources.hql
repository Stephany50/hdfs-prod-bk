insert into CDR.SPARK_IT_DIMELO_SOURCES
select
    CAST(CREATED_AT AS TIMESTAMP) CREATED_AT,
    CAST(updated_at AS TIMESTAMP) updated_at,
    id,
    active,
    name,
    status,
    community_type,
    community,
    content_archiving,
    auto_detect_content_language,
    content_languages,
    default_content_language,
    content_signature,
    hidden_from_stats,
    default_task_priority,
    channel_id,
    channel_name,
    sla_expired_strategy,
    sla_response,
    intervention_messages_boost,
    private_messages_supported,
    ORIGINAL_FILE_NAME,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -22, 10),'yyyy_MM_dd'))) ORIGINAL_FILE_DATE,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -22, 10),'yyyy_MM_dd'))) FILE_DATE
FROM CDR.SPARK_TT_DIMELO_SOURCES C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_DIMELO_SOURCES WHERE FILE_DATE>DATE_SUB(CURRENT_DATE, 7)) T ON T.FILE_NAME = C.ORIGINAL_FILE_NAME
WHERE  T.FILE_NAME IS NULL