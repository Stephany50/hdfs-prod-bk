insert into CDR.SPARK_IT_DIMELO_INTERVENTIONS
select
    CAST(created_at AS TIMESTAMP) created_at,
    CAST(updated_at AS TIMESTAMP) updated_at,
    CAST(closed_at AS TIMESTAMP) closed_at,
    closed_automatically,
    source_id,
    source_type,
    source_name,
    content_thread_id,
    id,
    status,
    deferred_at,
    user_id,
    user_name,
    user_replies_count,
    first_identity_content_id,
    first_user_reply_id,
    CAST(first_user_reply_at AS TIMESTAMP) first_user_reply_at,
    CAST(last_user_reply_at AS TIMESTAMP) last_user_reply_at,
    last_user_reply_in,
    last_user_reply_in_bh,
    first_user_reply_in,
    first_user_reply_in_bh,
    title,
    identity_id,
    identity_name,
    categories,
    comments_count,
    user_reply_in_average,
    user_reply_in_average_bh,
    user_reply_in_average_count,
    ORIGINAL_FILE_NAME,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -28, 10),'yyyy_MM_dd'))) ORIGINAL_FILE_DATE,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -28, 10),'yyyy_MM_dd'))) FILE_DATE
FROM CDR.TT_DIMELO_INTERVENTIONS C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_DIMELO_INTERVENTIONS WHERE FILE_DATE>DATE_SUB(CURRENT_DATE, 7)) T ON T.FILE_NAME = C.ORIGINAL_FILE_NAME
WHERE  T.FILE_NAME IS NULL