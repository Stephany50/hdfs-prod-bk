INSERT INTO MON.SPARK_FT_REPORTING_DIMELO
SELECT
    created_at,
    periode,
    user_id,
    user_name,
    SUM(expired_sum) expired_sum,
    SUM(transferred_sum) transferred_sum,
    SUM(terminated_sum) terminated_sum,
    SUM(deferred_sum) deferred_sum,
    SUM(restablished_sum) restablished_sum,
    SUM(assignes_sum) assignes_sum,
    SUM(acceptes_sum) acceptes_sum,
    SUM(missed_sum) missed_sum,
    team,
    channel,
    '###SLICE_VALUE###' EVENT_DATE
FROM
    (SELECT
        to_date(B.created_at) as created_at,
        B.periode,
        user_id,
        user_name,
        CASE WHEN LOWER(trim(B.name)) = 'task.expired_from_workbin' THEN 1 ELSE 0 END as expired_sum,
        CASE WHEN LOWER(trim(B.name)) = 'task.transferred' THEN 1 ELSE 0 END as transferred_sum,
        CASE WHEN LOWER(trim(B.name)) = 'task.completed' and LOWER(trim(B.action))='close' THEN 1 ELSE 0 END as terminated_sum,
        CASE WHEN LOWER(trim(B.action)) ='deferred' THEN 1 ELSE 0 END as deferred_sum,
        CASE WHEN LOWER(trim(B.name)) = 'task.resume' THEN 1 ELSE 0 END as restablished_sum,
        CASE WHEN LOWER(trim(B.name)) = 'task.supervisor_assigned' THEN 1 ELSE 0 END as assignes_sum,
        CASE WHEN LOWER(trim(B.name)) = 'task.taken' THEN 1 ELSE 0 END as acceptes_sum,
        CASE WHEN LOWER(trim(B.name)) = 'push.agent.missed_task' THEN 1 ELSE 0 END as missed_sum,
        CASE WHEN A.id IS NOT NULL THEN A.teams ELSE ' ' END as team,
        CASE WHEN B.content_source_id is not NULL AND C.id is not null THEN C.name ELSE ' ' END as channel
    FROM
        (SELECT
            CASE 
                WHEN HOUR(created_at) > 7 AND HOUR(created_at) < 22 THEN 'HO'
                ELSE 'HNO'
            END as periode,
            user_id,
            user_name,
            name,
            action,
            created_at, 
            content_source_id
        FROM (SELECT * FROM CDR.SPARK_IT_DIMELO_JOURNAL where file_date='###SLICE_VALUE###' and user_id is not null)PP)B
        LEFT JOIN
        (SELECT * FROM CDR.SPARK_IT_DIMELO_AGENTS where file_date='###SLICE_VALUE###')A
        ON trim(B.user_id) = trim(A.id) 
        LEFT JOIN
        (SELECT * FROM CDR.SPARK_IT_DIMELO_SOURCES where file_date='###SLICE_VALUE###' and id is not null)C
        ON trim(B.content_source_id) = trim(C.id)
    )RESULT
GROUP BY created_at,periode,user_id,user_name,team,channel


