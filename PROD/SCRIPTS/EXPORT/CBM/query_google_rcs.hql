SELECT
    msisdn,
    file_date,
    SUM(register) AS register,
    SUM(provision) AS provision,
    SUM(activity) AS activity
FROM (
    SELECT msisdn, file_date, 1 AS register, 0 AS provision, 0 AS activity
    FROM CDR.SPARK_IT_GOOGLE_RCS_REGISTER_REQUESTS
    WHERE file_date = '###SLICE_VALUE###'

    UNION

    SELECT msisdn, file_date, 0 AS register, 1 AS provision, 0 AS activity
    FROM CDR.SPARK_IT_GOOGLE_RCS_PROVISION
    WHERE file_date = '###SLICE_VALUE###'

    UNION

    SELECT from_user AS msisdn, file_date, 0 AS register, 0 AS provision, 1 AS activity
    FROM CDR.SPARK_IT_GOOGLE_RCS_ACTIVITY
    WHERE file_date = '###SLICE_VALUE###'
) AS combined_data
GROUP BY msisdn, file_date