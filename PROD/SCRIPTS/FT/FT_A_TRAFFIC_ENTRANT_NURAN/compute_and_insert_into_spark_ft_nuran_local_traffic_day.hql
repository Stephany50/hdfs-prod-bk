INSERT INTO MON.SPARK_FT_NURAN_LOCAL_TRAFFIC_DAY
SELECT
    SITE_NAME,
    SUM(CASE WHEN TRANSACTION_TYPE = 'TEL' THEN TRANSACTION_DURATION ELSE 0 END) AS DURATION,
    SUM(CASE WHEN TRANSACTION_TYPE = 'SMS' THEN 1 ELSE 0 END) AS SMS_COUNT ,
    CURRENT_TIMESTAMP AS INSERT_DATE,
    '###SLICE_VALUE###' AS EVENT_DATE
FROM
    (
        SELECT DISTINCT A_B_NUMBER, TRANSACTION_TYPE, TRANSACTION_TIME, SITE_NAME, TRANSACTION_DURATION
        FROM
            (
                SELECT a.*
                        ,
                       COUNT(*) OVER (PARTITION BY A_B_NUMBER, TRANSACTION_TYPE ,TRANSACTION_TIME, SITE_NAME, TRANSACTION_DURATION) AS NBRE
                FROM
                    (
                        SELECT CASE WHEN SERVED_MSISDN < PARTNER_GT THEN SERVED_MSISDN || '_' || PARTNER_GT
                                    ELSE PARTNER_GT || '_' || SERVED_MSISDN
                                   END AS A_B_NUMBER, SUBSTR(TRANSACTION_TYPE, 1, 3) AS TRANSACTION_TYPE, TRANSACTION_TIME, SITE_NAME, TRANSACTION_DURATION
                        FROM (SELECT * FROM MON.SPARK_FT_MSC_TRANSACTION) C JOIN (select (case when length(ci) =2 then concat('000',ci)
            when length(ci) =3 then concat('00',ci)
            when length(ci) =4 then concat('0',ci) else ci end) ci, site_name
from dim.dt_ci_lac_site_nuran) b
                        WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
                          AND FN_NNP_SIMPLE_DESTINATION (PARTNER_GT) = 'OCM'
                          AND SUBSTR(C.SERVED_PARTY_LOCATION,14,5) = b.CI
                    ) a
            ) T2
        WHERE NBRE = 2
    ) T1
GROUP BY SITE_NAME