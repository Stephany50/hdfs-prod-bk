INSERT INTO MON.SPARK_FT_FAKE_ACTIVATION_STATUS
            SELECT
               DATE_FORMAT('###SLICE_VALUE###', 'YYYYMM') EVENT_MONTH,
               MSISDN,
               RECEIVER_MSISDN,
               (CASE WHEN b.RECEIVER_MSISDN IS NULL THEN 1 ELSE 0 END) AS EST_FAKE_ACTIVATION,
               CURRENT_TIMESTAMP INSERT_DATE,
                '###SLICE_VALUE###' AS EVENT_DATE
            FROM
            (
                -- liste des activations jour - 30
                SELECT  MSISDN ,
                        COALESCE(REFILL_AMOUNT, 0) AS REFILL_AMOUNT,
                        ACTIVATION_DATE AS DATE_ACTIVATION,
                        FIRST_DAY_REFILL,
                        NEXT_MONTH_FIRST_DAY_REFIL_AMT,
                        REFILL_COUNT,
                        FIRST_DAY_REFILL_AMOUNT,
                        ROW_NUMBER() OVER (PARTITION BY MSISDN ORDER BY ACTIVATION_DATE DESC) AS RG
                FROM MON.SPARK_FT_ACTIVATION_MONTH1
                WHERE ACTIVATION_DATE = date_sub('###SLICE_VALUE###', 30)
            ) a
            LEFT JOIN
            (
                -- liste des abonnes ayant fait des C2S ou CAG sur les 30 jours suivants
                SELECT DISTINCT RECEIVER_MSISDN
                FROM MON.SPARK_FT_REFILL
                WHERE REFILL_DATE BETWEEN date_sub('###SLICE_VALUE###', 30) AND '###SLICE_VALUE###'
               AND REFILL_MEAN IN ('C2S', 'SCRATCH')
            ) b ON a.MSISDN = b.RECEIVER_MSISDN