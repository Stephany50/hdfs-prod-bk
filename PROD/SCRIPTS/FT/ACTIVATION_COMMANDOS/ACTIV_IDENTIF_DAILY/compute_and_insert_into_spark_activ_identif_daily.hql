INSERT INTO MON.SPARK_ACTIV_IDENTIF_DAILY
  SELECT  a.IDENTIFIER_MSISDN,
                    COUNT(DISTINCT a.MSISDN_IDENTIFIED) AS NBRE_IDENTIFIES,
                    COUNT(DISTINCT b.ACTIVATED_MSISDN) AS NBRE_ACTIVES,
                    COUNT(DISTINCT CASE WHEN b.DATE_ACTIVATION = a.DATE_IDENTIFICATION
                                                AND b.FIRST_DAY_REFILL = b.DATE_ACTIVATION
                                            THEN b.ACTIVATED_MSISDN ELSE NULL END) AS NBRE_SUPER_ACTIVATIONS,
                    COUNT(DISTINCT CASE WHEN b.DATE_ACTIVATION IS NOT NULL
                                                AND (b.FIRST_DAY_REFILL IS NOT NULL OR
                                                        ('###SLICE_VALUE###' = LAST_DAY('###SLICE_VALUE###')
                                                         AND COALESCE(b.NEXT_MONTH_FIRST_DAY_REFIL_AMT, 0) > 0
                                                         AND DATE_ACTIVATION = '###SLICE_VALUE###')
                                                     )
                                            THEN b.ACTIVATED_MSISDN ELSE NULL END) AS NBRE_BONNES_ACTIVATIONS,
                    COUNT(DISTINCT CASE WHEN b.DATE_ACTIVATION IS NULL
                                            THEN a.MSISDN_IDENTIFIED ELSE NULL END) AS NBRE_BAD_ACTIVATIONS,
                    COUNT(DISTINCT CASE WHEN b.DATE_ACTIVATION IS NOT NULL
                                                AND (
                                                     b.FIRST_DAY_REFILL IS NULL OR
                                                        ('###SLICE_VALUE###' = LAST_DAY('###SLICE_VALUE###')
                                                         AND COALESCE(b.NEXT_MONTH_FIRST_DAY_REFIL_AMT, 0) = 0
                                                         AND DATE_ACTIVATION = '###SLICE_VALUE###')
                                                     )
                                            THEN b.ACTIVATED_MSISDN ELSE NULL END) AS NBRE_DRY_ACTIVATIONS,
                    SUM(b.REFILL_COUNT) AS RECHARGES,
                    SUM(CASE WHEN b.DATE_ACTIVATION = a.DATE_IDENTIFICATION
                                                AND b.FIRST_DAY_REFILL = b.DATE_ACTIVATION THEN b.FIRST_DAY_REFILL_AMOUNT ELSE 0 END) AS RECHARGES_IMMEDIATES,
                    SUM(b.REFILL_AMOUNT) AS RECHARGES_CUMULEES,
                    AVG(DATEDIFF(to_date(b.DATE_ACTIVATION),to_date(a.DATE_IDENTIFICATION))) AS DELAI_MOYEN,
                    COUNT(DISTINCT CASE WHEN a.EST_SNAPPE = 'OUI' THEN a.MSISDN_IDENTIFIED ELSE NULL END) AS NBRE_SNAPPES,
                    COUNT(DISTINCT CASE WHEN a.EST_SNAPPE = 'NON' THEN a.MSISDN_IDENTIFIED ELSE NULL END) AS NBRE_NON_SNAPPES,
                    COUNT(DISTINCT CASE WHEN b.DATE_ACTIVATION IS NOT NULL AND b.REFILL_AMOUNT >= 250
                                            THEN b.ACTIVATED_MSISDN ELSE NULL END) AS NBRE_ACTIVATIONS_RECH_SUP_250,
                    COUNT(DISTINCT CASE WHEN a.EST_SNAPPE = 'OUI' THEN b.ACTIVATED_MSISDN ELSE NULL END) AS NBRE_ACTIVES_SNAPPES,
                    COUNT(DISTINCT CASE WHEN a.EST_SNAPPE = 'OUI' AND b.DATE_ACTIVATION IS NOT NULL AND b.REFILL_AMOUNT >= 250
                                            THEN b.ACTIVATED_MSISDN ELSE NULL END) AS NBRE_ACTIVES_SNAP_RECH_SUP_250,
                    MAX(COALESCE(d.NB_FAKE_ACTIVATION,0)) AS NBRE_FAKE_ACTIVATION,
                    NULL AS NBRE_SUSPENDU_BYPASS ,
                    NULL  NUM_SUBS ,
                    NULL  NUM_SDT ,
                   '###SLICE_VALUE###' AS EVENT_DATE
                FROM
                (SELECT MSISDN AS MSISDN_IDENTIFIED,
                        IDENTIFICATEUR AS IDENTIFIER_MSISDN,
                        DATE_IDENTIFICATION,
                        EST_SNAPPE
                    FROM DIM.SPARK_DT_BASE_IDENTIFICATION
                    WHERE DATE_IDENTIFICATION >= concat(substr('###SLICE_VALUE###',1,7),'-01')
                        AND DATE_IDENTIFICATION <='###SLICE_VALUE###'
                ) a
                LEFT JOIN
                (SELECT ACTIVATED_MSISDN,
                        REFILL_AMOUNT,
                        DATE_ACTIVATION,
                        FIRST_DAY_REFILL,
                        NEXT_MONTH_FIRST_DAY_REFIL_AMT,
                        REFILL_COUNT,
                        FIRST_DAY_REFILL_AMOUNT
                    FROM
                    (
                        SELECT MSISDN AS ACTIVATED_MSISDN,
                            COALESCE(REFILL_AMOUNT, 0) AS REFILL_AMOUNT,
                            ACTIVATION_DATE AS DATE_ACTIVATION,
                            FIRST_DAY_REFILL,
                            NEXT_MONTH_FIRST_DAY_REFIL_AMT,
                            REFILL_COUNT,
                            FIRST_DAY_REFILL_AMOUNT,
                            ROW_NUMBER() OVER (PARTITION BY MSISDN ORDER BY ACTIVATION_DATE DESC) AS RG
                        FROM MON.SPARK_FT_ACTIVATION_MONTH1
                        WHERE EVENT_MONTH = substr('###SLICE_VALUE###',1,7)
                            AND ACTIVATION_DATE <= '###SLICE_VALUE###'
                    ) T
                    WHERE RG = 1
                ) b ON a.MSISDN_IDENTIFIED = b.ACTIVATED_MSISDN
                LEFT JOIN
                (
                  SELECT
                        IDENTIFIER_MSISDN,
                        SUM(EST_FAKE_ACTIVATION) AS NB_FAKE_ACTIVATION
                    FROM(
                     SELECT MSISDN AS MSISDN_IDENTIFIED,
                            IDENTIFICATEUR AS IDENTIFIER_MSISDN,
                            DATE_IDENTIFICATION,
                            EST_SNAPPE
                            FROM DIM.SPARK_DT_BASE_IDENTIFICATION
                            WHERE DATE_IDENTIFICATION >= concat(substr(add_months('###SLICE_VALUE###',-1),1,7),'-01')
                                AND DATE_IDENTIFICATION < date_sub('###SLICE_VALUE###',30)) a
                    JOIN (
                    SELECT  EVENT_DATE, MSISDN, EST_FAKE_ACTIVATION
                         FROM SPARK_FT_FAKE_ACTIVATION_STATUS
                         WHERE EVENT_MONTH = substr('###SLICE_VALUE###',1,7)
                            AND EVENT_DATE <= date_sub('###SLICE_VALUE###',-1)
                    ) b ON a.MSISDN_IDENTIFIED = b.MSISDN
                    GROUP BY IDENTIFIER_MSISDN
                ) d ON a.IDENTIFIER_MSISDN = d.IDENTIFIER_MSISDN
                GROUP BY a.IDENTIFIER_MSISDN;