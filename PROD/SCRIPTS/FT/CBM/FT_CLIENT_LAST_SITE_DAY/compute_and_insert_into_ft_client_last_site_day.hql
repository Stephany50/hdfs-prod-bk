 add jar hdfs:///PROD/UDF/hive-udf-1.0.1.jar;

  create temporary function FN_GET_OPERATOR_CODE as 'cm.orange.bigdata.udf.GetOperatorCode';
  create temporary function fn_format_msisdn_to_9digits as 'cm.orange.bigdata.udf.GetNnpMsisdn9Digits';
 -- d_begin_date_MSISDN_etrangers := date_sub('###slice_value###', - 180 + 1)

                INSERT INTO FT_CLIENT_LAST_SITE_DAY
                SELECT
                   fn_format_msisdn_to_9digits(MSISDN), SITE_NAME,
                   TOWNNAME, ADMINISTRATIVE_REGION, COMMERCIAL_REGION,
                   LAST_LOCATION_DAY, OPERATOR_CODE, INSERT_DATE, EVENT_DATE
                FROM MON.FT_CLIENT_LAST_SITE_DAY
                WHERE OPERATOR_CODE <> 'UNKNOWN_OPERATOR' OR
                            (OPERATOR_CODE = 'UNKNOWN_OPERATOR' AND LAST_LOCATION_DAY >  date_sub('###slice_value###', 179));


            -- phase 2: ajouter au données par defaut (ancienne O.) de localisation, les nouvelle donnees de localisation ( N.)
                MERGE INTO MON.FT_CLIENT_LAST_SITE_DAY O
                   USING
                   (
                        -- Nouvelles donnees de localisation
                        SELECT
                            d_slice_value AS E_DATE,
                            a.MSISDN,
                            a.SITE_NAME,
                            a.TOWNNAME,
                            a.ADMINISTRATIVE_REGION,
                            a.COMMERCIAL_REGION,
                            b.LAST_LOCATION_DAY,
                            a.OPERATOR_CODE,
                            CURRENT_TIMESTAMP  INSERT_DATE
                        FROM
                        (
                            -- Recuperation de la localisation d'un abonne
                            SELECT
                                MSISDN,
                                SITE_NAME,
                                TOWNNAME,
                                ADMINISTRATIVE_REGION,
                                COMMERCIAL_REGION,
                                OPERATOR_CODE
                            FROM
                            (
                                SELECT  mon.fn_format_msisdn_to_9digits(MSISDN) MSISDN,
                                        SITE_NAME,
                                        TOWNNAME,
                                        ADMINISTRATIVE_REGION,
                                        COMMERCIAL_REGION,
                                        OPERATOR_CODE,
                                        ROW_NUMBER() OVER (PARTITION BY fn_format_msisdn_to_9digits(MSISDN) ORDER BY SUM (NVL (DUREE_SORTANT, 0) + NVL (DUREE_ENTRANT, 0) + NVL (NBRE_SMS_SORTANT, 0)  + NVL (NBRE_SMS_ENTRANT, 0) ) DESC) AS Rang
                                FROM FT_CLIENT_SITE_TRAFFIC_DAY a
                                WHERE EVENT_DATE ='###slice_value###'
                                GROUP BY fn_format_msisdn_to_9digits(MSISDN), SITE_NAME, TOWNNAME, ADMINISTRATIVE_REGION, COMMERCIAL_REGION, OPERATOR_CODE
                            )
                            WHERE Rang = 1
                        ) a,
                        (
                            -- Récupération de la date dernière localisation d'un numero
                            SELECT fn_format_msisdn_to_9digits(MSISDN) MSISDN, MAX(EVENT_DATE) AS LAST_LOCATION_DAY
                            FROM FT_CLIENT_SITE_TRAFFIC_DAY a
                            WHERE EVENT_DATE '###slice_value###'
                            GROUP BY fn_format_msisdn_to_9digits(MSISDN)
                        ) b
                        WHERE a.MSISDN = b.MSISDN
                    ) N
                   ON O.MSISDN = N.MSISDN
                   WHEN MATCHED THEN UPDATE SET
                       -- EVENT_DATE = N.E_DATE,
                        SITE_NAME = nvl (N.SITE_NAME, O.SITE_NAME),
                        TOWNNAME = nvl (N.TOWNNAME, O.TOWNNAME ),
                        ADMINISTRATIVE_REGION = nvl (N.ADMINISTRATIVE_REGION, O.ADMINISTRATIVE_REGION ),
                        COMMERCIAL_REGION = nvl (N.COMMERCIAL_REGION, O.COMMERCIAL_REGION ),
                        LAST_LOCATION_DAY = nvl (N.LAST_LOCATION_DAY, O.LAST_LOCATION_DAY ),
                        INSERT_DATE = N.INSERT_DATE
                   WHEN NOT MATCHED THEN
                        INSERT VALUES (N.E_DATE, N.MSISDN, N.SITE_NAME, N.LAST_LOCATION_DAY, N.TOWNNAME, N.ADMINISTRATIVE_REGION, N.COMMERCIAL_REGION, N.OPERATOR_CODE,N.INSERT_DATE)
                  ;