 INSERT INTO MON.FT_CLIENT_LAST_SITE_DAY
                                  SELECT
                                     MSISDN, TRIM(SITE_NAME) AS SITE_NAME,
                                     TOWNNAME, ADMINISTRATIVE_REGION, COMMERCIAL_REGION,
                                     LAST_LOCATION_DAY, OPERATOR_CODE, INSERT_DATE,EVENT_DATE
                                  FROM MON.TT_CLIENT_LAST_SITE_DAY;