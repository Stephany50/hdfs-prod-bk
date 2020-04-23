-- @TRAITEMENT :: INSERTION DES DONNÉES
-- EVENT_DATE, COMMERCIAL_OFFER, IN_BUNDLE_COUNT, OUT_BUNDLE_COUNT, RATED_COUNT, INSERT_DATE, OPERATOR_CODE_DAY
INSERT INTO MON.SPARK_FT_USERS_DATA_DAY
SELECT
       COMMERCIAL_OFFER,
       SUM(CASE WHEN BYTES_USED_IN_BUNDLE > 0 THEN 1 ELSE 0 END) AS IN_BUNDLE_COUNT,
       SUM(CASE WHEN BYTES_USED_OUT_BUNDLE > 0 THEN 1 ELSE 0 END) AS OUT_BUNDLE_COUNT,
       SUM(CASE WHEN BYTES_USED_OUT_BUNDLE > 0 OR BYTES_USED_IN_BUNDLE > 0 THEN 1 ELSE 0 END) AS RATED_COUNT,
       CURRENT_TIMESTAMP AS INSERT_DATE,
       OPERATOR_CODE,
        EVENT_DATE
FROM mon.SPARK_FT_DATA_CONSO_MSISDN_DAY
WHERE EVENT_DATE = '###SLICE_VALUE###'
GROUP BY EVENT_DATE,
       COMMERCIAL_OFFER,
       OPERATOR_CODE