-- DEJA DANS LA TABLE DE BASE
SELECT
    MSISDN,
    PRENOM AS DIR_FIRST_NAME,
    NOM AS DIR_LAST_NAME,
    NEE_LE AS DIR_BIRTH_DATE,
    VILLE_VILLAGE AS DIR_IDENTIFICATION_TOWN,
    TRUNC(DATE_IDENTIFICATION) AS DIR_IDENTIFICATION_DATE
FROM DIM.DT_BASE_IDENTIFICATION