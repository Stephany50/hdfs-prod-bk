
SELECT
NOM,
PRENOM,
DATE_NAISSANCE,
NUMERO_CNI,
MSISDN,
EST_ACTIF_OM_90J,
EST_ACTIF_OM_30J,
Activation_date_TEL,
Created_date_OM,
INSERT_DATE
FROM (
    SELECT
    NOM,
    PRENOM,
    DATE_NAISSANCE,
    NUMERO_CNI,
    MSISDN,
    EST_ACTIF_OM_90J,
    EST_ACTIF_OM_30J,
    Activation_date_TEL,
    Created_date_OM,
    CURRENT_TIMESTAMP() INSERT_DATE,
    '###SLICE_VALUE###' EVENT_DATE
    FROM
    (SELECT
    A.USER_LAST_NAME AS NOM,
    A.USER_FIRST_NAME AS PRENOM,
    A.BIRTH_DATE AS DATE_NAISSANCE,
    A.ID_NUMBER AS NUMERO_CNI,
    A.MSISDN AS MSISDN,
    case when not(B.msisdn is null or trim(B.msisdn) = '') then 'OUI' else 'NON' end AS EST_ACTIF_OM_90J,
    case when not(C.msisdn is null or trim(C.msisdn) = '') then 'OUI' else 'NON' end AS EST_ACTIF_OM_30J,
    A.Activation_date_TEL AS Activation_date_TEL,
    A.REGISTERED_ON AS Created_date_OM
    FROM
    (
    SELECT
    a.*,
    b.activation_date Activation_date_TEL
    FROM
    (
    SELECT
    a.*,
    row_number() OVER (PARTITION BY a.Msisdn ORDER BY REGISTERED_ON DESC nulls last) as rn
    from MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT a
    WHERE EVENT_DATE = '###SLICE_VALUE###'
    )a

    CROSS JOIN

    MON.SPARK_FT_CONTRACT_SNAPSHOT b
    WHERE
    a.MSISDN in
    (
    select MSISDN TELEPHONE
    from MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT
    where EVENT_DATE='###SLICE_VALUE###' and upper(trim(user_domain))='SUBSCRIBER'
    )
    and trim(a.MSISDN)=trim(b.ACCESS_KEY)
    and a.EVENT_DATE=b.EVENT_DATE
    and rn=1
    ) A
    LEFT JOIN
    (
    SELECT
    distinct msisdn
    from MON.SPARK_STATIC_FT_OM_ACTIVE_USER
    where event_date in ('###SLICE_VALUE###' ,date_sub('###SLICE_VALUE###', 30),date_sub('###SLICE_VALUE###', 60))
    ) B ON trim(A.msisdn) = trim(B.msisdn)
    LEFT JOIN
    (
    SELECT
    distinct msisdn
    from MON.SPARK_STATIC_FT_OM_ACTIVE_USER
    where event_date = '###SLICE_VALUE###'
    ) C
    ON trim(A.msisdn) = trim(C.msisdn)
    ) TTT
) R
WHERE EVENT_DATE='###SLICE_VALUE###'