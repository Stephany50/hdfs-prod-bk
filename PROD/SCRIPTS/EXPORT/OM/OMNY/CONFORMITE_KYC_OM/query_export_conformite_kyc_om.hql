SELECT
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(nom,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS nom,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(prenom,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS prenom,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(date_naissance,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS date_naissance,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(numero_cni,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS numero_cni,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(msisdn,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS msisdn,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(est_actif_om_90j,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS est_actif_om_90j,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(est_actif_om_30j,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS est_actif_om_30j,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(activation_date_tel,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS activation_date_tel,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(created_date_om,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS created_date_om,
cast(CURRENT_TIMESTAMP() as string) as INSERT_DATE,
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