INSERT INTO CTI.SVI_APP_TRANSFERE --appel svi vers les cc ou cti(données brutes sur les appels transférés)
SELECT DISTINCT 
DATE_DEBUT_OMS as JOUR,
DATE_FORMAT(`date_debut_oms_nq`, 'HH:mm:ss') HEURE,
case when DATE_FORMAT(date_debut_oms_nq, 'mm')<60
    then DATE_FORMAT(date_debut_oms_nq, 'HH')||':00'
    else DATE_FORMAT(date_debut_oms_nq, 'HH')||':30' end TRANCHE_HEURE,
ID_APPEL,
MSISDN,
SERVICE AS NUMERO_APPELE,
SEGMENT_CLIENT,
TYPE_HANGUP,
LANGUE,
NUMERO_TRANSFERT,
DATE_DEBUT_OMS,
CURRENT_TIMESTAMP() INSERT_DATE,
DATE_DEBUT_OMS as EVENT_DATE
FROM CTI.SVI_APPEL 
WHERE 
DATE_DEBUT_OMS = '###SLICE_VALUE###'
AND ID_APPEL IN
(select distinct ID_APPEL FROM CTI.SVI_NAVIGATION WHERE date_element ='###SLICE_VALUE###'
AND ELEMENT like '%MiseEnRelation%' AND TYPE_ELEMENT IN ('navigation'))