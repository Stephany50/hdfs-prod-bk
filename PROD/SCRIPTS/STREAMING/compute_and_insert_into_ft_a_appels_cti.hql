select
DATE_FORMAT(EVENT_TIME, 'yyyyMM') MOIS,
'S'||WEEKOFYEAR(EVENT_TIME) SEMAINE,
case when DATE_FORMAT(EVENT_TIME, 'mm')<30
    then DATE_FORMAT(EVENT_TIME, 'HH')||':00'
    else DATE_FORMAT(EVENT_TIME, 'HH')||':30' end TRANCHE_30MIN,
RESOURCE_NAME AGENT_LOGIN,
NOM AGENT_NOM,
DNIS SHORT_NUMBER,
UD_site_choisi SITE,
count(*) APPELS_RECUS,
sum(IF( DUREE_CONVERSATION>0 , 1 , 0) ) APPELS_TRAITES,
sum(IF(TECHNICAL_RESULT='Abandoned' , 1 , 0)) APPELS_PERDUS_SYSTEME,
sum(IF(RESULT_REASON='AbandonedWhileRinging' , 1 , 0)) APPELS_PERDUS_SONNERIE,
sum(IF(RESULT_REASON='AbandonedWhileQueued' , 1 , 0)) APPELS_PERDUS_FILE,
sum(IF(RESULT_REASON='AbandonedFromHold' , 1 , 0)) APPELS_PERDUS_ATTENTE,
sum(case when DUREE_CONVERSATION>=16 then 1 else 0 end ) APPELS_VALIDES,
sum(case when DUREE_CONVERSATION>0 and DUREE_CONVERSATION<16 then 1 else 0 end) APPEL_INVALIDES,
count(*) TOTAL_APPELS,
sum(case when DUREE_FILE <40 then 1 else 0 end) APPELS_PRIS_40S,
sum(case when DUREE_FILE <60 then 1 else 0 end) APPELS_PRIS_60S,
sum(case when DUREE_FILE <120 then 1 else 0 end) APPELS_PRIS_120S,
sum(DUREE_CONVERSATION) DUREE_APPELS_RECUS,
sum(DUREE_CONVERSATION)/count(*) MOYENNE_APPELS_RECUS,
sum(IF( DUREE_CONVERSATION>0 , DUREE_CONVERSATION , 0) ) DUREE_APPELS_TRAITES,
sum(IF(RESULT_REASON='AbandonedWhileRinging' , DUREE_FILE , 0)) DUREE_APPELS_PERDUS_SONNERIE,
sum(IF(RESULT_REASON='AbandonedWhileQueued' , DUREE_FILE , 0)) DUREE_APPELS_PERDUS_FILE,
sum(IF(RESULT_REASON='AbandonedFromHold' , DUREE_CONVERSATION , 0)) DUREE_APPELS_PERDUS_ATTENTE,
sum(case when DUREE_CONVERSATION>=16 then DUREE_CONVERSATION else 0 end ) DUREE_APPELS_VALIDES,
sum(case when DUREE_CONVERSATION>0 and DUREE_CONVERSATION<16 then DUREE_CONVERSATION else 0 end) DUREE_APPEL_INVALIDES,
sum(DUREE_CONVERSATION) DUREE_TOTALE,
count(distinct ANI) NOMBRE_CLIENT,
TO_DATE(EVENT_TIME) JOUR
FROM (select distinct * from CTI.IT_IRF_USER_DATA_CUST
WHERE EVENT_DATE = '2020-04-02' ) a
group by
DATE_FORMAT(EVENT_TIME, 'yyyyMM'),
'S'||WEEKOFYEAR(EVENT_TIME),
TO_DATE(EVENT_TIME),
case when DATE_FORMAT(EVENT_TIME, 'mm')<30
    then DATE_FORMAT(EVENT_TIME, 'HH')||':00'
    else DATE_FORMAT(EVENT_TIME, 'HH')||':30' end,
RESOURCE_NAME ,
NOM,
DNIS ,
UD_site_choisi