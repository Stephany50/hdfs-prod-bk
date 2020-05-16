INSERT INTO AGG.FT_A_APPELS_CTI_COUNT
SELECT ft.* FROM
( select IF(count(distinct event_date)=cast(substr('###SLICE_VALUE###', -2) as int), 'OK', 'NOK') result
from  CTI.IT_IRF_USER_DATA_CUST
where last_day('###SLICE_VALUE###')='###SLICE_VALUE###' and event_date between date_sub('###SLICE_VALUE###', cast(substr('###SLICE_VALUE###', -2) as int)-1) and '###SLICE_VALUE###' ) a
LEFT JOIN ( select count(*) nb_mois from AGG.FT_A_APPELS_CTI_COUNT where mois=DATE_FORMAT('###SLICE_VALUE###', 'yyyyMM') and type_periode='MOIS' ) b ON 1=1
LEFT JOIN (
select DATE_FORMAT(EVENT_DATE, 'yyyyMM') MOIS, NULL semaine, 'MOIS' TYPE_PERIODE, count(distinct ANI) NOMBRE_CLIENT, CURRENT_TIMESTAMP() INSERT_DATE, MAX(EVENT_DATE) EVENT_DATE
from CTI.IT_IRF_USER_DATA_CUST where last_day('###SLICE_VALUE###')='###SLICE_VALUE###' and event_date between date_sub('###SLICE_VALUE###', cast(substr('###SLICE_VALUE###', -2) as int)-1) and '###SLICE_VALUE###'
group by DATE_FORMAT(EVENT_DATE, 'yyyyMM') ) ft on 1=1
WHERE result='OK' and nb_mois = 0
UNION
SELECT ft.* FROM
(select if(count(distinct event_date)=7, 'OK', 'NOK') result from  CTI.IT_IRF_USER_DATA_CUST where dayofweek('###SLICE_VALUE###')=1 and event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###') a
LEFT JOIN ( select count(*) nb_sem from AGG.FT_A_APPELS_CTI_COUNT where SEMAINE='S'||WEEKOFYEAR('###SLICE_VALUE###') and type_periode='SEMAINE' ) b ON 1=1
LEFT JOIN (
select NULL Mois, 'S'||WEEKOFYEAR(EVENT_DATE) SEMAINE, 'SEMAINE' TYPE_PERIODE, count(distinct ANI) NOMBRE_CLIENT, CURRENT_TIMESTAMP() INSERT_DATE, MAX(EVENT_DATE) EVENT_DATE
from CTI.IT_IRF_USER_DATA_CUST where dayofweek('###SLICE_VALUE###')=1 and event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###'
group by 'S'||WEEKOFYEAR(EVENT_DATE) ) ft on 1=1
WHERE result='OK' and nb_sem = 0
UNION
SELECT ft.* FROM
(select if(count(*)>${hivevar:seuil_nb_ligne}, 'OK', 'NOK') result from CTI.IT_IRF_USER_DATA_CUST where event_date = '###SLICE_VALUE###' ) a
LEFT JOIN (select count(*) nb_day from AGG.FT_A_APPELS_CTI_COUNT where event_date='###SLICE_VALUE###' and type_periode='JOUR' ) b on 1=1
LEFT JOIN (
select Null Mois, null Semaine, 'JOUR' TYPE_PERIODE, count(distinct ANI) NOMBRE_CLIENT, CURRENT_TIMESTAMP() INSERT_DATE, EVENT_DATE
from CTI.IT_IRF_USER_DATA_CUST where event_date='###SLICE_VALUE###'
group by EVENT_DATE ) ft on 1=1
WHERE result='OK' and nb_day = 0
