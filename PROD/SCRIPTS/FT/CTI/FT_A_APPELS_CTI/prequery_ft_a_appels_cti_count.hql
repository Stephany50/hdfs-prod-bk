select IF(nb_it>29000 and (nb_day=0 or nb_sem=0 or nb_mois=0), 'OK', 'NOK') val
from (select count(*) nb_it from CTI.IT_IRF_USER_DATA_CUST where event_date = '###SLICE_VALUE###') it,
(select count(*) nb_day from AGG.FT_A_APPELS_CTI_COUNT where event_date='###SLICE_VALUE###' and type_periode='JOUR' ) a,
(select count(*) nb_mois from AGG.FT_A_APPELS_CTI_COUNT where mois=DATE_FORMAT('###SLICE_VALUE###', 'yyyyMM') and type_periode='MOIS' ) b,
(select count(*) nb_sem from AGG.FT_A_APPELS_CTI_COUNT where SEMAINE='S'||WEEKOFYEAR('###SLICE_VALUE###') and type_periode='SEMAINE' ) c