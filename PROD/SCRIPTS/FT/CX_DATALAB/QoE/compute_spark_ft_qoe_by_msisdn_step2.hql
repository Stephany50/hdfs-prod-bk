---Historisons les informations calculées
---"2022-11-14" : Les données QoE de la semaine S44
---"2022-11-13" : Les données QoE de la semaine S44 pour NOSO
---"2022-11-17" : Dernier calcul de la QoE
---"2022-11-16" : Données Charly

set hive.exec.dynamic.partition.mode=nonstrict;
insert into MON.SPARK_FT_CXD_CEM_INDICATEURS
select a.*, "2022-11-16" EVENT_DATE from TMP.KYC_CEM_INDICATEURS_LOCALISES a;
