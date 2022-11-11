---Historisons les informations calculées
insert into MON.SPARK_FT_CXD_CEM_INDICATEURS
select a.*, "2022-11-11" EVENT_DATE from TMP.KYC_CEM_INDICATEURS_LOCALISES a;
