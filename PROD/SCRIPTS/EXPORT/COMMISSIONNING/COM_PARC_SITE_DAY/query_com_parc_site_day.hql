SELECT 
nvl(to_date(EVENT_DATE),'') EVENT_DATE,
nvl(PARC_TYPE,'') PARC_TYPE,
nvl(PROFILE,'') PROFILE,
nvl(STATUT,'') STATUT,
nvl(EFFECTIF,'') EFFECTIF,
nvl(SITE_NAME,'') SITE_NAME,
nvl(CONTRACT_TYPE,'') CONTRACT_TYPE,
nvl(OPERATOR_CODE,'')  OPERATOR_CODE
FROM MON.SPARK_FT_PARCS_SITE_DAY
where event_date = "###SLICE_VALUE###"