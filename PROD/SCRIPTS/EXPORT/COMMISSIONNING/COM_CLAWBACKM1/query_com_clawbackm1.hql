SELECT 
nvl(identifier_msisdn,'') identifier_msisdn,
nvl(vol_paye,'') vol_paye,
nvl(regul_enceigne,'') regul_enceigne,
nvl(inf250,'') inf250,
nvl(inf300,'') inf300,
nvl(inf400,'') inf400,
nvl(inf450,'') inf450,
nvl(to_date(insert_date),'') insert_date,
nvl(to_date(event_date),'') event_date
FROM MON.SPARK_FT_CLAWBACKM1
WHERE EVENT_DATE='###SLICE_VALUE###'