INSERT INTO TMP.TT_KYC_FT_A_PERS_MO_DETAIL_ST2
select b.* from
(select distinct cust_guid from MON.SPARK_FT_KYC_BDI_FLOTTE where event_date=DATE_SUB('###SLICE_VALUE###',1)) a
left join (select * from MON.SPARK_FT_KYC_CRM_B2B where event_date=DATE_SUB('###SLICE_VALUE###',1)) b on trim(a.cust_guid) = trim(b.guid)
where b.guid is not null


