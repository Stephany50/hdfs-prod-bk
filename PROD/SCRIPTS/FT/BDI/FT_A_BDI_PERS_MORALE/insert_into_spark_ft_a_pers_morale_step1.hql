INSERT INTO TMP.TT_KYC_FT_A_PERS_MO_ST1
select b.* from
(select distinct cust_guid from MON.SPARK_FT_KYC_BDI_FLOTTE where event_date='###SLICE_VALUE###') a
left join (select * from MON.SPARK_FT_KYC_CRM_B2B where event_date='###SLICE_VALUE###') b on trim(a.cust_guid) = trim(b.guid)
where b.guid is not null