insert into TMP.tt_gap_bdi_hlr
select distinct U.msisdn from
(select distinct msisdn from
(select msisdn,odbic,odboc,
(case when trim(ODBOC) is null or trim(ODBIC) is null then 'U'
WHEN trim(ODBOC) = '1'
then case when trim(ODBIC) = '1' then  'SUSPENDU'
else 'SUSPENDU_SORTANT'
end
WHEN trim(ODBIC) = '1'
then case when trim(ODBOC) <> '1' then  'SUSPENDU_ENTRANT'
end
else 'ACTIF'
end )  as statut
from (
select
msisdn,
odbic,
odboc,
row_number() over(partition by msisdn order by odboc) as rang
from cdr.spark_it_bdi_hlr
where original_file_date='###SLICE_VALUE###'
and trim(msisdn) rlike '^\\d+$'
and length(substr(trim(msisdn),-9,9))=9) a
where rang = 1) xx where trim(statut) in ('ACTIF','SUSPENDU_ENTRANT','SUSPENDU_SORTANT')
) U
left join
(select * from
(select distinct msisdn
from cdr.SPARK_IT_BDI_FULL1
where original_file_date=date_sub('###SLICE_VALUE###',1)) A
union
select * from
(select distinct msisdn
from cdr.spark_it_bdi_crm_b2c
where original_file_date='###SLICE_VALUE###') B) V
on  substr(upper(trim(U.msisdn)),-9,9) = substr(upper(trim(V.msisdn)),-9,9)
where V.msisdn is null