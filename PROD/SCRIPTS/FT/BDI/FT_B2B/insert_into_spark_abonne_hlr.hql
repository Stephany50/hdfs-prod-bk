insert into MON.SPARK_ABONNE_HLR
select msisdn,odbic,odboc,
case when trim(ODBOC) is null OR trim(ODBIC)  is null then 'U'
    WHEN trim(ODBOC) = '1'
    then case when trim(ODBIC) = '1' then  'SUSPENDU'
              else 'SUSPENDU_SORTANT'
          end
    WHEN trim(ODBIC) = '1'
    then case when trim(ODBOC) <> '1' then  'SUSPENDU_ENTRANT'
          end
    else 'ACTIF'
end as statut,
current_timestamp() as insert_date,
date_add('###SLICE_VALUE###',1) as event_date
from (
select
msisdn,
odbic,
odboc,
row_number() over(partition by msisdn order by odboc) as rang
from cdr.spark_it_bdi_hlr
where original_file_date=date_add('###SLICE_VALUE###',1)
and trim(msisdn) rlike '^\\d+$'
and length(substr(trim(msisdn),-9,9))=9) a
where rang = 1