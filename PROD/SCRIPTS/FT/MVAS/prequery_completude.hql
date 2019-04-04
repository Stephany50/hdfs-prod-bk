-- Requête de completude plus simple lorsqu'on a pas besoin de connaitre quels sont les fichiers manquants

--select if(T1.nb < (T1.offset+1), 'NOK', 'OK')
--from (
--select count(*) nb, max(cast(substr(T.original_file_name, 19, 5) as int)) - min(cast(substr(T.original_file_name, 19, 5) as int)) offset
--from (
-- select distinct original_file_name
-- from default.received_files
-- where original_file_month between '2019-04' and '2019-04'
--  and file_type='SMSC_MVAS'
--  and original_file_date='2019-04-04'
--) T
--where substr(T.original_file_name, -5, 1)='3'
--and T.original_file_name like '%NDO%'
--) T1;



-- On reconstitue l'ensemble des fichiers qui sont censés être chargés et on vérifie les fichiers manquant dans cet ensemble
select if(NDO_1.missing_files = 0 and NDO_2.missing_files = 0 and NDO_3.missing_files = 0 and MAK_1.missing_files = 0 and MAK_2.missing_files = 0 and MAK_3.missing_files = 0, 'OK', 'NOK')
from
(
    select count(R1.original_file_name) missing_files
    from 
    (
     select concat(T1.left_side, lpad(pe.i + T1.min_val, 5, '0'), T1.right_side) original_file_name
     from 
     (
      select 
       max(cast(substr(original_file_name, 19, 5) as int)) max_val, 
       min(cast(substr(original_file_name, 19, 5) as int)) min_val, 
       max(substr(original_file_name, 1, 18)) left_side,
       max(substr(original_file_name, 24, 18)) right_side
      from default.received_files 
      where original_file_month between DATE_FORMAT(DATE_SUB('###SLICE_VALUE###',${hivevar:date_offset}), 'yyyy-MM') and DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM')
       and file_type='SMSC_MVAS' 
       and original_file_date='###SLICE_VALUE###'
       and substr(original_file_name, -5, 1)='1' 
       and original_file_name like '%NDO%' 
     ) T1 
     lateral view
     posexplode(split(space(T1.max_val - T1.min_val),' ')) pe as i,x
    ) R1 
    left join 
    (
     select original_file_name
     from default.received_files 
     where original_file_month between DATE_FORMAT(DATE_SUB('###SLICE_VALUE###',${hivevar:date_offset}), 'yyyy-MM') and DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM')
      and file_type='SMSC_MVAS' 
      and original_file_date='###SLICE_VALUE###'
      and substr(original_file_name, -5, 1)='1' 
      and original_file_name like '%NDO%' 
    ) R2 on (R1.original_file_name = R2.original_file_name)
    where R2.original_file_name is null
) NDO_1 ,
(
    select count(R1.original_file_name) missing_files
    from 
    (
     select concat(T1.left_side, lpad(pe.i + T1.min_val, 5, '0'), T1.right_side) original_file_name
     from 
     (
      select 
       max(cast(substr(original_file_name, 19, 5) as int)) max_val, 
       min(cast(substr(original_file_name, 19, 5) as int)) min_val, 
       max(substr(original_file_name, 1, 18)) left_side,
       max(substr(original_file_name, 24, 18)) right_side
      from default.received_files 
      where original_file_month between DATE_FORMAT(DATE_SUB('###SLICE_VALUE###',${hivevar:date_offset}), 'yyyy-MM') and DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM')
       and file_type='SMSC_MVAS' 
       and original_file_date='###SLICE_VALUE###'
       and substr(original_file_name, -5, 1)='2' 
       and original_file_name like '%NDO%' 
     ) T1 
     lateral view
     posexplode(split(space(T1.max_val - T1.min_val),' ')) pe as i,x
    ) R1 
    left join 
    (
     select original_file_name
     from default.received_files 
     where original_file_month between DATE_FORMAT(DATE_SUB('###SLICE_VALUE###',${hivevar:date_offset}), 'yyyy-MM') and DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM')
      and file_type='SMSC_MVAS' 
      and original_file_date='###SLICE_VALUE###'
      and substr(original_file_name, -5, 1)='2' 
      and original_file_name like '%NDO%' 
    ) R2 on (R1.original_file_name = R2.original_file_name)
    where R2.original_file_name is null
) NDO_2 ,
(
    select count(R1.original_file_name) missing_files
    from 
    (
     select concat(T1.left_side, lpad(pe.i + T1.min_val, 5, '0'), T1.right_side) original_file_name
     from 
     (
      select 
       max(cast(substr(original_file_name, 19, 5) as int)) max_val, 
       min(cast(substr(original_file_name, 19, 5) as int)) min_val, 
       max(substr(original_file_name, 1, 18)) left_side,
       max(substr(original_file_name, 24, 18)) right_side
      from default.received_files 
      where original_file_month between DATE_FORMAT(DATE_SUB('###SLICE_VALUE###',${hivevar:date_offset}), 'yyyy-MM') and DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM')
       and file_type='SMSC_MVAS' 
       and original_file_date='###SLICE_VALUE###'
       and substr(original_file_name, -5, 1)='3' 
       and original_file_name like '%NDO%' 
     ) T1 
     lateral view
     posexplode(split(space(T1.max_val - T1.min_val),' ')) pe as i,x
    ) R1 
    left join 
    (
     select original_file_name
     from default.received_files 
     where original_file_month between DATE_FORMAT(DATE_SUB('###SLICE_VALUE###',${hivevar:date_offset}), 'yyyy-MM') and DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM')
      and file_type='SMSC_MVAS' 
      and original_file_date='###SLICE_VALUE###'
      and substr(original_file_name, -5, 1)='3' 
      and original_file_name like '%NDO%' 
    ) R2 on (R1.original_file_name = R2.original_file_name)
    where R2.original_file_name is null
) NDO_3 ,
(
    select count(R1.original_file_name) missing_files
    from 
    (
     select concat(T1.left_side, lpad(pe.i + T1.min_val, 5, '0'), T1.right_side) original_file_name
     from 
     (
      select 
       max(cast(substr(original_file_name, 19, 5) as int)) max_val, 
       min(cast(substr(original_file_name, 19, 5) as int)) min_val, 
       max(substr(original_file_name, 1, 18)) left_side,
       max(substr(original_file_name, 24, 18)) right_side
      from default.received_files 
      where original_file_month between DATE_FORMAT(DATE_SUB('###SLICE_VALUE###',${hivevar:date_offset}), 'yyyy-MM') and DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM')
       and file_type='SMSC_MVAS' 
       and original_file_date='###SLICE_VALUE###'
       and substr(original_file_name, -5, 1)='1' 
       and original_file_name like '%MAK%' 
     ) T1 
     lateral view
     posexplode(split(space(T1.max_val - T1.min_val),' ')) pe as i,x
    ) R1 
    left join 
    (
     select original_file_name
     from default.received_files 
     where original_file_month between DATE_FORMAT(DATE_SUB('###SLICE_VALUE###',${hivevar:date_offset}), 'yyyy-MM') and DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM')
      and file_type='SMSC_MVAS' 
      and original_file_date='###SLICE_VALUE###'
      and substr(original_file_name, -5, 1)='1' 
      and original_file_name like '%MAK%' 
    ) R2 on (R1.original_file_name = R2.original_file_name)
    where R2.original_file_name is null
) MAK_1 ,
(
    select count(R1.original_file_name) missing_files
    from 
    (
     select concat(T1.left_side, lpad(pe.i + T1.min_val, 5, '0'), T1.right_side) original_file_name
     from 
     (
      select 
       max(cast(substr(original_file_name, 19, 5) as int)) max_val, 
       min(cast(substr(original_file_name, 19, 5) as int)) min_val, 
       max(substr(original_file_name, 1, 18)) left_side,
       max(substr(original_file_name, 24, 18)) right_side
      from default.received_files 
      where original_file_month between DATE_FORMAT(DATE_SUB('###SLICE_VALUE###',${hivevar:date_offset}), 'yyyy-MM') and DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM')
       and file_type='SMSC_MVAS' 
       and original_file_date='###SLICE_VALUE###'
       and substr(original_file_name, -5, 1)='2' 
       and original_file_name like '%MAK%' 
     ) T1 
     lateral view
     posexplode(split(space(T1.max_val - T1.min_val),' ')) pe as i,x
    ) R1 
    left join 
    (
     select original_file_name
     from default.received_files 
     where original_file_month between DATE_FORMAT(DATE_SUB('###SLICE_VALUE###',${hivevar:date_offset}), 'yyyy-MM') and DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM')
      and file_type='SMSC_MVAS' 
      and original_file_date='###SLICE_VALUE###'
      and substr(original_file_name, -5, 1)='2' 
      and original_file_name like '%MAK%' 
    ) R2 on (R1.original_file_name = R2.original_file_name)
    where R2.original_file_name is null
) MAK_2 ,
(
    select count(R1.original_file_name) missing_files
    from 
    (
     select concat(T1.left_side, lpad(pe.i + T1.min_val, 5, '0'), T1.right_side) original_file_name
     from 
     (
      select 
       max(cast(substr(original_file_name, 19, 5) as int)) max_val, 
       min(cast(substr(original_file_name, 19, 5) as int)) min_val, 
       max(substr(original_file_name, 1, 18)) left_side,
       max(substr(original_file_name, 24, 18)) right_side
      from default.received_files 
      where original_file_month between DATE_FORMAT(DATE_SUB('###SLICE_VALUE###',${hivevar:date_offset}), 'yyyy-MM') and DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM')
       and file_type='SMSC_MVAS' 
       and original_file_date='###SLICE_VALUE###'
       and substr(original_file_name, -5, 1)='3' 
       and original_file_name like '%MAK%' 
     ) T1 
     lateral view
     posexplode(split(space(T1.max_val - T1.min_val),' ')) pe as i,x
    ) R1 
    left join 
    (
     select original_file_name
     from default.received_files 
     where original_file_month between DATE_FORMAT(DATE_SUB('###SLICE_VALUE###',${hivevar:date_offset}), 'yyyy-MM') and DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM')
      and file_type='SMSC_MVAS' 
      and original_file_date='###SLICE_VALUE###'
      and substr(original_file_name, -5, 1)='3' 
      and original_file_name like '%MAK%' 
    ) R2 on (R1.original_file_name = R2.original_file_name)
    where R2.original_file_name is null
) MAK_3 
;