
insert into cdr.it_log partition (LOG_DATE)
select
  filename,
  merged_filename,
  fluxtype,
  provenance,
  status,
  from_unixtime(cast(log_datetime/1000 as bigint)) log_datetime,
  regexp_replace(regexp_replace(flowfile_attr, "\\.", "_"), "%7C", "|") flowfile_attr,
  current_timestamp() insert_date,
  to_date(from_unixtime(cast(log_datetime/1000 as bigint))) log_date
from cdr.tt_log
group by 
  filename,
  merged_filename,
  fluxtype,
  provenance,
  status,
  log_datetime,
  flowfile_attr

