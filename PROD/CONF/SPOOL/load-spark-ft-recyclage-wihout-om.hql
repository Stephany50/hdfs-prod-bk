flux.yarn.queue = "compute"
flux.log-level = "ERROR"

flux.input-type = "HIVE"
flux.output-type = "HIVE"

flux.spark.setup-conf = []

flux.setup-conf = []

flux.spark.setup-conf += {"key": "spark.sql.crossJoin.enabled", "value": "true"}
flux.spark.setup-conf += {"key": "hive.exec.dynamic.partition.mode","value": "nonstrict"}
flux.spark.setup-conf += {"key": "spark.sql.files.ignoreCorruptFiles","value": "true"}

flux.name = "LOAD_SPARK_SPOOL_RECYCLAGE_SANS_OM"

flux.has-date-processing = true

flux.slice-value-type = "DAILY"
flux.slice-begin-value = -15
flux.slice-end-value = -1
flux.slice-step-value = 1
flux.slice-begin-value-offset = 0
flux.slice-end-value-offset = 0
flux.slice-has-state-query=true
flux.slice-state-query="""
select
    case when count(*) = 15 then 'OK' else 'NOK' end
from
(
    select
        status,
        row_number() over(partition by event_date order by insert_date desc) rang
    from mon.export_history
    where job_instanceid = '${hivevar:job_instanceid}'
    and event_date between date_sub(current_date, 15) and date_sub(current_date, 1)
) R
where status = 'OK' and rang = 1
"""
flux.slice-has-filter-query = true
flux.slice-filter-query = """
select
    date_format(datecode,'yyyy-MM-dd')
from
dim.dt_dates
where datecode between date_sub(current_date, 15) and date_sub(current_date, 1)
and datecode in
(
    select event_date
    from
    (
        select
            *,
            row_number() over(partition by event_date order by insert_date desc) rang
        from mon.export_history
        where job_instanceid = '${hivevar:job_instanceid}'
        and event_date between date_sub(current_date, 15) and date_sub(current_date, 1)
    ) T
    where status = 'OK' and rang = 1
)
"""
flux.slice-date-format = "yyyy-MM-dd"

flux.has-pre-queries = true
flux.has-exec-queries = true
flux.has-post-queries = true

flux.pre-exec-queries += "/PROD/SCRIPTS/SPOOL/RECYCLAGE/prequery_spool_recylage_without_om.hql"

flux.exec-queries += "/PROD/SCRIPTS/SPOOL/RECYCLAGE/compute_and_insert_spool_recyclage_without_om.hql"

flux.inline.post-exec-queries += "INSERT INTO MON.EXPORT_HISTORY VALUES ('${hivevar:job_instanceid}', 'OK', CURRENT_TIMESTAMP, '###SLICE_VALUE###')"


flux.hdfs.output-format = "csv"
flux.hdfs.output-has-header = "true"
flux.hdfs.output-separator = ";"
flux.hdfs.output-mode = "overwrite"
flux.hdfs.output-path = "/PROD/EXPORT/EXPORT_SPOOL_RECYCLAGE_SANS_OM"
