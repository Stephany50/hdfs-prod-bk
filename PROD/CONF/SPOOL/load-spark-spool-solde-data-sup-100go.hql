flux.yarn.queue = "compute"
flux.log-level = "ERROR"

flux.input-type = "HIVE"
flux.output-type = "HIVE"

flux.spark.setup-conf = []

flux.setup-conf = []

flux.spark.setup-conf += {"key": "spark.sql.crossJoin.enabled", "value": "true"}
flux.spark.setup-conf += {"key": "hive.exec.dynamic.partition.mode","value": "nonstrict"}

flux.name = "LOAD_SPARK_SPOOL_SOLDE_DATA_SUP_100GO"

flux.has-date-processing = true

flux.slice-value-type = "DAILY"
flux.slice-begin-value = -7
flux.slice-end-value = -1
flux.slice-step-value = 1
flux.slice-begin-value-offset = 0
flux.slice-end-value-offset = 0
flux.slice-has-state-query=true
flux.slice-state-query="""
select  if(count(*)=7,'OK','NOK')

from dim.dt_dates
where datecode between date_sub(current_date, 7) and date_sub(current_date, 1)
    and datecode in (select distinct original_file_date from SPOOL.SPOOL_SOLDE_DATA_SUP_100GO where original_file_date between date_sub(current_date, 7) and date_sub(current_date, 1))
"""
flux.slice-has-filter-query = true
flux.slice-filter-query = """
select
    date_format(datecode,'yyyy-MM-dd')
from dim.dt_dates
where datecode between date_sub(current_date, 7) and date_sub(current_date, 1)
    and datecode in (select distinct original_file_date from SPOOL.SPOOL_SOLDE_DATA_SUP_100GO where original_file_date between date_sub(current_date, 7) and date_sub(current_date, 1))
"""
flux.slice-date-format = "yyyy-MM-dd"

flux.has-pre-queries = true
flux.has-exec-queries = true
flux.has-post-queries = false

flux.pre-exec-queries += "/PROD/SCRIPTS/SPOOL/SPOOL_SOLDE_DATA_SUP_100GO/prequery_spool_solde_data_sup_100go.hql"

flux.exec-queries += "/PROD/SCRIPTS/SPOOL/SPOOL_SOLDE_DATA_SUP_100GO/compute_and_insert_into_spool_solde_data_sup_100go.hql"