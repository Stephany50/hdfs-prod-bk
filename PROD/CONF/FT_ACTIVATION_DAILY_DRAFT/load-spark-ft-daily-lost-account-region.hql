flux.yarn.queue = "compute"
flux.log-level = "ERROR"

flux.input-type = "HIVE"
flux.output-type = "HIVE"

flux.spark.setup-conf += {"key": "hive.exec.dynamic.partition.mode","value": "nonstrict"}
flux.spark.setup-conf += {"key": "spark.sql.crossJoin.enabled", "value": "true"}

flux.spark.setup-var += {"key": "date_offset", "value": "32"}
flux.spark.setup-var += {"key": "table_type", "value": "FT"}
flux.spark.setup-var += {"key": "table_name", "value": "SPARK_FT_DAILY_LOST_ACCOUNT_REGION"}
flux.spark.setup-var += {"key": "database_table_name", "value": "MON.SPARK_FT_DAILY_LOST_ACCOUNT_REGION"}
flux.spark.setup-var += {"key": "table_partition", "value": "EVENT_DATE"}

flux.name = "LOAD_SPARK_FT_DAILY_LOST_ACCOUNT_REGION"

flux.has-date-processing = true

flux.slice-value-type = "DAILY"
flux.slice-begin-value = -7
flux.slice-end-value = 0
flux.slice-step-value = 1
flux.slice-begin-value-offset = 0
flux.slice-end-value-offset = 0
flux.slice-has-state-query=true
flux.slice-state-query="""
select  if(count(*)=8,'OK','NOK')

from dim.dt_dates
where datecode between date_sub(current_date, 7) and date_sub(current_date, 0)
    and datecode in (select distinct event_date from MON.SPARK_FT_DAILY_LOST_ACCOUNT_REGION where event_date between date_sub(current_date, 7) and date_sub(current_date, 0))
"""
flux.slice-has-filter-query = true
flux.slice-filter-query = """
select
    date_format(datecode,'yyyy-MM-dd')
from dim.dt_dates
where datecode between date_sub(current_date, 7) and date_sub(current_date, 0)
    and datecode in (select distinct event_date from MON.SPARK_FT_DAILY_LOST_ACCOUNT_REGION where event_date between date_sub(current_date, 7) and date_sub(current_date, 0))
"""
flux.slice-date-format = "yyyy-MM-dd"

flux.has-pre-queries = true
flux.has-exec-queries = true
flux.has-post-queries = false

flux.pre-exec-queries += "/PROD/SCRIPTS/FT/FT_ACTIVATION_DAILY_DRAFT/FT_DAILY_LOST_ACCOUNT_REGION/prequery_spark_ft_daily_lost_account_region.hql"


flux.exec-queries += "/PROD/SCRIPTS/FT/FT_ACTIVATION_DAILY_DRAFT/FT_DAILY_LOST_ACCOUNT_REGION/insert_into_spark_ft_daily_lost_account_region.hql"

