flux.yarn.queue = "compute"
flux.log-level = "ERROR"

flux.input-type = "HIVE"
flux.output-type = "HIVE"
flux.spark.setup-conf += {"key": "hive.exec.dynamic.partition.mode","value": "nonstrict"}
flux.spark.setup-conf += {"key": "spark.sql.crossJoin.enabled", "value": "true"}
flux.spark.setup-conf += {"key": "hive.enforce.bucketing", "value": "false"}
flux.spark.setup-conf += {"key": "spark.debug.maxToStringFields", "value": "100"}

flux.name = "LOAD_SPARK_TT_PICO_TRAFFIC_TELCO"

flux.has-date-processing = true

flux.slice-value-type = "MONTHLY"
flux.slice-begin-value = -6
flux.slice-end-value = -1
flux.slice-step-value = 1
flux.slice-begin-value-offset = 0
flux.slice-end-value-offset = 0
flux.slice-date-format = "yyyy-MM"

flux.has-pre-queries = true
flux.has-exec-queries = true
flux.has-post-queries = false

flux.pre-exec-queries += "/PROD/SCRIPTS/FT/PICO_CREDIT/prequery_spark_tt_pico_traffic_telco.hql"

flux.exec-queries += "/PROD/SCRIPTS/FT/PICO_CREDIT/compute_and_insert_into_spark_tt_pico_traffic_telco.hql"
