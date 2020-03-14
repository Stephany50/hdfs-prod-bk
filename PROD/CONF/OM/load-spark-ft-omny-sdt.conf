flux.yarn.queue = "PROD"
flux.log-level = "ERROR"

flux.input-type = "HIVE"
flux.output-type = "HIVE"

flux.spark.setup-conf = []

flux.setup-conf = []

flux.spark.setup-conf += {"key": "spark.sql.crossJoin.enabled", "value": "true"}
flux.spark.setup-conf += {"key": "hive.exec.dynamic.partition.mode","value": "nonstrict"}

flux.name = "LOAD_SPARK_FT_OMNY_SDT"

flux.has-date-processing = true

flux.slice-value-type = "DAILY"
flux.slice-begin-value = -14
flux.slice-end-value = -1
flux.slice-step-value = 1
flux.slice-begin-value-offset = 0
flux.slice-end-value-offset = 0
flux.slice-date-format = "yyyy-MM-dd"

flux.has-pre-queries = true
flux.has-exec-queries = true
flux.has-post-queries = false

flux.pre-exec-queries += "/PROD/SCRIPTS/FT/OM/FT_OMNY_SDT/prequery_spark_ft_omny_sdt.hql"

flux.inline.exec-queries += "TRUNCATE TABLE TMP.TT_OMNY_SDT_1"
flux.inline.exec-queries += "TRUNCATE TABLE TMP.TT_OMNY_SDT_2"
flux.inline.exec-queries += "TRUNCATE TABLE TMP.TT_OMNY_SDT_3"
flux.inline.exec-queries += "TRUNCATE TABLE TMP.TT_OMNY_SDT_4"

flux.exec-queries += "/PROD/SCRIPTS/FT/OM/FT_OMNY_SDT/compute_and_insert_into_spark_tt_omny_sdt_1.hql"
flux.exec-queries += "/PROD/SCRIPTS/FT/OM/FT_OMNY_SDT/compute_and_insert_into_spark_tt_omny_sdt_2.hql"
flux.exec-queries += "/PROD/SCRIPTS/FT/OM/FT_OMNY_SDT/compute_and_insert_into_spark_tt_omny_sdt_3.hql"
flux.exec-queries += "/PROD/SCRIPTS/FT/OM/FT_OMNY_SDT/compute_and_insert_into_spark_tt_omny_sdt_4.hql"

flux.exec-queries += "/PROD/SCRIPTS/FT/OM/FT_OMNY_SDT/compute_and_insert_into_spark_ft_omny_sdt.hql"
