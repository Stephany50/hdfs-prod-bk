select if(agg_profile=0 and cdr_profile>0,'OK','NOK') FROM
(select count(*) agg_profile from AGG.SPARK_ZTE_PROFILE where MAX_ORIGINAL_FILE_DATE=date_sub('###SLICE_VAULE###',1)) a,
(select count(*) cdr_profile from CDR.SPARK_IT_ZTE_PROFILE where ORIGINAL_FILE_DATE ='###SLICE_VAULE###' ) b