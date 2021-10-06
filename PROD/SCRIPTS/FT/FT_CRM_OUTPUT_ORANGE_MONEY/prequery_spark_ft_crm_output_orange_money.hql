select
if(A.nb_ft_crm = 0 and B.nb_it_crm >= 10 and C.nb_ft_om >= 10
            ,'OK','NOK')
FROM (select count(*) as nb_ft_crm from MON.spark_ft_crm_output_orange_money where event_date=to_date('###SLICE_VALUE###')) A,
(select count(*) as nb_it_crm from CDR.SPARK_IT_CRM_ABONNEMENTS where ORIGINAL_FILE_DATE=to_date('###SLICE_VALUE###')) B,
(select count(*) as nb_ft_om from MON.spark_ft_omny_account_snapshot where event_date=DATE_SUB(to_date('###SLICE_VALUE###'),1)) C