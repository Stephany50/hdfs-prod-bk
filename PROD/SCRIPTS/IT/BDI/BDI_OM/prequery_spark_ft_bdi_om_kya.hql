SELECT IF(A.nb_A = 0 and A1.nb_A1 > 0  and B.nb_B > 0 and C.nb_C > 0 AND D.nb_D > 0 AND  E.nb_E > 0 AND F.nb_F >0
AND H.nb_H > 0 and ft_dmomm.nb_ft_dmomm >= 10
,'OK','NOK') FROM
(SELECT COUNT(*) nb_A FROM MON.SPARK_FT_BDI_OM_KYA WHERE event_date='###SLICE_VALUE###') A,
(SELECT COUNT(*) nb_A1 FROM MON.SPARK_FT_BDI_OM_KYA WHERE event_date=date_sub('###SLICE_VALUE###',1)) A1,
(SELECT count(*) nb_B FROM CDR.SPARK_IT_KYC_BDI_FULL WHERE original_file_date=DATE_ADD('###SLICE_VALUE###',1)) B,
(SELECT COUNT(*) nb_C FROM MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT_NEW WHERE event_date= '###SLICE_VALUE###' )C,
(SELECT count(*) nb_D FROM CDR.SPARK_IT_CRM_CONTACT_BASE WHERE original_file_date='###SLICE_VALUE###') D,
(SELECT COUNT(*) nb_E FROM CDR.SPARK_IT_CRM_PARTENAIRE_BASE WHERE original_file_date='###SLICE_VALUE###') E,
(SELECT COUNT(*) nb_F FROM CDR.SPARK_IT_CRM_MANDATAIRE_BASE WHERE original_file_date='###SLICE_VALUE###')F,
(SELECT COUNT(*) nb_H FROM cdr.spark_it_kaabu_client_directory WHERE date_creation='###SLICE_VALUE###')H,
(select count(*) as nb_ft_dmomm from  MON.SPARK_FT_DATAMART_OM_MONTH where mois = substr(add_months(to_date('###SLICE_VALUE###'),-1),1,7)) ft_dmomm

