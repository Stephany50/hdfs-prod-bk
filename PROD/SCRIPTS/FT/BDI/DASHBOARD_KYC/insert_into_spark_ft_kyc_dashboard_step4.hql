--Automatisation des KPIs Telco contenus dans le fichier Excel
insert into AGG.SPARK_FT_A_KYC_DASHBOARD_KPIS_TELCO
SELECT type_personne,sheetname,key,value,current_timestamp() AS insert_date,'###SLICE_VALUE###' AS EVENT_DATE
FROM
--- KPIs DE LA FEUILLE 1
--- KPIs sur les Personnes Majeurs
((SELECT type_personne,sheetname,R.key,R.value
FROM (select trim(type_personne) type_personne,'KPI' sheetname, num_pce_absent_nb,
        num_pce_no_aut_nb, numero_piece_uniq_en_lettre_nb,
        num_pce_inf4_nb, num_piece_a_caract_non_aut_nb,
        numero_piece_egale_msisdn_nb, nom_prenom_absent_nb,
        nom_prenom_douteux_nb, scan_pce_absent_nb,
        date_expir_absent_nb, date_expir_douteux_nb,
        date_naiss_absent_nb, date_naiss_douteux_nb,
        adresse_absent_nb, adresse_douteux_nb,
        imei_absent_nb, date_activ_absent_nb,
        type_pce_id_absent_nb, multisim_nb,
        num_tel_absent_nb, cni_expir_nb,
        contrat_souscri_absent_nb, scan_fantesiste_nb,
        plan_local_absent_nb, ligne_en_anomalie_nb,
        actif_famille_nb, total_famille_nb
        from AGG.SPARK_FT_A_BDI where event_date='###SLICE_VALUE###' and trim(type_personne)='MAJEUR'
) LATERAL VIEW EXPLODE(MAP(
    '1#NUMERO_PIECE@1#ABSENT',num_pce_absent_nb,
    '1#NUMERO_PIECE@2#NON_AUTORISE',num_pce_no_aut_nb,
    '1#NUMERO_PIECE@3#UNIQUEMENT_EN_LETTRE',numero_piece_uniq_en_lettre_nb,
    '1#NUMERO_PIECE@4#INFERIEUR_A_4',num_pce_inf4_nb,
    '1#NUMERO_PIECE@5#CAR_NON_AUTO',num_piece_a_caract_non_aut_nb,
    '1#NUMERO_PIECE@6#EGALE_MSISDN',numero_piece_egale_msisdn_nb,
    '2#NOM@1#ABSENT',nom_prenom_absent_nb,
    '2#NOM@2#DOUTEUX',nom_prenom_douteux_nb,
    '3#SCAN_IDENTITE@1#ABSENT',scan_pce_absent_nb,
    '4#DATE_EXPIRATION@1#ABSENTE',date_expir_absent_nb,
    '4#DATE_EXPIRATION@2#DOUTEUSE',date_expir_douteux_nb,
    '5#DATE_NAISSANCE@1#ABSENTE',date_naiss_absent_nb,
    '5#DATE_NAISSANCE@2#DOUTEUSE',date_naiss_douteux_nb,
    '6#ADRESSE@1#ABSENTE',adresse_absent_nb,
    '6#ADRESSE@2#DOUTEUSE',adresse_douteux_nb,
    '7#CODE_IMEI@1#ABSENT',imei_absent_nb,
    '8#DATE_ACTIVATION@1#ABSENTE',date_activ_absent_nb,
    '9#TYPE_PIECE@1#ABSENT',type_pce_id_absent_nb,
    '10#MULTI_SIM@1#NOMBRE',multisim_nb,
    '11#NUMERO_TELEPHONE@1#ABSENT',num_tel_absent_nb,
    '12#CNI_EXPIREE_6MOIS@1#NOMBRE',cni_expir_nb,
    '13#CONTRAT_SOUSCRIPTION@1#ABSENT',contrat_souscri_absent_nb,
    '14#SCAN_FANTESISTE@1#NOMBRE',scan_fantesiste_nb,
    '15#PLAN_LOCALISATION@1#ABSENT',plan_local_absent_nb,
    '16#NBR_LIGNE_EN_ANOMALIE@1#NOMBRE',ligne_en_anomalie_nb,
    '17@1#TOTAL_ACTIF_FAMILLE',actif_famille_nb,
    '18@1#TOTAL_FAMILLE',total_famille_nb
)) R as key, value)
UNION
---KPIs sur les Personnes Mineurs
(SELECT type_personne,sheetname,R.key,R.value
FROM (select trim(type_personne) type_personne,'KPI' sheetname, num_pce_absent_nb, 
            num_pce_inf4_nb,num_pce_no_aut_nb,scan_pce_absent_nb,
            num_pce_tut_absent_nb,num_pce_tut_no_aut_nb,
            num_pce_tut_eg_m_nb,nom_prenom_absent_nb,
            nom_prenom_douteux_nb,date_expir_absent_nb,
            date_expir_douteux_nb,date_naiss_absent_nb,
            date_naiss_douteux_nb,multisim_nb,imei_absent_nb,
            nom_tut_absent_nb,nom_tut_douteux_nb,
            date_naiss_tut_absent_nb,date_naiss_tut_douteux_nb,
            date_activ_absent_nb,num_tel_absent_nb,
            type_pce_id_absent_nb,adresse_absent_nb,
            adresse_douteux_nb,cni_expir_nb,
            contrat_souscri_absent_nb,scan_fantesiste_nb,
            plan_local_absent_nb,ligne_en_anomalie_nb,
            actif_famille_nb,total_famille_nb
            from AGG.SPARK_FT_A_BDI where event_date= '###SLICE_VALUE###' and trim(type_personne)='MINEUR'
) LATERAL VIEW EXPLODE(MAP(
    '1#NUMERO_PIECE@1#ABSENT',num_pce_absent_nb,
    '1#NUMERO_PIECE@2#INFERIEUR_A_4',num_pce_inf4_nb,
    '1#NUMERO_PIECE@3#NON_AUTORISE',num_pce_no_aut_nb,
    '2#SCAN_IDENTITE@1#ABSENT',scan_pce_absent_nb,
    '3#NUMERO_PIECE_PARENT@1#ABSENT',num_pce_tut_absent_nb,
    '3#NUMERO_PIECE_PARENT@2#NON_AUTORISE',num_pce_tut_no_aut_nb,
    '3#NUMERO_PIECE_PARENT@3#EGALE_MSISDN',num_pce_tut_eg_m_nb,
    '4#NOM@1#ABSENT',nom_prenom_absent_nb,
    '4#NOM@2#DOUTEUX',nom_prenom_douteux_nb,
    '5#DATE_EXPIRATION@1#ABSENTE',date_expir_absent_nb,
    '5#DATE_EXPIRATION@2#DOUTEUSE',date_expir_douteux_nb,
    '6#DATE_NAISSANCE@1#ABSENTE',date_naiss_absent_nb,
    '6#DATE_NAISSANCE@2#DOUTEUSE',date_naiss_douteux_nb,
    '7#MULTI_SIM@1#NOMBRE',multisim_nb,
    '8#CODE_IMEI@1#ABSENT',imei_absent_nb,
    '9#NOM_TUTEUR@1#ABSENT',nom_tut_absent_nb,
    '9#NOM_TUTEUR@2#DOUTEUX',nom_tut_douteux_nb,
    '10#DATE_NAISS_PARENT@1#ABSENTE',date_naiss_tut_absent_nb,
    '10#DATE_NAISS_PARENT@2#DOUTEUSE',date_naiss_tut_douteux_nb,
    '11#DATE_ACTIVATION@1#ABSENTE',date_activ_absent_nb,
    '12#NUMERO_TELEPHONE@1#ABSENT',num_tel_absent_nb,
    '13#TYPE_PIECE@1#ABSENT',type_pce_id_absent_nb,
    '14#ADRESSE@1#ABSENTE',adresse_absent_nb,
    '14#ADRESSE@2#DOUTEUSE',adresse_douteux_nb,   
    '15#CNI_EXPIREE_6MOIS@1#NOMBRE',cni_expir_nb,
    '16#CONTRAT_SOUSCRIPTION@1#ABSENT',contrat_souscri_absent_nb,
    '17#SCAN_FANTESISTE@1#NOMBRE',scan_fantesiste_nb,
    '18#PLAN_LOCALISATION@1#ABSENT',plan_local_absent_nb,
    '19#NBR_LIGNE_EN_ANOMALIE@1#NOMBRE',ligne_en_anomalie_nb,
    '20@1#TOTAL_ACTIF_FAMILLE',actif_famille_nb,
    '21@1#TOTAL_FAMILLE',total_famille_nb
)) R as key, value)
UNION
---KPI sur les personnes Morales M2M
(SELECT type_personne,sheetname,R.key,R.value
 FROM (
    SELECT trim(type_personne) type_personne,'KPI' sheetname, NOM_STRUCTURE_AN,NUM_RCCM_AN,NUM_RPSTANT_LEGAL_AN,
    DATE_SOUSCRIPTION_AN,ADRESSE_STRUCTURE_AN,NUM_TELEPHONE_AN,NOM_PRENOM_AN,
    NUMERO_PIECE_AN,IMEI_AN,ADRESSE_AN,STATUT_AN,NB_LIGNE_EN_ANOMALIE,NB_ACTIFS,NB_TOTAL
    FROM AGG.SPARK_FT_A_BDI_B2B where event_date= '###SLICE_VALUE###' and 
    trim(type_personne) in ('M2M','FLOTTE')
 ) LATERAL VIEW EXPLODE(MAP(
     '1#NOM_STRUCTURE@1#ABSENT/DOUTEUX',NOM_STRUCTURE_AN,
     '2#NUM_REGISTRE_COM@1#ABSENT/DOUTEUX',NUM_RCCM_AN,
     '3#NUM_PIECE_RPSTANT_LEGAL@1#ABSENT/DOUTEUX',NUM_RPSTANT_LEGAL_AN,
     '4#DATE_SOUSCRIPTION@1#ABSENT/DOUTEUX',DATE_SOUSCRIPTION_AN,
     '5#ADRESSE_STRUCTURE@1#ABSENT/DOUTEUX',ADRESSE_STRUCTURE_AN,
     '6#NUM_TELEPHONE@1#ABSENT/DOUTEUX',NUM_TELEPHONE_AN,
     '7#NOM_PRENOM@1#ABSENT/DOUTEUX',NOM_PRENOM_AN,
     '8#NUMERO_PIECE@1#ABSENT/DOUTEUX',NUMERO_PIECE_AN,
     '9#IMEI@1#ABSENT/DOUTEUX',IMEI_AN,
     '10#ADRESSE@1#ABSENT/DOUTEUX',ADRESSE_AN,
     '11#STATUT@1#ABSENT/DOUTEUX',STATUT_AN,
     '12@1#NBR_LIGNE_EN_ANOMALIE',NB_LIGNE_EN_ANOMALIE,
     '13@1#TOTAL_ACTIF_FAMILLE',NB_ACTIFS,
     '14@1#TOTAL_FAMILLE',NB_TOTAL)) R as key,value)
UNION
--- Les KPIs dans la feuille RECAP : PERSONNES PHYSIQUES
(
    SELECT TYPE_PERSONNE,SHEETNAME,R.key,R.value
    FROM (
        SELECT trim(type_personne) TYPE_PERSONNE,'RECAP' SHEETNAME,
        ligne_en_anomalie_nb TOTAL_ANOMALIE,actif_famille_nb TOTAL_ACTIF_FAMILLE,
        (actif_famille_nb - ligne_en_anomalie_nb) TOTAL_CONFORME,
        (total_famille_nb - actif_famille_nb) LIGNES_SUSPENDUES, total_famille_nb TOTAL_FAMILLE
        FROM AGG.SPARK_FT_A_BDI WHERE event_date='###SLICE_VALUE###' AND trim(type_personne) in ('MAJEUR','MINEUR')
    )LATERAL VIEW EXPLODE(MAP(
        '1#NBR_LIGNES NON-CONFORMES',TOTAL_ANOMALIE,
        '2#NBR_LIGNES CONFORMES',TOTAL_CONFORME,
        '3#NBR_LIGNES SUSPENDUES',LIGNES_SUSPENDUES,
        '4#TOTAL ACTIFS',TOTAL_ACTIF_FAMILLE,
        '5#TOTAL',TOTAL_FAMILLE
    ))R as key,value
)
UNION
--- Les KPIs dans la feuille RECAP : PERSONNES MORALES
(
    SELECT TYPE_PERSONNE,SHEETNAME,R.key,R.value
    FROM (
        SELECT trim(type_personne) TYPE_PERSONNE,'RECAP' SHEETNAME,nb_ligne_en_anomalie TOTAL_ANOMALIE,
        (nb_actifs-nb_ligne_en_anomalie) TOTAL_CONFORME,nb_actifs TOTAL_ACTIF_FAMILLE,
        (nb_total-nb_actifs) LIGNES_SUSPENDUES,nb_total TOTAL_FAMILLE
        FROM AGG.SPARK_FT_A_BDI_B2B WHERE event_date='###SLICE_VALUE###' AND trim(type_personne) in ('M2M','FLOTTE')
    )LATERAL VIEW EXPLODE(MAP(
        '1#NBR_LIGNES NON-CONFORMES',TOTAL_ANOMALIE,
        '2#NBR_LIGNES CONFORMES',TOTAL_CONFORME,
        '3#NBR_LIGNES SUSPENDUES',LIGNES_SUSPENDUES,
        '4#TOTAL ACTIFS',TOTAL_ACTIF_FAMILLE,
        '5#TOTAL',TOTAL_FAMILLE
    ))R as key,value
)
UNION
--- Les KPIs dans la feuille Mini RECAP : PERSONNES PHYSIQUES
(
    SELECT '' TYPE_PERSONNE,SHEETNAME,R.key,R.value
    FROM (
        SELECT 'Mini-RECAP' SHEETNAME,
        sum(actif_famille_nb) TOTAL_ACTIF_FAMILLE,
        (sum(actif_famille_nb) - sum(ligne_en_anomalie_nb)) TOTAL_CONFORME,
        sum(ligne_en_anomalie_nb) TOTAL_ANOMALIE,
        ((sum(actif_famille_nb) - sum(ligne_en_anomalie_nb))/sum(actif_famille_nb)) TAUX_CONFORMITE
        FROM AGG.SPARK_FT_A_BDI WHERE event_date='###SLICE_VALUE###' AND trim(type_personne) in ('MAJEUR','MINEUR')
    )LATERAL VIEW EXPLODE(MAP(
        '1#TOTAL BASE ACTIFS B2C',TOTAL_ACTIF_FAMILLE,
        '2#NBR_LIGNES CONFORMES',TOTAL_CONFORME,
        '3#NBR_LIGNES NON-CONFORMES',TOTAL_ANOMALIE,
        '5#TAUX DE CONFORMITE',TAUX_CONFORMITE
    ))R as key,value
)
UNION
--- Les KPIs dans la feuille Mini RECAP : PERSONNES MORALES
(
    SELECT '' TYPE_PERSONNE,SHEETNAME,R.key,R.value
    FROM (
        SELECT 'Mini-RECAP' SHEETNAME,sum(nb_actifs) TOTAL_ACTIF_FAMILLE,
        (sum(nb_actifs)-sum(nb_ligne_en_anomalie)) TOTAL_CONFORME,
        sum(nb_ligne_en_anomalie) TOTAL_ANOMALIE,
        ((sum(nb_actifs)-sum(nb_ligne_en_anomalie))/sum(nb_actifs)) TAUX_CONFORMITE
        FROM AGG.SPARK_FT_A_BDI_B2B WHERE event_date='###SLICE_VALUE###' AND trim(type_personne) in ('M2M','FLOTTE')
    )LATERAL VIEW EXPLODE(MAP(
        '1#TOTAL BASE ACTIFS B2B',TOTAL_ACTIF_FAMILLE,
        '2#NBR_LIGNES CONFORMES',TOTAL_CONFORME,
        '3#NBR_LIGNES NON-CONFORMES',TOTAL_ANOMALIE,
        '5#TAUX DE CONFORMITE',TAUX_CONFORMITE
    ))R as key,value
))