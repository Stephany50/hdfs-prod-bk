insert into TMP.TT_MIN_AMELIORE
SELECT
SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.NUMERO_PIECE_ABSENT = 'OUI' then 1 else 0 end) NB_NUMERO_PIECE_ABSENT
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.NUMERO_PIECE_INF_4 = 'OUI' then 1 else 0 end) NB_NUMERO_PIECE_INF_4
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.NUMERO_PIECE_NON_AUTHORISE = 'OUI' then 1 else 0 end)
NB_NUMERO_PIECE_NON_AUTHORISE
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND (A.DISPONIBILITE_SCAN is null OR A.DISPONIBILITE_SCAN = '')  then 1 else 0 end) NB_SCAN_INDISPONIBLE
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.NUMERO_PIECE_TUT_ABSENT = 'OUI' then 1 else 0 end)
NB_NUMERO_PIECE_TUT_ABSENT
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.NUMERO_PIECE_TUT_NON_AUTH = 'OUI' then 1 else 0 end)
NB_NUMERO_PIECE_TUT_NON_AUTH
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.NUMERO_PIECE_TUT_EGALE_MSISDN = 'OUI' then 1 else 0 end)
NB_NUM_PIECE_TUT_EGALE_MSISDN
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.NOM_PRENOM_ABSENT = 'OUI' then 1 else 0 end) NB_NOM_PRENOM_ABSENT
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.NOM_PRENOM_DOUTEUX = 'OUI' then 1 else 0 end) NOM_PRENOM_DOUTEUX
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.DATE_EXPIRATION_ABSENT = 'OUI' then 1 else 0 end)
NB_DATE_EXPIRATION_ABSENT
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.DATE_EXPIRATION_DOUTEUSE = 'OUI' then 1 else 0 end)
NB_DATE_EXPIRATION_DOUTEUSE
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.DATE_NAISSANCE_ABSENT = 'OUI' then 1 else 0 end)
NB_DATE_NAISSANCE_ABSENT
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.DATE_NAISSANCE_DOUTEUX = 'OUI' then 1 else 0 end)
NB_DATE_NAISSANCE_DOUTEUX
,SUM(case when not(B.msisdn is null or trim(B.msisdn) = '') AND A.est_suspendu = 'NON' then 1 else 0 end) NB_MULTI_SIM
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.IMEI_ABSENT = 'OUI' THEN 1 ELSE 0 END) NB_IMEI_ABSENT
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.NOM_PARENT_ABSENT ='OUI' then 1 else 0 end) NB_NOM_PARENT_ABSENT
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.NOM_PARENT_DOUTEUX='OUI' then 1 else 0 end) NB_NOM_PARENT_DOUTEUX
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.DATE_NAISSANCE_TUT_ABSENT='OUI' then 1 else 0 end)
NB_DATE_NAISSANCE_TUT_ABSENT
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.DATE_NAISSANCE_TUT_DOUTEUX ='OUI' then 1 else 0 end)
NB_DATE_NAISSANCE_TUT_DOUTEUX
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.DATE_ACTIVATION  IS NULL   THEN 1 ELSE 0 END) NB_DATE_ACTIVATION_ABSENTE
,sum(case when (A.msisdn is null or trim(A.msisdn) = '') AND
               A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' then 1 ELSE 0 END) NB_MSISDN_ABSENT
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND (A.TYPE_PIECE IS NULL OR A.TYPE_PIECE = '') THEN 1 ELSE 0 END) NB_TYPE_PIECE_IDENTITE_ABSENTE
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.ADRESSE_ABSENT = 'OUI' then 1 else 0 end) NB_ADRESSE_ABSENT
,SUM (case when not(A.msisdn is null or trim(A.msisdn) = '') AND
                A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.ADRESSE_DOUTEUSE = 'OUI' then 1 else 0 end)
NB_ADDRESSE_DOUTEUSE
,sum(case when not(A.msisdn is null or trim(A.msisdn) = '') AND
               A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND A.CNI_EXPIRE = 'OUI' then 1 else 0 end) NB_CNI_EXPIRE
,sum(case when not(A.msisdn is null or trim(A.msisdn) = '') AND
               A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND (A.CONTRAT_SOUCRIPTION is null OR A.CONTRAT_SOUCRIPTION  = '') then 1 ELSE 0 END)
NB_CONTRAT_SOUCRIPTION_ABSENT
,SUM(case when not(C.msisdn is null or trim(C.msisdn) = '') AND A.est_suspendu = 'NON'  then 1 else 0 end) AS SCAN_FANTAISISTE
,sum(case when not(A.msisdn is null or trim(A.msisdn) = '') AND
               A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' AND (A.PLAN_LOCALISATION is null OR A.PLAN_LOCALISATION = '') then 1 ELSE 0 END) NB_PLAN_LOCALISATION_ABSENT
,sum(CASE WHEN not(A.msisdn is null or trim(A.msisdn) = '') AND
               A.est_suspendu = 'NON' AND A.CONFORME_ART = 'NON' THEN 1 ELSE 0 END) NB_LIGNE_EN_ANOMALIE
,sum(case when not(A.msisdn is null or trim(A.msisdn) = '') and A.est_suspendu = 'NON' then 1 else 0 end) AS NB_ACTIFS
,sum(case when not(A.msisdn is null or trim(A.msisdn) = '')   then 1 else 0 end) AS NB_FAMILLE
FROM (
   select *
   from MON.SPARK_FT_BDI_AMELIORE
where type_personne = 'MINEUR'
and event_date = to_date('###SLICE_VALUE###')
) A
left join (
    select distinct msisdn
    from TMP.TT_MULTISIMS_AMELIORE
) B ON trim(A.msisdn) = trim(B.msisdn)
left join (
    select distinct msisdn
    from TMP.TT_SCANS_FANTAISISTES_AMELIORE
) C ON trim(A.msisdn) = trim(C.msisdn)