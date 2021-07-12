insert into TMP.TT_BDI_SF
select
a.MSISDN,
a.TYPE_PIECE,
a.NUMERO_PIECE,
a.NOM_PRENOM,
a.NOM,
a.PRENOM,
a.DATE_NAISSANCE,
a.DATE_EXPIRATION,
a.ADRESSE,
a.NUMERO_PIECE_TUTEUR,
a.NOM_PARENT,
a.DATE_NAISSANCE_TUTEUR,
a.DATE_ACTIVATION,
a.DATE_CHANGEMENT_STATUT,
a.statut_bscs as STATUT_ZSMART,
a.IMEI,
a.STATUT_DEROGATION,
a.REGION_ADMINISTRATIVE,
a.REGION_COMMERCIALE,
a.SITE_NAME,
a.VILLE,
a.LONGITUDE,
a.LATITUDE,
a.OFFRE_COMMERCIALE,
a.TYPE_CONTRAT,
a.SEGMENTATION,
a.STATUT_IN,
a.NUMERO_PIECE_ABSENT,
a.NUMERO_PIECE_TUT_ABSENT,
a.NUMERO_PIECE_INF_4,
a.NUMERO_PIECE_TUT_INF_4,
a.NUMERO_PIECE_NON_AUTHORISE,
a.NUMERO_PIECE_TUT_NON_AUTH,
a.NUMERO_PIECE_EGALE_MSISDN,
a.NUMERO_PIECE_TUT_EGALE_MSISDN,
a.NUMERO_PIECE_A_CARACT_NON_AUTH,
a.NUMERO_PIECE_TUT_CARAC_NON_A,
a.NUMERO_PIECE_UNIQUEMENT_LETTRE,
a.NUMERO_PIECE_TUT_UNIQ_LETTRE,
a.NOM_PRENOM_ABSENT,
a.NOM_PARENT_ABSENT,
a.NOM_PRENOM_DOUTEUX,
a.NOM_PARENT_DOUTEUX,
a.DATE_NAISSANCE_ABSENT,
a.DATE_NAISSANCE_TUT_ABSENT,
a.DATE_EXPIRATION_ABSENT,
a.ADRESSE_ABSENT,
a.ADRESSE_DOUTEUSE,
a.TYPE_PERSONNE,
a.DATE_NAISSANCE_DOUTEUX,
a.DATE_NAISSANCE_TUT_DOUTEUX,
a.DATE_EXPIRATION_DOUTEUSE,
a.CNI_EXPIRE,
a.EST_SUSPENDU,
a.CONFORME_ART,
a.IMEI_ABSENT,
a.ADRESSE_TUTEUR,
a.TYPE_PIECE_TUTEUR,
a.ACCEPTATION_CGV,
a.CONTRAT_SOUCRIPTION,
a.DISPONIBILITE_SCAN,
a.PLAN_LOCALISATION,
a.IDENTIFICATEUR,
a.PROFESSION_IDENTIFICATEUR,
a.MOTIF_REJET_BO,
a.EST_CONFORME_MAJ_KYC,
a.EST_CONFORME_MIN_KYC,
a.EST_SNAPPE,
a.STATUT_VALIDATION_BO,
substr(trim(a.msisdn),-3) as PART_MSISDN,
b.lang as LANGUE,
a.INSERT_DATE,
a.EVENT_DATE
from
(select A.*
from
(select *
from Mon.spark_ft_bdi
where event_date='###SLICE_VALUE###') A
join (select *
from cdr.spark_it_bdi_crm_b2c
where original_file_date=date_add('###SLICE_VALUE###',1)) B
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(B.msisdn))
where trim(A.msisdn) rlike '^\\d+$' and length(FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.msisdn))) = 9) a
left join (select *
from Mon.spark_ft_contract_snapshot
where event_date=date_add('###SLICE_VALUE###',1)) b
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(b.access_key))