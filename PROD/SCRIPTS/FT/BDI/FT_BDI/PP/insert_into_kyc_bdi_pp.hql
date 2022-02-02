insert into MON.SPARK_FT_KYC_BDI_PP
select
ft2.MSISDN AS MSISDN,
ft2.TYPE_PIECE AS TYPE_PIECE,
ft2.NUMERO_PIECE AS NUMERO_PIECE,
ft2.NOM_PRENOM AS NOM_PRENOM,
ft2.NOM AS NOM,
ft2.PRENOM AS PRENOM,
ft2.DATE_NAISSANCE AS DATE_NAISSANCE,
ft2.DATE_EXPIRATION AS DATE_EXPIRATION,
ft2.ADRESSE AS ADRESSE,
ft2.NUMERO_PIECE_TUTEUR AS NUMERO_PIECE_TUTEUR,
ft2.NOM_PARENT AS NOM_PARENT,
ft2.DATE_NAISSANCE_TUTEUR AS DATE_NAISSANCE_TUTEUR,
ft2.NOM_STRUCTURE AS NOM_STRUCTURE,
ft2.NUMERO_REGISTRE_COMMERCE AS NUMERO_REGISTRE_COMMERCE,
ft2.NUMERO_PIECE_REP_LEGAL AS NUMERO_PIECE_REP_LEGAL,
ft2.DATE_ACTIVATION AS DATE_ACTIVATION,
ft2.DATE_CHANGEMENT_STATUT AS DATE_CHANGEMENT_STATUT,
ft2.STATUT_BSCS AS STATUT_BSCS,
ft2.ODBINCOMINGCALLS AS ODBINCOMINGCALLS,
ft2.ODBOUTGOINGCALLS AS ODBOUTGOINGCALLS,
ft2.IMEI AS IMEI,
ft2.STATUT_DEROGATION AS STATUT_DEROGATION,
ft2.REGION_ADMINISTRATIVE AS REGION_ADMINISTRATIVE,
ft2.REGION_COMMERCIALE AS REGION_COMMERCIALE,
ft2.SITE_NAME AS SITE_NAME,
ft2.VILLE AS VILLE,
ft2.LONGITUDE AS LONGITUDE,
ft2.LATITUDE AS LATITUDE,
ft2.OFFRE_COMMERCIALE AS OFFRE_COMMERCIALE,
ft2.TYPE_CONTRAT AS TYPE_CONTRAT,
ft2.SEGMENTATION AS SEGMENTATION,
ft2.REV_M_3 AS REV_M_3,
ft2.REV_M_2 AS REV_M_2,
ft2.REV_M_1 AS REV_M_1,
ft2.REV_MOY AS REV_MOY,
ft2.STATUT_IN AS STATUT_IN,
ft2.NUMERO_PIECE_ABSENT AS NUMERO_PIECE_ABSENT,
ft2.NUMERO_PIECE_TUT_ABSENT AS NUMERO_PIECE_TUT_ABSENT,
ft2.NUMERO_PIECE_INF_4 AS NUMERO_PIECE_INF_4,
ft2.NUMERO_PIECE_TUT_INF_4 AS NUMERO_PIECE_TUT_INF_4,
ft2.NUMERO_PIECE_NON_AUTHORISE AS NUMERO_PIECE_NON_AUTHORISE,
ft2.NUMERO_PIECE_TUT_NON_AUTH AS NUMERO_PIECE_TUT_NON_AUTH,
ft2.NUMERO_PIECE_EGALE_MSISDN AS NUMERO_PIECE_EGALE_MSISDN,
ft2.NUMERO_PIECE_TUT_EGALE_MSISDN AS NUMERO_PIECE_TUT_EGALE_MSISDN,
ft2.NUMERO_PIECE_A_CARACT_NON_AUTH AS NUMERO_PIECE_A_CARACT_NON_AUTH,
ft2.NUMERO_PIECE_TUT_CARAC_NON_A AS NUMERO_PIECE_TUT_CARAC_NON_A,
ft2.NUMERO_PIECE_UNIQUEMENT_LETTRE AS NUMERO_PIECE_UNIQUEMENT_LETTRE,
ft2.NUMERO_PIECE_TUT_UNIQ_LETTRE AS NUMERO_PIECE_TUT_UNIQ_LETTRE,
ft2.NOM_PRENOM_ABSENT AS NOM_PRENOM_ABSENT,
ft2.NOM_PARENT_ABSENT AS NOM_PARENT_ABSENT,
ft2.NOM_PRENOM_DOUTEUX AS NOM_PRENOM_DOUTEUX,
ft2.NOM_PARENT_DOUTEUX AS NOM_PARENT_DOUTEUX,
ft2.DATE_NAISSANCE_ABSENT AS DATE_NAISSANCE_ABSENT,
ft2.DATE_NAISSANCE_TUT_ABSENT AS DATE_NAISSANCE_TUT_ABSENT,
ft2.DATE_EXPIRATION_ABSENT AS DATE_EXPIRATION_ABSENT,
ft2.ADRESSE_ABSENT AS ADRESSE_ABSENT,
ft2.ADRESSE_DOUTEUSE AS ADRESSE_DOUTEUSE,
ft2.TYPE_PERSONNE_INCONNU AS TYPE_PERSONNE_INCONNU,
ft2.MINEUR_MAL_IDENTIFIE AS MINEUR_MAL_IDENTIFIE,
ft2.TYPE_PERSONNE AS TYPE_PERSONNE,
ft2.DATE_ACQUISITION AS DATE_ACQUISITION,
ft2.DATE_NAISSANCE_DOUTEUX AS DATE_NAISSANCE_DOUTEUX,
ft2.DATE_NAISSANCE_TUT_DOUTEUX AS DATE_NAISSANCE_TUT_DOUTEUX,
ft2.DATE_EXPIRATION_DOUTEUSE AS DATE_EXPIRATION_DOUTEUSE,
ft2.CNI_EXPIRE AS CNI_EXPIRE,
ft2.MULTI_SIM AS MULTI_SIM,
ft2.EST_PRESENT_OM AS EST_PRESENT_OM,
ft2.EST_PRESENT_ZEB AS EST_PRESENT_ZEB,
ft2.EST_PRESENT_ART AS EST_PRESENT_ART,
ft2.EST_PRESENT_GP AS EST_PRESENT_GP,
ft2.EST_PRESENT_OCM AS EST_PRESENT_OCM,
ft2.EST_ACTIF_OM AS EST_ACTIF_OM,
ft2.EST_CLIENT_VIP AS EST_CLIENT_VIP,
ft2.REV_OM_M_3 AS REV_OM_M_3,
ft2.REV_OM_M_2 AS REV_OM_M_2,
ft2.REV_OM_M_1 AS REV_OM_M_1,
ft2.EST_ACTIF_DATA AS EST_ACTIF_DATA,
ft2.TRAFFIC_DATA_M_3 AS TRAFFIC_DATA_M_3,
ft2.TRAFFIC_DATA_M_2 AS TRAFFIC_DATA_M_2,
ft2.TRAFFIC_DATA_M_1 AS TRAFFIC_DATA_M_1,
ft2.CONFORM_OCM_P_MORALE_M2M AS CONFORM_OCM_P_MORALE_M2M,
ft2.CONFORM_ART_P_MORALE_M2M AS CONFORM_ART_P_MORALE_M2M,
ft2.CONFORM_OCM_P_MORALE_FLOTTE AS CONFORM_OCM_P_MORALE_FLOTTE,
ft2.CONFORM_ART_P_MORALE_FLOTTE AS CONFORM_ART_P_MORALE_FLOTTE,
ft2.CONFORM_OCM_P_PHY_MAJEUR AS CONFORM_OCM_P_PHY_MAJEUR,
ft2.CONFORM_ART_P_PHY_MAJEUR AS CONFORM_ART_P_PHY_MAJEUR,
ft2.CONFORM_OCM_P_PHY_MINEUR AS CONFORM_OCM_P_PHY_MINEUR,
ft2.CONFORM_ART_P_PHY_MINEUR AS CONFORM_ART_P_PHY_MINEUR,
ft2.EST_SUSPENDU AS EST_SUSPENDU,
ft2.NOM_STRUCTURE_ABSENT AS NOM_STRUCTURE_ABSENT,
ft2.NUMERO_REGISTRE_ABSENT AS NUMERO_REGISTRE_ABSENT,
ft2.NUMERO_REGISTRE_DOUTEUX AS NUMERO_REGISTRE_DOUTEUX,
ft2.CONFORME_ART AS CONFORME_ART,
ft2.CONFORME_OCM AS CONFORME_OCM,
ft2.IMEI_ABSENT AS IMEI_ABSENT,
ft2.EST_PREMIUM AS EST_PREMIUM,
ft2.ADRESSE_TUTEUR AS ADRESSE_TUTEUR,
ft2.TYPE_PIECE_TUTEUR AS TYPE_PIECE_TUTEUR,
ft2.ACCEPTATION_CGV AS ACCEPTATION_CGV,
ft2.CONTRAT_SOUCRIPTION AS CONTRAT_SOUCRIPTION,
ft2.DISPONIBILITE_SCAN AS DISPONIBILITE_SCAN,
ft2.PLAN_LOCALISATION AS PLAN_LOCALISATION,
ft2.IDENTIFICATEUR AS IDENTIFICATEUR,
ft2.PROFESSION_IDENTIFICATEUR AS PROFESSION_IDENTIFICATEUR,
ft2.DATE_VALIDATION_BO AS DATE_VALIDATION_BO,
ft2.STATUT_VALIDATION_BO AS STATUT_VALIDATION_BO,
lower(ft2.MOTIF_REJET_BO) AS MOTIF_REJET_BO,
ft2.STATUT_VALIDATION_BOO AS STATUT_VALIDATION_BOO,
ft2.DISPONIBILITE_SCAN_SID AS DISPONIBILITE_SCAN_SID,
ft2.EST_CONFORME_MAJ_KYC AS EST_CONFORME_MAJ_KYC,
ft2.EST_CONFORME_MIN_KYC AS EST_CONFORME_MIN_KYC,
ft2.EST_SNAPPE AS EST_SNAPPE,
current_timestamp() AS INSERT_DATE,
'###SLICE_VALUE###' AS EVENT_DATE
from (
 select ft.*,
 row_number() over(partition by ft.msisdn order by ft.date_activation  DESC NULLS LAST) as RANG
 from TMP.TT_KYC_BDI_PP_ST3 ft
) ft2 where RANG = 1