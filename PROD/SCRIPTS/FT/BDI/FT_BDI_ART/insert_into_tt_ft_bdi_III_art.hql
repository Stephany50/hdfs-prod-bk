insert into TMP.TT_FT_BDI_III_ART
SELECT
        A.MSISDN    MSISDN,
        A.TYPE_PIECE    TYPE_PIECE,
        A.NUMERO_PIECE    NUMERO_PIECE,
        A.NOM_PRENOM   NOM_PRENOM,
        A.NOM   NOM,
        A.PRENOM    PRENOM,
        A.DATE_NAISSANCE    DATE_NAISSANCE,
        A.DATE_EXPIRATION    DATE_EXPIRATION,
        A.ADRESSE    ADRESSE,
        A.NUMERO_PIECE_TUTEUR    NUMERO_PIECE_TUTEUR,
        A.NOM_PARENT    NOM_PARENT,
        A.DATE_NAISSANCE_TUTEUR    DATE_NAISSANCE_TUTEUR,
        A.NOM_STRUCTURE    NOM_STRUCTURE,
        A.NUMERO_REGISTRE_COMMERCE    NUMERO_REGISTRE_COMMERCE,
        A.NUMERO_PIECE_REP_LEGAL    NUMERO_PIECE_REP_LEGAL,
        A.DATE_ACTIVATION    DATE_ACTIVATION,
        A.DATE_CHANGEMENT_STATUT    DATE_CHANGEMENT_STATUT,
        A.STATUT_BSCS    STATUT_BSCS,
        A.ODBINCOMINGCALLS    ODBINCOMINGCALLS,
        A.ODBOUTGOINGCALLS    ODBOUTGOINGCALLS,
        A.IMEI    IMEI,
        A.STATUT_DEROGATION    STATUT_DEROGATION,
        A.REGION_ADMINISTRATIVE    REGION_ADMINISTRATIVE,
        A.REGION_COMMERCIALE    REGION_COMMERCIALE,
        A.SITE_NAME    SITE_NAME,
        A.VILLE    VILLE,
        A.LONGITUDE    LONGITUDE,
        A.LATITUDE    LATITUDE,
        A.OFFRE_COMMERCIALE    OFFRE_COMMERCIALE,
        A.TYPE_CONTRAT    TYPE_CONTRAT,
        A.SEGMENTATION    SEGMENTATION,
        A.REV_M_3    REV_M_3,
        A.REV_M_2    REV_M_2,
        A.REV_M_1    REV_M_1,
        A.REV_MOY    REV_MOY,
        A.STATUT_IN    STATUT_IN,
        A.NUMERO_PIECE_ABSENT    NUMERO_PIECE_ABSENT,
        A.NUMERO_PIECE_TUT_ABSENT    NUMERO_PIECE_TUT_ABSENT,
        A.NUMERO_PIECE_INF_4    NUMERO_PIECE_INF_4,
        A.NUMERO_PIECE_TUT_INF_4    NUMERO_PIECE_TUT_INF_4,
        A.NUMERO_PIECE_NON_AUTHORISE    NUMERO_PIECE_NON_AUTHORISE,
        A.NUMERO_PIECE_TUT_NON_AUTH    NUMERO_PIECE_TUT_NON_AUTH,
        A.NUMERO_PIECE_EGALE_MSISDN    NUMERO_PIECE_EGALE_MSISDN,
        A.NUMERO_PIECE_TUT_EGALE_MSISDN    NUMERO_PIECE_TUT_EGALE_MSISDN,
        A.NUMERO_PIECE_A_CARACT_NON_AUTH    NUMERO_PIECE_A_CARACT_NON_AUTH,
        A.NUMERO_PIECE_TUT_CARAC_NON_A    NUMERO_PIECE_TUT_CARAC_NON_A,
        A.NUMERO_PIECE_UNIQUEMENT_LETTRE    NUMERO_PIECE_UNIQUEMENT_LETTRE,
        A.NUMERO_PIECE_TUT_UNIQ_LETTRE    NUMERO_PIECE_TUT_UNIQ_LETTRE,
        A.NOM_PRENOM_ABSENT    NOM_PRENOM_ABSENT,
        A.NOM_PARENT_ABSENT    NOM_PARENT_ABSENT,
        A.NOM_PRENOM_DOUTEUX    NOM_PRENOM_DOUTEUX,
        A.NOM_PARENT_DOUTEUX    NOM_PARENT_DOUTEUX,
        A.DATE_NAISSANCE_ABSENT    DATE_NAISSANCE_ABSENT,
        A.DATE_NAISSANCE_TUT_ABSENT    DATE_NAISSANCE_TUT_ABSENT,
        A.DATE_EXPIRATION_ABSENT    DATE_EXPIRATION_ABSENT,
        A.ADRESSE_ABSENT    ADRESSE_ABSENT,
        A.ADRESSE_DOUTEUSE    ADRESSE_DOUTEUSE,
        A.TYPE_PERSONNE_INCONNU    TYPE_PERSONNE_INCONNU,
        A.MINEUR_MAL_IDENTIFIE    MINEUR_MAL_IDENTIFIE,
        A.TYPE_PERSONNE    TYPE_PERSONNE,
        A.DATE_ACQUISITION    DATE_ACQUISITION,
        A.DATE_NAISSANCE_DOUTEUX    DATE_NAISSANCE_DOUTEUX,
        A.DATE_NAISSANCE_TUT_DOUTEUX    DATE_NAISSANCE_TUT_DOUTEUX,
        A.DATE_EXPIRATION_DOUTEUSE    DATE_EXPIRATION_DOUTEUSE,
        A.CNI_EXPIRE    CNI_EXPIRE,
        A.MULTI_SIM    MULTI_SIM,
        A.EST_PRESENT_OM    EST_PRESENT_OM,
        A.EST_PRESENT_ZEB    EST_PRESENT_ZEB,
        A.EST_PRESENT_ART    EST_PRESENT_ART,
        A.EST_PRESENT_GP    EST_PRESENT_GP,
        A.EST_PRESENT_OCM    EST_PRESENT_OCM,
        A.EST_ACTIF_OM    EST_ACTIF_OM,
        A.EST_CLIENT_VIP    EST_CLIENT_VIP,
        A.REV_OM_M_3    REV_OM_M_3,
        A.REV_OM_M_2    REV_OM_M_2,
        A.REV_OM_M_1    REV_OM_M_1,
        A.EST_ACTIF_DATA    EST_ACTIF_DATA,
        A.TRAFFIC_DATA_M_3    TRAFFIC_DATA_M_3,
        A.TRAFFIC_DATA_M_2    TRAFFIC_DATA_M_2,
        A.TRAFFIC_DATA_M_1    TRAFFIC_DATA_M_1,
        A.CONFORM_OCM_P_MORALE_M2M    CONFORM_OCM_P_MORALE_M2M,
        A.CONFORM_ART_P_MORALE_M2M    CONFORM_ART_P_MORALE_M2M,
        A.CONFORM_OCM_P_MORALE_FLOTTE    CONFORM_OCM_P_MORALE_FLOTTE,
        A.CONFORM_ART_P_MORALE_FLOTTE    CONFORM_ART_P_MORALE_FLOTTE,
        A.CONFORM_OCM_P_PHY_MAJEUR    CONFORM_OCM_P_PHY_MAJEUR,
        A.CONFORM_ART_P_PHY_MAJEUR    CONFORM_ART_P_PHY_MAJEUR,
        A.CONFORM_OCM_P_PHY_MINEUR    CONFORM_OCM_P_PHY_MINEUR,
        A.CONFORM_ART_P_PHY_MINEUR    CONFORM_ART_P_PHY_MINEUR,
        A.EST_SUSPENDU    EST_SUSPENDU,
        A.NOM_STRUCTURE_ABSENT    NOM_STRUCTURE_ABSENT,
        A.NUMERO_REGISTRE_ABSENT    NUMERO_REGISTRE_ABSENT,
        A.NUMERO_REGISTRE_DOUTEUX    NUMERO_REGISTRE_DOUTEUX,
        case when trim(A.TYPE_PERSONNE) is null
                OR trim(A.TYPE_PERSONNE)= ''
                OR trim(A.TYPE_PERSONNE) in ('PP','MAJEUR') then trim(A.CONFORM_ART_P_PHY_MAJEUR)
             when trim(A.TYPE_PERSONNE) =  'MINEUR'  then trim(A.CONFORM_ART_P_PHY_MINEUR)
             when trim(A.TYPE_PERSONNE) = 'PERSONNE_MORALE' then trim(A.CONFORM_ART_P_MORALE_FLOTTE)
             when trim(A.TYPE_PERSONNE) = 'MACHINE_2_MACHINE' then trim(A.CONFORM_ART_P_MORALE_M2M)
        END AS CONFORME_ART,
        case when trim(A.TYPE_PERSONNE) is null
                        OR trim(A.TYPE_PERSONNE)= ''
                        OR trim(A.TYPE_PERSONNE) in ('PP','MAJEUR') then trim(A.CONFORM_OCM_P_PHY_MAJEUR)
             when trim(A.TYPE_PERSONNE) =  'MINEUR'  then trim(A.CONFORM_OCM_P_PHY_MINEUR)
             when trim(A.TYPE_PERSONNE) = 'PERSONNE_MORALE' then trim(A.CONFORM_OCM_P_MORALE_FLOTTE)
             when trim(A.TYPE_PERSONNE) = 'MACHINE_2_MACHINE' then trim(A.CONFORM_OCM_P_MORALE_M2M)
        END AS CONFORME_OCM,
        A.IMEI_ABSENT    IMEI_ABSENT,
        A.EST_PREMIUM    EST_PREMIUM,
        A.ADRESSE_TUTEUR    ADRESSE_TUTEUR,
        A.TYPE_PIECE_TUTEUR     TYPE_PIECE_TUTEUR,
        A.ACCEPTATION_CGV   ACCEPTATION_CGV,
        A.CONTRAT_SOUCRIPTION   CONTRAT_SOUCRIPTION,
        A.DISPONIBILITE_SCAN    DISPONIBILITE_SCAN,
        A.PLAN_LOCALISATION     PLAN_LOCALISATION,
        B.IDENTIFICATEUR IDENTIFICATEUR,
        B.PROFESSION_IDENTIFICATEUR PROFESSION_IDENTIFICATEUR,
        A.DATE_VALIDATION_BO    DATE_VALIDATION_BO    ,
        A.STATUT_VALIDATION_BO     STATUT_VALIDATION_BO  ,
        A.MOTIF_REJET_BO  MOTIF_REJET_BO ,
    (Case when A.STATUT_VALIDATION_BO='1'   then 'VERIFIE ET VALIDE'
          when A.STATUT_VALIDATION_BO='2'   then   'VERIFIE ET REJETE NIVEAU 1'
          when A.STATUT_VALIDATION_BO='3'    then 'VERIFIE ET CORRIGE'
          when A.STATUT_VALIDATION_BO= '-1'  then 'VERIFIE ET NON CONFORME NIVEAU 1'
          when A.STATUT_VALIDATION_BO= '-2'  then 'VERIFIE ET NON CONFORME NIVEAU 2'
          when A.STATUT_VALIDATION_BO= '-3'  then 'VERIFIE ET NON CONFORME NIVEAU 3'
          when A.STATUT_VALIDATION_BO= '-4'  then 'PRET POUR LA SUSPENSION'
          when A.STATUT_VALIDATION_BO= '-5'  then 'SUSPENDU APPEL SORTANT BO'
          when A.STATUT_VALIDATION_BO= '-6'  then 'SUSPENDU HORS BO'
          when A.STATUT_VALIDATION_BO= '-7'  then 'MODIFIER AVANT LA SUSPENSION'
          when A.STATUT_VALIDATION_BO= '-8'  then  'ANNULE'
          when A.STATUT_VALIDATION_BO= '-9'  then  'DESACTIVE DANS BSCS'
          when (A.STATUT_VALIDATION_BO is null or A.STATUT_VALIDATION_BO = '' or
                  A.STATUT_VALIDATION_BO = '10'
                  ) and A.date_validation_bo is not null then 'STATUT INCONNU'
          else 'NON VERIFIE'
    end) STATUT_VALIDATION_BOO,
    'N/A' AS DISPONIBILITE_SCAN_SID,
    A.EST_CONFORME_MAJ_KYC AS EST_CONFORME_MAJ_KYC,
    A.EST_CONFORME_MIN_KYC As EST_CONFORME_MIN_KYC,
    case when trim(B.msisdn) is null or trim(B.msisdn) = '' then 'INEXISTANT'
         else trim(B.EST_SNAPPE)
    end AS EST_SNAPPE,
    current_timestamp() AS insert_date,
    A.EVENT_DATE  AS   EVENT_DATE
    FROM
    (
        SELECT
            A.*,
            (CASE
                WHEN trim(A.TYPE_PERSONNE) = 'MACHINE_2_MACHINE' THEN
                    (
                        CASE WHEN (
                                A.NOM_STRUCTURE IS NOT NULL AND trim(A.NOM_STRUCTURE) <> '' AND
                                (A.NUMERO_REGISTRE_COMMERCE IS NOT NULL AND trim(A.NUMERO_REGISTRE_COMMERCE) <> ''
                                 AND UPPER(trim(A.NUMERO_REGISTRE_COMMERCE)) <> 'NON RENSEIGNE')
                            ) THEN 'OUI' ELSE 'NON'
                    END)
                ELSE NULL
            END) CONFORM_OCM_P_MORALE_M2M,
            (CASE
                WHEN trim(A.TYPE_PERSONNE) = 'MACHINE_2_MACHINE' THEN
                    (
                        CASE WHEN (
                                A.NOM_STRUCTURE IS NOT NULL AND trim(A.NOM_STRUCTURE) <> '' AND
                                (A.NUMERO_REGISTRE_COMMERCE IS NOT NULL AND trim(A.NUMERO_REGISTRE_COMMERCE) <> '' AND
                                UPPER(trim(A.NUMERO_REGISTRE_COMMERCE)) <> 'NON RENSEIGNE')
                            ) THEN 'OUI' ELSE 'NON'
                    END)
                ELSE NULL
            END) CONFORM_ART_P_MORALE_M2M,
            (CASE
                WHEN trim(A.TYPE_PERSONNE) = 'PERSONNE_MORALE' THEN
                    (
                        CASE WHEN (
                                    A.NOM_STRUCTURE IS NOT NULL AND trim(A.NOM_STRUCTURE) <> '' AND
                                    (A.NUMERO_REGISTRE_COMMERCE IS NOT NULL AND trim(A.NUMERO_REGISTRE_COMMERCE) <> '' AND
                                     UPPER(trim(A.NUMERO_REGISTRE_COMMERCE)) <> 'NON RENSEIGNE') AND
                                    A.NUMERO_PIECE_REP_LEGAL IS NOT NULL AND trim(A.NUMERO_PIECE_REP_LEGAL) <> '' AND
                                    trim(A.NOM_PRENOM_ABSENT) = 'NON' AND A.NOM_PRENOM_DOUTEUX = 'NON' AND
                                    A.IMEI IS NOT NULL AND trim(A.IMEI) <> '' AND
                                    trim(A.NUMERO_PIECE_ABSENT) = 'NON' AND A.NUMERO_PIECE_INF_4 = 'NON' AND A.NUMERO_PIECE_NON_AUTHORISE =
                                    'NON'
                                    AND A.NUMERO_PIECE_EGALE_MSISDN = 'NON' AND A.NUMERO_PIECE_A_CARACT_NON_AUTH = 'NON' AND A.NUMERO_PIECE_UNIQUEMENT_LETTRE = 'NON'
                            ) THEN 'OUI' ELSE 'NON'
                    END)
                ELSE NULL
            END) CONFORM_OCM_P_MORALE_FLOTTE,
            (CASE
                WHEN trim(A.TYPE_PERSONNE) = 'PERSONNE_MORALE' THEN
                    (
                        CASE WHEN (
                                    A.NOM_STRUCTURE IS NOT NULL AND trim(A.NOM_STRUCTURE) <> '' AND
                                    (A.NUMERO_REGISTRE_COMMERCE IS NOT NULL AND trim(A.NUMERO_REGISTRE_COMMERCE) <> '' AND
                                     UPPER(trim(A.NUMERO_REGISTRE_COMMERCE)) <> 'NON RENSEIGNE') AND
                                    A.NUMERO_PIECE_REP_LEGAL IS NOT NULL AND
                                    NOM_PRENOM_ABSENT = 'NON' AND A.NOM_PRENOM_DOUTEUX = 'NON' AND
                                    A.IMEI IS NOT NULL AND trim(A.IMEI) <> '' AND
                                    A.NUMERO_PIECE_ABSENT = 'NON' AND A.NUMERO_PIECE_INF_4 = 'NON' AND A.NUMERO_PIECE_NON_AUTHORISE = 'NON'
                                    AND A.NUMERO_PIECE_EGALE_MSISDN = 'NON' AND A.NUMERO_PIECE_A_CARACT_NON_AUTH = 'NON' AND A.NUMERO_PIECE_UNIQUEMENT_LETTRE = 'NON'
                            ) THEN 'OUI' ELSE 'NON'
                    END)
                ELSE NULL
            END) CONFORM_ART_P_MORALE_FLOTTE,
            (CASE
                WHEN trim(A.TYPE_PERSONNE) = 'MAJEUR' OR A.TYPE_PERSONNE IS NULL OR trim(A.TYPE_PERSONNE) = '' OR
                A.TYPE_PERSONNE = 'PP' THEN
                    (
                        CASE WHEN (
                                A.DATE_ACTIVATION IS NOT NULL  AND
                                A.NOM_PRENOM_ABSENT = 'NON' AND A.NOM_PRENOM_DOUTEUX = 'NON' AND
                                A.NUMERO_PIECE_ABSENT = 'NON' AND A.NUMERO_PIECE_INF_4 = 'NON' AND A.NUMERO_PIECE_NON_AUTHORISE = 'NON'
                                AND A.NUMERO_PIECE_EGALE_MSISDN = 'NON' AND A.NUMERO_PIECE_A_CARACT_NON_AUTH = 'NON' AND A.NUMERO_PIECE_UNIQUEMENT_LETTRE = 'NON' AND
                                A.DATE_EXPIRATION_DOUTEUSE = 'NON' AND
                                A.TYPE_PIECE IS NOT NULL AND trim(A.TYPE_PIECE) <> '' AND
                                A.DATE_NAISSANCE_ABSENT = 'NON' AND A.DATE_NAISSANCE_DOUTEUX = 'NON' AND
                                A.CNI_EXPIRE = 'NON' AND
                                A.MULTI_SIM = 'NON' AND
                                A.ADRESSE IS NOT NULL AND trim(A.ADRESSE) <> '' AND
                                A.IMEI IS NOT NULL AND trim(A.IMEI) <> ''
                            ) THEN 'OUI' ELSE 'NON'
                    END)
                ELSE NULL
            END) CONFORM_OCM_P_PHY_MAJEUR,
            (CASE
                WHEN A.TYPE_PERSONNE = 'MAJEUR' OR A.TYPE_PERSONNE IS NULL OR trim(A.TYPE_PERSONNE) = '' OR A.TYPE_PERSONNE = 'PP' THEN
                    (
                        CASE WHEN (
                                A.DATE_ACTIVATION IS NOT NULL  AND
                                A.NOM_PRENOM_ABSENT = 'NON' AND A.NOM_PRENOM_DOUTEUX = 'NON' AND
                                A.NUMERO_PIECE_ABSENT = 'NON' AND A.NUMERO_PIECE_INF_4 = 'NON' AND A.NUMERO_PIECE_NON_AUTHORISE = 'NON'
                                AND A.NUMERO_PIECE_EGALE_MSISDN = 'NON' AND A.NUMERO_PIECE_A_CARACT_NON_AUTH = 'NON' AND A.NUMERO_PIECE_UNIQUEMENT_LETTRE = 'NON' AND
                                A.DATE_EXPIRATION_DOUTEUSE = 'NON' AND
                                A.TYPE_PIECE IS NOT NULL AND trim(A.TYPE_PIECE) <> '' AND
                                A.DATE_NAISSANCE_ABSENT = 'NON' AND A.DATE_NAISSANCE_DOUTEUX = 'NON' AND
                                A.CNI_EXPIRE = 'NON' AND
                                A.MULTI_SIM = 'NON' AND
                                A.ADRESSE IS NOT NULL AND trim(A.ADRESSE) <> ''  AND
                                A.IMEI IS NOT NULL AND trim(A.IMEI) <> ''
                            ) THEN 'OUI' ELSE 'NON'
                    END)
                ELSE NULL
            END) CONFORM_ART_P_PHY_MAJEUR,
            (CASE
                WHEN A.TYPE_PERSONNE = 'MINEUR' THEN
                    (
                        CASE WHEN (
                                A.DATE_ACTIVATION IS NOT NULL  AND
                                A.NOM_PRENOM_ABSENT = 'NON' AND A.NOM_PRENOM_DOUTEUX = 'NON' AND
                                A.NUMERO_PIECE_ABSENT = 'NON' AND A.NUMERO_PIECE_INF_4 = 'NON' AND A.NUMERO_PIECE_NON_AUTHORISE = 'NON'
                                AND A.NUMERO_PIECE_EGALE_MSISDN = 'NON' AND A.NUMERO_PIECE_A_CARACT_NON_AUTH = 'NON' AND A.NUMERO_PIECE_UNIQUEMENT_LETTRE = 'NON' AND
                                A.DATE_EXPIRATION_DOUTEUSE = 'NON' AND
                                A.TYPE_PIECE IS NOT NULL AND trim(A.TYPE_PIECE) <> '' AND
                                A.DATE_NAISSANCE_ABSENT = 'NON' AND A.DATE_NAISSANCE_DOUTEUX = 'NON' AND
                                A.NOM_PARENT_ABSENT = 'NON' AND A.NOM_PARENT_DOUTEUX = 'NON' AND
                                A.NUMERO_PIECE_TUT_ABSENT = 'NON' AND A.NUMERO_PIECE_TUT_INF_4 = 'NON' AND
                                A.NUMERO_PIECE_TUT_NON_AUTH = 'NON' AND A.NUMERO_PIECE_TUT_EGALE_MSISDN = 'NON' AND
                                A.NUMERO_PIECE_TUT_CARAC_NON_A = 'NON' AND A.NUMERO_PIECE_TUT_UNIQ_LETTRE = 'NON' AND
                                A.DATE_NAISSANCE_TUTEUR IS NOT NULL  AND
                                A.CNI_EXPIRE = 'NON' AND
                                A.MULTI_SIM = 'NON' AND
                                A.ADRESSE IS NOT NULL AND trim(A.ADRESSE) <> '' AND
                                A.IMEI IS NOT NULL  AND trim(A.IMEI) <> ''
                            ) THEN 'OUI' ELSE 'NON'
                    END)
                ELSE NULL
            END) CONFORM_OCM_P_PHY_MINEUR,
            (CASE
                WHEN A.TYPE_PERSONNE = 'MINEUR' THEN
                    (
                        CASE WHEN (
                                A.DATE_ACTIVATION IS NOT NULL  AND
                                A.NOM_PRENOM_ABSENT = 'NON' AND A.NOM_PRENOM_DOUTEUX = 'NON' AND
                                A.NUMERO_PIECE_ABSENT = 'NON' AND A.NUMERO_PIECE_INF_4 = 'NON' AND A.NUMERO_PIECE_NON_AUTHORISE = 'NON'
                                AND A.NUMERO_PIECE_EGALE_MSISDN = 'NON' AND A.NUMERO_PIECE_A_CARACT_NON_AUTH = 'NON' AND A.NUMERO_PIECE_UNIQUEMENT_LETTRE = 'NON' AND
                                A.DATE_EXPIRATION_DOUTEUSE = 'NON' AND
                                A.TYPE_PIECE IS NOT NULL AND trim(A.TYPE_PIECE) <> '' AND
                                A.DATE_NAISSANCE_ABSENT = 'NON' AND A.DATE_NAISSANCE_DOUTEUX = 'NON' AND
                                A.NOM_PARENT_ABSENT = 'NON' AND A.NOM_PARENT_DOUTEUX = 'NON' AND
                                A.NUMERO_PIECE_TUT_ABSENT = 'NON' AND A.NUMERO_PIECE_TUT_INF_4 = 'NON' AND A.NUMERO_PIECE_TUT_NON_AUTH = 'NON'
                                AND A.NUMERO_PIECE_TUT_EGALE_MSISDN = 'NON' AND A.NUMERO_PIECE_TUT_CARAC_NON_A = 'NON' AND A.NUMERO_PIECE_TUT_UNIQ_LETTRE = 'NON' AND
                                A.DATE_NAISSANCE_TUTEUR IS NOT NULL AND
                                A.CNI_EXPIRE = 'NON' AND
                                A.MULTI_SIM = 'NON' AND
                                A.ADRESSE IS NOT NULL AND trim(A.ADRESSE) <> '' AND
                                A.IMEI IS NOT NULL AND trim(A.IMEI) <> ''
                            ) THEN 'OUI' ELSE 'NON'
                    END)
                ELSE NULL
            END) CONFORM_ART_P_PHY_MINEUR,
            (case
                WHEN trim(A.ODBOUTGOINGCALLS) = '1'
                then case when trim(A.ODBINCOMINGCALLS) = '1' then  'OUI'
                          else 'NON'
                      end
                WHEN trim(A.ODBINCOMINGCALLS) = '1'
                then case when trim(A.ODBOUTGOINGCALLS) <> '1' then  'NON'
                      end
                else 'NON'
            end) as  EST_SUSPENDU,
            (CASE WHEN A.NOM_STRUCTURE IS NULL OR trim(A.NOM_STRUCTURE) = '' THEN 'OUI' ELSE 'NON' END) NOM_STRUCTURE_ABSENT,
            (CASE WHEN A.NUMERO_REGISTRE_COMMERCE IS NULL OR trim(A.NUMERO_REGISTRE_COMMERCE) = '' THEN 'OUI' ELSE 'NON'
            END) NUMERO_REGISTRE_ABSENT,
            (CASE WHEN UPPER(trim(A.NUMERO_REGISTRE_COMMERCE)) = 'NON RENSEIGNE' THEN 'OUI' ELSE 'NON' END) NUMERO_REGISTRE_DOUTEUX,
            (CASE WHEN A.IMEI IS NULL OR trim(A.IMEI) = '' THEN 'OUI' ELSE 'NON' END) IMEI_ABSENT,
            (case when A.DATE_ACTIVATION IS NULL
                OR A.NOM_PRENOM_ABSENT = 'OUI'
                OR A.NOM_PRENOM_DOUTEUX = 'OUI'
                OR A.NUMERO_PIECE_ABSENT = 'OUI'
                OR A.NUMERO_PIECE_INF_4 = 'OUI'
                OR A.NUMERO_PIECE_NON_AUTHORISE = 'OUI'
                OR A.NUMERO_PIECE_EGALE_MSISDN = 'OUI'
                OR A.NUMERO_PIECE_A_CARACT_NON_AUTH = 'OUI'
                OR A.NUMERO_PIECE_UNIQUEMENT_LETTRE = 'OUI'
                OR A.DATE_EXPIRATION_DOUTEUSE = 'OUI'
                OR A.DATE_EXPIRATION_ABSENT = 'OUI'
                OR A.DATE_NAISSANCE_ABSENT = 'OUI'
                OR A.DATE_NAISSANCE_DOUTEUX = 'OUI'
                OR A.MULTI_SIM = 'OUI'
                OR A.ADRESSE_ABSENT = 'OUI'
                OR A.ADRESSE_DOUTEUSE = 'OUI'
                OR A.IMEI is null OR trim(A.IMEI) = ''
                OR A.TYPE_PIECE IS NULL OR trim(A.TYPE_PIECE) = ''
                OR A.MSISDN IS NULL OR trim(A.MSISDN) = ''
                then 'NON' else 'OUI'
             end ) AS EST_CONFORME_MAJ_KYC,
             (case when A.DATE_ACTIVATION IS NULL
                 OR A.NOM_PRENOM_ABSENT = 'OUI'
                 OR A.NOM_PRENOM_DOUTEUX = 'OUI'
                 OR A.NUMERO_PIECE_ABSENT = 'OUI'
                 OR A.NUMERO_PIECE_INF_4 = 'OUI'
                 OR A.NUMERO_PIECE_NON_AUTHORISE = 'OUI'
                 OR A.NUMERO_PIECE_EGALE_MSISDN = 'OUI'
                 OR A.NUMERO_PIECE_A_CARACT_NON_AUTH = 'OUI'
                 OR A.NUMERO_PIECE_UNIQUEMENT_LETTRE = 'OUI'
                 OR A.DATE_EXPIRATION_DOUTEUSE = 'OUI'
                 OR A.DATE_EXPIRATION_ABSENT = 'OUI'
                 OR A.TYPE_PIECE IS NULL OR trim(A.TYPE_PIECE) = ''
                 OR A.DATE_NAISSANCE_ABSENT = 'OUI'
                 OR A.DATE_NAISSANCE_DOUTEUX = 'OUI'
                 OR A.NOM_PARENT_ABSENT = 'OUI'
                 OR A.NOM_PARENT_DOUTEUX = 'OUI'
                 OR A.NUMERO_PIECE_TUT_ABSENT = 'OUI'
                 OR A.NUMERO_PIECE_TUT_INF_4 = 'OUI'
                 OR A.NUMERO_PIECE_TUT_NON_AUTH = 'OUI'
                 OR A.NUMERO_PIECE_TUT_EGALE_MSISDN = 'OUI'
                 OR A.NUMERO_PIECE_TUT_CARAC_NON_A = 'OUI'
                 OR A.NUMERO_PIECE_TUT_UNIQ_LETTRE = 'OUI'
                 OR A.DATE_NAISSANCE_TUT_ABSENT = 'OUI'
                 OR A.DATE_NAISSANCE_TUT_DOUTEUX ='OUI'
                 OR A.MULTI_SIM = 'OUI'
                 OR A.ADRESSE_ABSENT = 'OUI'
                 OR A.ADRESSE_DOUTEUSE = 'OUI'
                 OR A.IMEI is null OR trim(A.IMEI) = ''
                 OR A.MSISDN IS NULL OR trim(A.MSISDN) = '' then 'NON' else 'OUI'
              END) AS EST_CONFORME_MIN_KYC
        FROM TMP.TT_FT_BDI_II_ART A
    ) A
    LEFT JOIN (SELECT MSISDN, IDENTIFICATEUR, PROFESSION_IDENTIFICATEUR,EST_SNAPPE FROM
    DIM.SPARK_DT_BASE_IDENTIFICATION) B
    ON trim(A.MSISDN) = trim(B.MSISDN)