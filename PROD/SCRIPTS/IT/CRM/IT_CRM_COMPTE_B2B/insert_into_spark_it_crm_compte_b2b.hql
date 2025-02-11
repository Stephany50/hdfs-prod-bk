INSERT INTO CDR.SPARK_IT_CRM_COMPTE_B2B
SELECT 
  CODE_CLIENT                   ,
  RAISON_SOCIALE                ,
  COMPTE_CLIENT_PRODUIT         ,
  TYPE_CLIENT                   ,
  LBL_TYPE_CLIENT               ,
  TYPE_CLIENT_BSCS              ,
  LBL_TYPE_CLIENT_BSCS          ,
  TYPE_D_IDENTIFIANT            ,
  LBL_TYPE_D_IDENTIFIANT        ,
  NUM_REGISTRE_DE_COMMERCE      ,
  NUM_DE_CONTRIBUABLE           ,
  NUM_ORGANISATION              ,
  CATEGORIE_CLIENT              ,
  LBL_CATEGORIE_CLIENT          ,
  DELIVRE_LE                    ,
  RESPONSABLE_DU_COMPTE_        ,
  TYPE_DE_RELATION              ,
  LBL_TYPE_DE_RELATION          ,
  COMMERCIAL                    ,
  LBL_COMMERCIAL                ,
  COMPTE_PARENT                 ,
  COMPTES_ENFANTS               ,
  STATUT_CLIENT                 ,
  LBL_STATUT_CLIENT             ,
  LANGUE                        ,
  LBL_LANGUE                    ,
  DEUXIEME_LANGUE               ,
  LBL_DEUXIEME_LANGUE           ,
  TROISIEME_LANGUE              ,
  LBL_TROISIEME_LANGUE          ,
  VIP___VOIX                    ,
  VIP___SMS                     ,
  VIP___DATA                    ,
  VIP___OM                      ,
  VIP___GLOBALE                 ,
  TELEPHONE                     ,
  TELECOPIE                     ,
  SITE_WEB                      ,
  CONTACT_PRINCIPAL             ,
  EMAIL                         ,
  TELEPHONE_PRINCIPAL           ,
  SECTEUR_D_ACTIVITE            ,
  LBL_SECTEUR_D_ACTIVITE        ,
  FORME_JURIDIQUE               ,
  LBL_FORME_JURIDIQUE           ,
  HOMOLOGATION___SCORING        ,
  CA                            ,
  NOMBRE_D_EMPLOYES             ,
  PROSPECT_D_ORIGINE            ,
  LBL_PROSPECT_D_ORIGINE        ,
  DERNIERE_DATE_DE_LA_CAMP      ,
  ENVOYER_DOCUMENTS_MKT         ,
  LBL_ENVOYER_DOCUMENTS_MKT     ,
  LIMITE_DE_CREDIT              ,
  SUSPENSION_DE_CREDIT          ,
  EXONERATION_TVA               ,
  VALIDE_AU                     ,
  RISTOURNE                     ,
  PERIODE__MOIS_                ,
  REMISE                        ,
  CANAL_ASSOCIATION             ,
  DATE_D_ENTREE                 ,
  DATE_DE_SORTIE                ,
  MODE_DE_COMMUNICATION         ,
  LBL_MODE_DE_COMM              ,
  ENVOI_COURRIER_ELECTRONIQUE   ,
  LBL_ENVOI_COURRIER_ELECT      ,
  ENVOI_COURRIER_ELEC_EN_NBRE   ,
  LBL_ENVOI_COURRIER_ELEC_NBRE  ,
  ENVOI_TELEPHONE_SMS           ,
  LBL_ENVOI_TELEPHONE_SMS       ,
  ENVOI_TELECOPIE               ,
  LBL_ENVOI_TELECOPIE           ,
  ENVOI_COURRIER_POSTAL         ,
  LBL_ENVOI_COURRIER_POSTAL     ,
  CREE_LE                       ,
  CREE_PAR                      ,
  LBL_CREE_PAR                  ,
  MODIFIE_LE                    ,
  MODIFIE_PAR                   ,
  ORIGINAL_FILE_NAME            ,
  CURRENT_DATE() INSERTED_DATE,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -17, 6),'ddMMyy'))) ORIGINAL_FILE_DATE
  FROM CDR.TT_CRM_COMPTE_B2B C
  LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_CRM_COMPTE_B2B) T ON T.FILE_NAME = C.ORIGINAL_FILE_NAME
WHERE  T.FILE_NAME IS NULL
