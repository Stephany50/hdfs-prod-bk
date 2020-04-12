INSERT INTO SPARK_TT_BDI_STK_PERS_MORALE
SELECT
b.COMPTE_CLIENT COMPTE_CLIENT,
s.RAISON_SOCIALE RAISON_SOCIALE,
s.NOM_REPRESENTANT_LEGAL NOM_REPRESENTANT_LEGAL,
s.PRENOM_REPRESENTANT_LEGAL PRENOM_REPRESENTANT_LEGAL,
s.CNI_REPRESENTANT_LOCAL CNI_REPRESENTANT_LOCAL,
s.CONTACT_TELEPHONIQUE CONTACT_TELEPHONIQUE,
s.ADRESSE_STRUCTURE ADRESSE_STRUCTURE,
s.NUMERO_REGISTRE_COMMERCE NUMERO_REGISTRE_COMMERCE

FROM bdi_#YYYY##MM##DD#_stk_pers_morale s JOIN bdi_#YYYY##MM##DD#_tmp b ON s.MSISDN = b.MSISDN