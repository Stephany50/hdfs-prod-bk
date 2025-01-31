CREATE TABLE CDR.TT_CRM_ABONNEMENTS
   (	ID NUMBER(*,0),
	COMPTE_B2B VARCHAR2(100 BYTE),
	COMPTE_B2C VARCHAR2(100 BYTE),
	COMPTE_FACTURATION VARCHAR2(100 BYTE),
	TYPE_ABONNEMENT NUMBER(*,0),
	LBL_TYPE_ABONNEMENT VARCHAR2(100 BYTE),
	MSISDN_IDWIMAX VARCHAR2(100 BYTE),
	ICCI_ADRESSE_MAC VARCHAR2(100 BYTE),
	IMSI VARCHAR2(100 BYTE),
	CODE_PUK1 VARCHAR2(100 BYTE),
	CODE_PUK2 VARCHAR2(100 BYTE),
	DATE_ACTIVATION DATE,
	STATUT_ABONNEMENT NUMBER(*,0),
	LBL_STATUT_ABONNEMENT VARCHAR2(100 BYTE),
	DATE_MAJ_STATUT DATE,
	OFFRE NUMBER(*,0),
	LBL_OFFRE VARCHAR2(100 BYTE),
	PROPRIETAIRE VARCHAR2(100 BYTE),
	LBL_PROPRIETAIRE VARCHAR2(100 BYTE),
	ORIGINAL_FILE_NAME VARCHAR2(100 BYTE),
	ORIGINAL_FILE_DATE DATE,
	INSERTED_DATE DATE
)