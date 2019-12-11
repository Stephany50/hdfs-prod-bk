CREATE TABLE MON.FT_DATAMART_OM_MONTH 
   (	
    MSISDN VARCHAR(50), 
	USER_ID VARCHAR(100), 
	DATE_CREATION_CPTE DATE, 
	DATE_DERNIERE_ACTIVITE_OM DATE, 
	NB_JR_ACTIVITE INT, 
	NB_OPERATIONS INT, 
	NB_SERVICES_DISTINCTS INT, 
	SOLDE_DEBUT_MOIS INT, 
	SOLDE_FIN_MOIS INT, 
	ARPU_OM INT, 
	NB_CI INT, 
	MONTANT_CI INT, 
	NB_CO INT, 
	MONTANT_CO INT, 
	FRAIS_CO INT, 
	NB_BILL_PAY INT, 
	MONTANT_BILL_PAY INT, 
	FRAIS_BILL_PAY INT, 
	NB_MERCHPAY INT, 
	MONTANT_MERCHPAY INT, 
	FRAIS_MERCHPAY INT, 
	NB_PARTENAIRES_DISTINCTS INT, 
	NB_P2P_RECU INT, 
	MONTANT_P2P_RECU INT, 
	NB_MSISDN_TRANSMIS_P2P INT, 
	NB_P2P_ORANGE INT, 
	MONTANT_P2P_ORANGE INT, 
	FRAIS_P2P_ORANGE INT, 
	NB_TNO INT, 
	MONTANT_TNO INT, 
	FRAIS_TNO INT, 
	NB_MSISDN_RECUS_P2P INT, 
	NB_TOTAL_P2P INT, 
	MONTANT_TOTAL_P2P INT, 
	FRAIS_TOTAL_P2P INT, 
	NB_TOP_UP INT, 
	MONTANT_TOP_UP INT, 
	NB_TOP_UP_POUR_TIER INT, 
	MONTANT_TOP_UP_POUR_TIER INT, 
	NB_AUTRES INT, 
	MONTANT_AUTRES INT, 
	NB_BUNDLES_DATA INT, 
	MONTANT_BDLE_DATA INT, 
	NB_BUNDLE_VOIX INT, 
	MONTANT_BDLE_VOIX INT, 
	USER_TYPE VARCHAR(50), 
	INSERT_DATE TIMESTAMP 
  	)
COMMENT 'FT_AGENT_SESSION - FT'
PARTITIONED BY (MOIS  VARCHAR(25))
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864") 