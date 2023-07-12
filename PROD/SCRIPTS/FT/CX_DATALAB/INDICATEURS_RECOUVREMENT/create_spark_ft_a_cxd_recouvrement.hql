---Table qui contient les KPIs B2B Creance, badDebt et risque de dotation.
--- categ : la catégorie du client Ex: PME, SOHO, GC, etc..
--- statut : le statut di client
--- creance : la creance du client
--- creance NBR jrs : Créance vielle de moins de NBR.
--- badDebt : La badDebt.
--- risqueDot : le risque de dotation. 
CREATE TABLE MON.SPARK_FT_A_CXD_RECOUVREMENT(
   categ   string,
   statut  string,
   nbr_client_actif bigint,
   nbr_client_suspendu bigint,
   nbr_client_inactif bigint,
   creance double,
   creance0jrs    double,
   creance30jrs   double,
   creance60jrs   double,
   creance90jrs   double,
   creancePlus120jrs     double,
   badDebt double,
   risqueDot double,
   insert_date timestamp
)PARTITIONED BY (event_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


--Stagging table RECOUV. datalake
CREATE TABLE TMP.SQ_CXD_RECOUVREMENT(
   categ    VARCHAR(256),
   statut   VARCHAR(256),
   nbr_client_actif decimal(20,7),
   nbr_client_suspendu decimal(20,7),
   nbr_client_inactif decimal(20,7),
   creance decimal(20,7),
   creance0jrs    decimal(20,7),
   creance30jrs   decimal(20,7),
   creance60jrs   decimal(20,7),
   creance90jrs   decimal(20,7),
   creancePlus120jrs     decimal(20,7),
   badDebt decimal(20,7),
   risqueDot decimal(20,7),
   insert_date timestamp,
   EVENT_DATE  DATE
)STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


--Stagging table RECOUV. DWH
CREATE TABLE MON.SQ_FT_CXD_RECOUVREMENT(
   categ   VARCHAR(256 BYTE),
   statut  VARCHAR(256 BYTE),
   nbr_client_actif decimal(20,7),
   nbr_client_suspendu decimal(20,7),
   nbr_client_inactif decimal(20,7),
   creance decimal(20,7),
   creance0jrs    decimal(20,7),
   creance30jrs   decimal(20,7),
   creance60jrs   decimal(20,7),
   creance90jrs   decimal(20,7),
   creancePlus120jrs     decimal(20,7),
   badDebt decimal(20,7),
   risqueDot decimal(20,7),
   insert_date timestamp,
   EVENT_DATE  DATE
);

--final table RECOUV DWH
DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'MON.SQ_FT_CXD_RECOUVREMENT';
  MIN_DATE_PARTITION := '20221201';
  MAX_DATE_PARTITION := '20221231';
  KEY_COLUMN_PART_NAME := 'EVENT_DATE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'FT_CXD_RECOUVREMENT';
  PART_PARTITION_NAME := 'FT_CXD_RECOUVREMENT_';
  PART_TYPE_PERIODE := 'JOUR';
  PART_RETENTION := 1000;
  PART_TBS_CIBLE :=  'TAB_P_MON_Jour_16M';
  PART_GARDER_01_DU_MOIS := 'NON';
  PART_PCT_FREE := 0;
  PART_COMPRESSION := 'COMPRESS';
  PART_ROTATION_ACTIVE := 'OUI';
  PART_FORMAT := 'yyyymmdd';
  MON.CREATE_PARTITIONED_TABLE ( SAMPLE_TABLE, MIN_DATE_PARTITION, MAX_DATE_PARTITION, KEY_COLUMN_PART_NAME, KEY_COLUMN_PART_TYPE, PART_OWNER, PART_TABLE_NAME, PART_PARTITION_NAME, PART_TYPE_PERIODE, PART_RETENTION, PART_TBS_CIBLE, PART_GARDER_01_DU_MOIS, PART_PCT_FREE, PART_COMPRESSION, PART_ROTATION_ACTIVE, PART_FORMAT );
  COMMIT; 
END;
