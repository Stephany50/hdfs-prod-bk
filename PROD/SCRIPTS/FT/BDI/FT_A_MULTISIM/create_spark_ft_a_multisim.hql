--- BY BornToBe
--- NAME : AGG.SPARK_FT_A_MULTISIM
--- DESC : Contient un ensemble de kpi centrés sur les multisim telco, et certains details sur le top de ces multisim.
---       
---FIELDS:
---      * NBR_TOTAL_MULTISIM L'ensemble des multisim a une date donnée.
---      * NBR_TOTAL_NUM_PIECE  nombre de numeros piece distincts.
---      * NBR_TOTAL_NUM_PIECE_VAL : nombre de numeros piece distincts valides.
---      * NBR_TOTAL_NUM_PIECE_INVAL :nombre de numeros piece distincts invalides.
---      * NBR_TOTAL_NOM_PRENOM :nombre de nom prenom distincts.
---      * NBR_TOTAL_NOM_PRE_VAL :nombre de nom prenom distincts valides.
---      * NBR_TOTAL_NOM_PRE_INVAL :nombre de nom prenom distincts invalides.
---      * TYPE_ROWS : les valeurs sont TOTAL / NONE. Il s'agit du tyoe de ligne(aggregat ou detail)
---      * ATTRIBUT : les valeurs possibles sont NONE, NOM, PIECE. S'il s'agit des details sur le nom.

CREATE TABLE AGG.SPARK_FT_A_MULTISIM (
    NBR_TOTAL_MULTISIM              BIGINT,
    NBR_TOTAL_NUM_PIECE             BIGINT,
    NBR_TOTAL_NUM_PIECE_VAL         BIGINT,
    NBR_LIGNE_NUM_PIECE_VAL         BIGINT,
    NBR_TOTAL_NUM_PIECE_INVAL       BIGINT,
    NBR_LIGNE_NUM_PIECE_INVAL       BIGINT,
    NBR_TOTAL_NOM_PRENOM            BIGINT,
    NBR_TOTAL_NOM_PRE_VAL           BIGINT,
    NBR_LIGNE_NOM_PRE_VAL           BIGINT,
    NBR_TOTAL_NOM_PRE_INVAL         BIGINT,
    NBR_LIGNES_NOM_PRE_INVAL        BIGINT,
    TYPE_ROWS                       VARCHAR(10),
    ATTRIBUT                        VARCHAR(20),
    VAL_ATTRIB                      VARCHAR(256),
    COUNT_ATTRIB                    BIGINT,
    INSERT_DATE                     TIMESTAMP
) COMMENT 'SPARK_FT_A_MULTISIM table'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


--Staging table in DataLake
CREATE TABLE TMP.SQ_FT_A_MULTISIM(
    NBR_TOTAL_MULTISIM            decimal(20,3),
    NBR_TOTAL_NUM_PIECE           decimal(20,3),
    NBR_TOTAL_NUM_PIECE_VAL       decimal(20,3),
    NBR_LIGNE_NUM_PIECE_VAL       decimal(20,3),
    NBR_TOTAL_NUM_PIECE_INVAL     decimal(20,3),
    NBR_LIGNE_NUM_PIECE_INVAL     decimal(20,3),
    NBR_TOTAL_NOM_PRENOM          decimal(20,3),
    NBR_TOTAL_NOM_PRE_VAL         decimal(20,3),
    NBR_LIGNE_NOM_PRE_VAL         decimal(20,3),
    NBR_TOTAL_NOM_PRE_INVAL       decimal(20,3),
    NBR_LIGNES_NOM_PRE_INVAL      decimal(20,3),
    TYPE_ROWS                       VARCHAR(10),
    ATTRIBUT                        VARCHAR(20),
    VAL_ATTRIB                     VARCHAR(256),
    COUNT_ATTRIB                   decimal(20,3),
    INSERT_DATE timestamp,
    EVENT_DATE date
);


--Staging table in DWH
CREATE TABLE MON.SQ_FT_A_MULTISIM
(
    NBR_TOTAL_MULTISIM            decimal(20,3),
    NBR_TOTAL_NUM_PIECE           decimal(20,3),
    NBR_TOTAL_NUM_PIECE_VAL       decimal(20,3),
    NBR_LIGNE_NUM_PIECE_VAL       decimal(20,3),
    NBR_TOTAL_NUM_PIECE_INVAL     decimal(20,3),
    NBR_LIGNE_NUM_PIECE_INVAL     decimal(20,3),
    NBR_TOTAL_NOM_PRENOM          decimal(20,3),
    NBR_TOTAL_NOM_PRE_VAL         decimal(20,3),
    NBR_LIGNE_NOM_PRE_VAL         decimal(20,3),
    NBR_TOTAL_NOM_PRE_INVAL       decimal(20,3),
    NBR_LIGNES_NOM_PRE_INVAL      decimal(20,3),
    TYPE_ROWS                       VARCHAR(10 BYTE),
    ATTRIBUT                        VARCHAR(20 BYTE),
    VAL_ATTRIB                     VARCHAR(256 BYTE),
    COUNT_ATTRIB                   decimal(20,3),
    INSERT_DATE timestamp,
    EVENT_DATE date
);

DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'MON.SQ_FT_A_MULTISIM';
  MIN_DATE_PARTITION := '20210701';
  MAX_DATE_PARTITION := '20220701';
  KEY_COLUMN_PART_NAME := 'EVENT_DATE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'FT_A_MULTISIM';
  PART_PARTITION_NAME := 'FT_A_MS_';
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

