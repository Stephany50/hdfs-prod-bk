--- BY BornToBe
--- NAME : AGG.SPARK_FT_A_BDI_PERS_MORALE
--- DESC : Contient un ensemble de kpi des personnes morales entreprise, repartie en "3 ections
---       
---FIELDS:
---      * SECTION 1: les kpi qui donnent la vue globale sur l'etat de  la base des personnes morales.
---      * SECTION 2: les kpi qui donnent la vue globale sur l'etat sur les nouvelles acquisions (les champs se terminant par _new).
---      * SECTION 2: les kpi qui donnent la vue sur la repartition des anomalie en fonction du type d'entreprises (les champs se terminant par _recap_an).
CREATE TABLE AGG.SPARK_FT_A_BDI_PERS_MORALE (
nbr_total_pm bigint,
nbr_total_pm_conform bigint,
nbr_total_pm_an bigint,     
nbr_total_ns_an bigint,     
nbr_total_rl_an bigint,     
nbr_total_rccm_an bigint,   
nbr_total_ad_an bigint, 
    
nbr_total_pm_new bigint,    
nbr_total_pm_conform_new bigint,
nbr_total_pm_an_new bigint, 
nbr_total_ns_an_new bigint, 
nbr_total_rl_an_new bigint, 
nbr_total_rccm_an_new bigint,
nbr_total_ad_an_new bigint, 

compte string,  
nbr_total_pm_recap_an bigint, 
nbr_total_ns_recap_an bigint,
nbr_total_rl_recap_an bigint,
nbr_total_rccm_recap_an bigint,
nbr_total_ad_recap_an bigint, 
insert_date timestamp      
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


---STAGGING TABLE IN DATALAKE
CREATE TABLE TMP.SQ_FT_A_BDI_PERS_MORALE (
nbr_total_pm decimal(20,3),
nbr_total_pm_conform decimal(20,3),
nbr_total_pm_an decimal(20,3),     
nbr_total_ns_an decimal(20,3),     
nbr_total_rl_an decimal(20,3),     
nbr_total_rccm_an decimal(20,3),   
nbr_total_ad_an decimal(20,3),     
nbr_total_pm_new decimal(20,3),    
nbr_total_pm_conform_new decimal(20,3),
nbr_total_pm_an_new decimal(20,3), 
nbr_total_ns_an_new decimal(20,3), 
nbr_total_rl_an_new decimal(20,3), 
nbr_total_rccm_an_new decimal(20,3),
nbr_total_ad_an_new decimal(20,3), 
compte VARCHAR(20),  
nbr_total_pm_recap_an decimal(20,3), 
nbr_total_ns_recap_an decimal(20,3),
nbr_total_rl_recap_an decimal(20,3),
nbr_total_rccm_recap_an decimal(20,3),
nbr_total_ad_recap_an decimal(20,3), 
insert_date timestamp,
event_date DATE  
);


---STAGGING TABLE IN DWH
CREATE TABLE MON.SQ_FT_A_BDI_PERS_MORALE (
nbr_total_pm decimal(20,3),
nbr_total_pm_conform decimal(20,3),
nbr_total_pm_an decimal(20,3),     
nbr_total_ns_an decimal(20,3),     
nbr_total_rl_an decimal(20,3),     
nbr_total_rccm_an decimal(20,3),   
nbr_total_ad_an decimal(20,3),     
nbr_total_pm_new decimal(20,3),    
nbr_total_pm_conform_new decimal(20,3),
nbr_total_pm_an_new decimal(20,3), 
nbr_total_ns_an_new decimal(20,3), 
nbr_total_rl_an_new decimal(20,3), 
nbr_total_rccm_an_new decimal(20,3),
nbr_total_ad_an_new decimal(20,3), 
compte VARCHAR(20 BYTE),  
nbr_total_pm_recap_an decimal(20,3), 
nbr_total_ns_recap_an decimal(20,3),
nbr_total_rl_recap_an decimal(20,3),
nbr_total_rccm_recap_an decimal(20,3),
nbr_total_ad_recap_an decimal(20,3), 
insert_date timestamp,
event_date DATE  
);

---TABLE IN DWH
DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'MON.SQ_FT_A_BDI_PERS_MORALE';
  MIN_DATE_PARTITION := '20210701';
  MAX_DATE_PARTITION := '20220701';
  KEY_COLUMN_PART_NAME := 'EVENT_DATE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'FT_A_BDI_PERS_MORALE';
  PART_PARTITION_NAME := 'FT_A_PM_';
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
