CREATE  TABLE  MON.SPARK_FT_OM_BICEC_TRANSACTION  (
		AGE  VARCHAR(90),  
		DEV  VARCHAR(90),  
		CHA  VARCHAR(90),  
		NCP  VARCHAR(90),  
		SUF  VARCHAR(5),  
		OPE  VARCHAR(20),  
		UTI  VARCHAR(5),  
		CLC  VARCHAR(90),  
		DCO  VARCHAR(10),  
		DVA  VARCHAR(10),  
		MON  DECIMAL(17,2),  
		SEN  VARCHAR(90),  
		LIB  VARCHAR(30),  
		PIE  VARCHAR(11),  
		MAR  VARCHAR(5),  
		AGSA VARCHAR(90),  
		AGEM VARCHAR(90),  
		AGDE VARCHAR(90),  
		DEVC VARCHAR(90),  
		MCTV  DECIMAL(17,2),  
		PIEO  VARCHAR(11),   
		USER_ID VARCHAR(50),  
		REMARK  VARCHAR(200),
		INSERT_DATE  TIMESTAMP 
      )  COMMENT  'FT_OM_BICEC_TRANSACTION'
PARTITIONED  BY  (EVENT_DATE  DATE)
 STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')