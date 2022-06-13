--***********************************************************---
------------ IT Table- IT_DIMELO_MESSAGES  -------------------
---- NKONGYUM PROSPER AKWO
---***********************************************************---,

        
CREATE TABLE CDR.SPARK_IT_DIMELO_MESSAGES (
	CREATED_AT  TIMESTAMP,
	updated_at  TIMESTAMP,
	source_id  VARCHAR(1000),
	source_type  VARCHAR(1000),
	source_name  VARCHAR(1000),
	content_thread_id  VARCHAR(1000),
	type VARCHAR(1000),
	id  VARCHAR(1000),
	private_message VARCHAR(1000),
	created_from  VARCHAR(50),
	auto_submitted  BIGINT,
	status  VARCHAR(50),
	ignored_from  VARCHAR(50),
	categories  VARCHAR(50),
	intervention_id  VARCHAR(50),
	initial_created_at  TIMESTAMP,
	creator_id  VARCHAR(1000),
    creator_name VARCHAR(1000),
	author_id VARCHAR(50),
	foreign_id VARCHAR(50),
	foreign_categories VARCHAR(50),
	rating VARCHAR(50),
	published VARCHAR(50),
	approval_required VARCHAR(50),
	remotely_deleted VARCHAR(50),
	language VARCHAR(50),
	in_reply_to_id VARCHAR(50),
	in_reply_to_author_id VARCHAR(50),
	attachments_count VARCHAR(50),
	synchronization_status VARCHAR(50),
	synchronization_error VARCHAR(50),
	ORIGINAL_FILE_NAME VARCHAR(50),
	ORIGINAL_FILE_SIZE INT,
	ORIGINAL_FILE_LINE_COUNT INT,
	ORIGINAL_FILE_DATE DATE,
  	INSERT_DATE TIMESTAMP
)
PARTITIONED BY (CALLDATE DATE,FILE_DATE DATE)
CLUSTERED BY(callreference) INTO 128 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
;

,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,

629cecf9bd0e543c94cd0388,Comment,629e89357133680241d32d07,0,synchronizer,0,Replied,"","YYY - Inconnu (à requalifier plus tard), Webconseiller_N1, neutre",629eea2c9090ee49c3f2d8ca,2022-06-07T00:09:41+01:00,"","",629cbc8a7133689e055ed95f,5239540509472156_558937855627663,"","",1,0,0,fr,"","",0,success,""