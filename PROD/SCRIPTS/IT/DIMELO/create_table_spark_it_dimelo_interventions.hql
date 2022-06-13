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

created_at,updated_at,closed_at,closed_automatically,source_id,source_type,source_name,content_thread_id,id,status,deferred_at,user_id,user_name,user_replies_count,first_identity_content_id,first_user_reply_id,first_user_reply_at,last_user_reply_at,last_user_reply_in,last_user_reply_in_bh,first_user_reply_in,first_user_reply_in_bh,title,identity_id,identity_name,categories,comments_count,user_reply_in_average,user_reply_in_average_bh,user_reply_in_average_count
2022-06-07T00:02:25+01:00,2022-06-07T11:30:23+01:00,2022-06-07T10:23:08+01:00,"",59843cc57a8beb359d59d8cd,dimelo_messaging,My Orange,629e8780713368893f27f17e,629e87813b4e8d34f30de9ef,Closed,"",6204c5ec5bd5442ffb85e0e1,Livia Louise BAOMBE,14,629e8780713368893f27f17d,629e87819090ee4a1146df89,2022-06-07T00:02:25+01:00,2022-06-07T10:23:08+01:00,37244,12188,2,0,Bsr,629e8780713368893f27f177,ELODIE NADEGE TSINGUIA KAMALA AZEMKOU,"C05 - Réinitialisation code secret, Webconseiller_N1, neutre",0,9533,1288,3
2022-06-07T00:03:28+01:00,2022-06-07T16:40:44+01:00,2022-06-07T16:40:39+01:00,"",59843cc57a8beb359d59d8cd,dimelo_messaging,My Orange,629e87bfbd0e5431b92fbdf7,629e87c05bd54489705daf6c,Closed,"",60e498f83298c90b20e209c9,Rose Félicité SIBENOU TSAMO,25,629e87bfbd0e5431b92fbdf6,629e87c13b4e8d34f30de9fa,2022-06-07T00:03:29+01:00,2022-06-07T16:40:39+01:00,59831,34839,2,0,Bonjour,61b31daebd0e546abc1ec35a,LEONNEL KAMELA,"F01 - Informations commerciales, Webconseiller_N1, neutre",0,4829,725,6"