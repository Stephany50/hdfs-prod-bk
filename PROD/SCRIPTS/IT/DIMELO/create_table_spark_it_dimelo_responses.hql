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

satisfaction_answered_at,satisfaction_response_id,satisfaction_sent_at,satisfaction_survey_id,content_thread_id,intervention_id,intervention_categories,user_id,user_name,identity_id,identity_name,main_indicator,source_id,source_type,source_name,id,survey_id,survey_type,question_1,answer_1,question_1_foreign_id,answer_1_value,question_2,answer_2,question_2_foreign_id,answer_2_value,question_3,answer_3,question_3_foreign_id,answer_3_value,question_4,answer_4,question_4_foreign_id,answer_4_value,question_5,answer_5,question_5_foreign_id,answer_5_value,question_6,answer_6,question_6_foreign_id,answer_6_value,question_7,answer_7,question_7_foreign_id,answer_7_value,question_8,answer_8,question_8_foreign_id,answer_8_value,question_9,answer_9,question_9_foreign_id,answer_9_value,question_10,answer_10,question_10_foreign_id,answer_10_value,question_11,answer_11,question_11_foreign_id,answer_11_value,question_12,answer_12,question_12_foreign_id,answer_12_value,question_13,answer_13,question_13_foreign_id,answer_13_value,question_14,answer_14,question_14_foreign_id,answer_14_value
2022-06-07T03:26:20+01:00,63950,2022-06-03T10:00:42+01:00,90223828,62994654bd0e5442778852db,629946563b4e8d7f414c3306,"ZZZ - AUTRE MOTIF, Webconseiller_N1, neutre",6204c5ec5bd5442ffb85e0e1,Livia Louise BAOMBE,62994654bd0e5442778852d4,JEAN PIERRE MOUZONG ASSALA,5,59843cc57a8beb359d59d8cd,Engage Messaging,My Orange,629eb97a7133688d99168acd,5e789fcb3298c96652b5306a,alchemer,What is your global satisfaction with the quality of service provided to you?,Very satisfactory,24,5,What is the reason for your dissatisfaction?,"",3,"",Could you explain please?,"",23,"",A quel sujet nous avez-vous contacté?,Vous informer/ Acheter/ souscrire à une offre ou un service,25,Vous informer/ Acheter/ souscrire à une offre ou un service,Is your request resolved,"No, my request is still not solved",9,"No, my request is still not solved",How easy was it to get a resolution to your request?,"",14,"","We are sorry! For you, what is the main difficulty encountered in resolving your request?",Besoin d'une fille pour baisé,26,Besoin d'une fille pour baisé,Waiting times before being received were reasonable,Agree,5,Agree,My request was processed within an acceptable time frame,Agree,6,Agree,I have been informed at every stage of the processing of my request,Fully agree,7,Fully agree,The answers and solutions communicated were perfectly adapted to my needs,Fully agree,8,Fully agree,"On a scale of 1 to 10, to what extent would you recommend Orange digital customer service to those around you?",10,29,10,"Following this survey, do you allow us to contact you again if necessary?",Yes,27,Yes,Could you please give us your phone number?,699761104,28,699761104
2022-06-07T06:07:39+01:00,63951,2022-05-08T15:00:28+01:00,90223828,6277a8b27133686a539052cd,6277a8b33298c90620d9e8bb,"E07 - Identification, Webconseiller_N1, neutre",6204c5c15bd5443818c417ee,Cybell Ainslynn NDOUMBE,611ac050bd0e54906c0e94fd,OUSSEINI AMINATOU SANI,4,59843cc57a8beb359d59d8cd,Engage Messaging,My Orange,629ee004713368893f282dfb,5e789fcb3298c96652b5306a,alchemer,What is your global satisfaction with the quality of service provided to you?,Satisfactory,24,4,What is the reason for your dissatisfaction?,"",3,"",Could you explain please?,"",23,"",A quel sujet nous avez-vous contacté?,"Vous désinscrire / Résilier une offre , une service , un abonnement.",25,"Vous désinscrire / Résilier une offre , une service , un abonnement.",Is your request resolved,Yes after several reminders,9,Yes after several reminders,How easy was it to get a resolution to your request?,Difficult,14,Difficult,"We are sorry! For you, what is the main difficulty encountered in resolving your request?",Les réponses tardives,26,Les réponses tardives,Waiting times before being received were reasonable,Agree,5,Agree,My request was processed within an acceptable time frame,Disagree,6,Disagree,I have been informed at every stage of the processing of my request,Agree,7,Agree,The answers and solutions communicated were perfectly adapted to my needs,Agree,8,Agree,"On a scale of 1 to 10, to what extent would you recommend Orange digital customer service to those around you?",8,29,8,"Following this survey, do you allow us to contact you again if necessary?",Yes,27,Yes,Could you please give us your phone number?,693489824,28,693489824