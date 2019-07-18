CREATE TABLE IF NOT EXISTS MON.FT_PDM_TRANSACTION (
MSISDN VARCHAR(50) ,
OCM_OUT_CALL BIGINT ,
DATE_OCM_OUT_CALL DATE ,
OCM_OUT_SMS BIGINT ,
DATE_OCM_OUT_SMS DATE ,
OCM_IN_CALL BIGINT ,
DATE_OCM_IN_CALL DATE ,
OCM_IN_SMS BIGINT ,
DATE_OCM_IN_SMS DATE ,
MTN_OUT_CALL BIGINT ,
DATE_MTN_OUT_CALL DATE ,
MTN_OUT_SMS BIGINT ,
DATE_MTN_OUT_SMS DATE ,
MTN_IN_CALL BIGINT ,
DATE_MTN_IN_CALL DATE ,
MTN_IN_SMS BIGINT ,
DATE_MTN_IN_SMS DATE ,
CAMTEL_OUT_CALL BIGINT ,
DATE_CAMTEL_OUT_CALL DATE ,
CAMTEL_OUT_SMS BIGINT ,
DATE_CAMTEL_OUT_SMS DATE ,
CAMTEL_IN_CALL BIGINT ,
DATE_CAMTEL_IN_CALL DATE ,
CAMTEL_IN_SMS BIGINT ,
DATE_CAMTEL_IN_SMS DATE ,
VIETTEL_OUT_CALL BIGINT ,
DATE_VIETTEL_OUT_CALL DATE ,
VIETTEL_OUT_SMS BIGINT ,
DATE_VIETTEL_OUT_SMS DATE ,
VIETTEL_IN_CALL BIGINT ,
DATE_VIETTEL_IN_CALL DATE ,
VIETTEL_IN_SMS BIGINT ,
DATE_VIETTEL_IN_SMS DATE ,
INTER_OUT_CALL BIGINT ,
DATE_INTER_OUT_CALL DATE ,
INTER_OUT_SMS BIGINT ,
DATE_INTER_OUT_SMS DATE ,
INTER_IN_CALL BIGINT ,
DATE_INTER_IN_CALL DATE ,
INTER_IN_SMS BIGINT ,
DATE_INTER_IN_SMS DATE ,
ROAM_OUT_CALL BIGINT ,
DATE_ROAM_OUT_CALL DATE ,
ROAM_OUT_SMS BIGINT ,
DATE_ROAM_OUT_SMS DATE ,
ROAM_IN_CALL BIGINT ,
DATE_ROAM_IN_CALL DATE ,
ROAM_IN_SMS BIGINT ,
DATE_ROAM_IN_SMS DATE ,
INSERT_DATE TIMESTAMP
) COMMENT 'Table de Transactions pour PDM obtenus a partir du MSC'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
;

CREATE TABLE IF NOT EXISTS MON.FT_PDM_TRANSACTION_OTHER (
MSISDN VARCHAR(50) ,
OCM_OUT_CALL BIGINT ,
DATE_OCM_OUT_CALL DATE ,
OCM_OUT_SMS BIGINT ,
DATE_OCM_OUT_SMS DATE ,
OCM_IN_CALL BIGINT ,
DATE_OCM_IN_CALL DATE ,
OCM_IN_SMS BIGINT ,
DATE_OCM_IN_SMS DATE ,
MTN_OUT_CALL BIGINT ,
DATE_MTN_OUT_CALL DATE ,
MTN_OUT_SMS BIGINT ,
DATE_MTN_OUT_SMS DATE ,
MTN_IN_CALL BIGINT ,
DATE_MTN_IN_CALL DATE ,
MTN_IN_SMS BIGINT ,
DATE_MTN_IN_SMS DATE ,
CAMTEL_OUT_CALL BIGINT ,
DATE_CAMTEL_OUT_CALL DATE ,
CAMTEL_OUT_SMS BIGINT ,
DATE_CAMTEL_OUT_SMS DATE ,
CAMTEL_IN_CALL BIGINT ,
DATE_CAMTEL_IN_CALL DATE ,
CAMTEL_IN_SMS BIGINT ,
DATE_CAMTEL_IN_SMS DATE ,
VIETTEL_OUT_CALL BIGINT ,
DATE_VIETTEL_OUT_CALL DATE ,
VIETTEL_OUT_SMS BIGINT ,
DATE_VIETTEL_OUT_SMS DATE ,
VIETTEL_IN_CALL BIGINT ,
DATE_VIETTEL_IN_CALL DATE ,
VIETTEL_IN_SMS BIGINT ,
DATE_VIETTEL_IN_SMS DATE ,
INTER_OUT_CALL BIGINT ,
DATE_INTER_OUT_CALL DATE ,
INTER_OUT_SMS BIGINT ,
DATE_INTER_OUT_SMS DATE ,
INTER_IN_CALL BIGINT ,
DATE_INTER_IN_CALL DATE ,
INTER_IN_SMS BIGINT ,
DATE_INTER_IN_SMS DATE ,
ROAM_OUT_CALL BIGINT ,
DATE_ROAM_OUT_CALL DATE ,
ROAM_OUT_SMS BIGINT ,
DATE_ROAM_OUT_SMS DATE ,
ROAM_IN_CALL BIGINT ,
DATE_ROAM_IN_CALL DATE ,
ROAM_IN_SMS BIGINT ,
DATE_ROAM_IN_SMS DATE ,
INSERT_DATE TIMESTAMP
) COMMENT 'Table de Transactions pour PDM obtenus a partir du MSC'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
;
