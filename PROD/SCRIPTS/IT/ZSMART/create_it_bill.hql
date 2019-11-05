CREATE  TABLE CDR.IT_BILL
(

	BILL_ID VARCHAR(200),
	Invoice_number VARCHAR(200),
	CUST_ID VARCHAR(200),
	Bill_date timestamp,
	bill_amount VARCHAR(200),
	remaining_amount VARCHAR(200),
	ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    INSERT_DATE timestamp

)
PARTITIONED BY (original_file_date DATE)
CLUSTERED BY(BILL_ID) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");