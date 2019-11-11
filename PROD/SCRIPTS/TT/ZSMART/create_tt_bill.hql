CREATE EXTERNAL TABLE CDR.TT_BILL
(

	BILL_ID VARCHAR(200),
	Invoice_number VARCHAR(200),
	CUST_ID VARCHAR(200),
	Bill_date VARCHAR(200),
	bill_amount VARCHAR(200),
	remaining_amount VARCHAR(200),
	ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT

)
COMMENT 'BILL external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/ZMART/BILL_'
TBLPROPERTIES ('serialization.null.format'='');