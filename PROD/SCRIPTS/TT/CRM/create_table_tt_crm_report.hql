CREATE EXTERNAL TABLE CDR.tt_crm_report (
    ORIGINAL_FILE_NAME VARCHAR(50),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    TicketNumber              VARCHAR(400),
    onc_Numeroappelant       VARCHAR(400),
    Date_Interaction         VARCHAR(400),
    categorie           VARCHAR(400),
    typarticle              VARCHAR(400),
    article               VARCHAR(400),
    motif      VARCHAR(400),
    Agent         VARCHAR(400),
    CUID_AGENT     VARCHAR(400),
    comment_interact    VARCHAR(400),
    description_ticket     VARCHAR(400)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/CRM/REPORT'
TBLPROPERTIES ('serialization.null.format'='')




INSERT_DATE TIMESTAMP