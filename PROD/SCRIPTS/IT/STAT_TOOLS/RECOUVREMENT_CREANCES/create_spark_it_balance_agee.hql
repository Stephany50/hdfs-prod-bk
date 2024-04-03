CREATE TABLE CDR.SPARK_IT_BALANCE_AGEE (
ORIGINAL_FILE_NAME VARCHAR(150),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
CODE_CLIENT   INT,
ACCOUNT_NUMBER  VARCHAR(100),
ACCOUNT_NAME   VARCHAR(255),
CATEG          VARCHAR(50),
PAYMENT_TERM   INT,
STATUT         VARCHAR(50),
NOM            VARCHAR(255),
BALANCE        FLOAT,
DATE_DERNIERE_FACTURE    VARCHAR(100),
0JRS         FLOAT,
30JRS         FLOAT,
60JRS         FLOAT,
90JRS         FLOAT,
120JRS        FLOAT,
150JRS        FLOAT,
180JRS        FLOAT,
360JRS        FLOAT,
720JRS        FLOAT,
1080JRS       FLOAT,
1440JRS       FLOAT,
1800JRS       FLOAT,
PLUS_1800JRS        FLOAT,
INSERT_DATE TIMESTAMP,
ORIGINAL_FILE_DATE    DATE
)
PARTITIONED BY (AS_OF_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ("parquet.compress"="SNAPPY")


CREATE TABLE TMP.SQ_SPARK_IT_BALANCE_AGEE (
    original_file_name         VARCHAR(150),
    original_file_size         INT,
    original_file_line_count   INT,
    code_client                INT,
    account_number             VARCHAR(100),
    account_name               VARCHAR(255),
    categ                      VARCHAR(50),
    payment_term               INT,
    statut                     VARCHAR(50),
    nom                        VARCHAR(255),
    balance                    FLOAT,
    date_derniere_facture      VARCHAR(100),
    0jrs                       FLOAT,
    30jrs                      FLOAT,
    60jrs                      FLOAT,
    90jrs                      FLOAT,
    120jrs                     FLOAT,
    150jrs                     FLOAT,
    180jrs                     FLOAT,
    360jrs                     FLOAT,
    720jrs                     FLOAT,
    1080jrs                    FLOAT,
    1440jrs                    FLOAT,
    1800jrs                    FLOAT,
    plus_1800jrs               FLOAT,
    insert_date                TIMESTAMP,
    original_file_date         DATE,
    as_of_date                 DATE
);



CREATE TABLE TMP.SQ_SPARK_IT_BALANCE_AGEE (
    original_file_name         VARCHAR(150),
    original_file_size         INT,
    original_file_line_count   INT,
    code_client                INT,
    account_number             VARCHAR(100),
    account_name               VARCHAR(255),
    categ                      VARCHAR(50),
    payment_term               INT,
    statut                     VARCHAR(50),
    nom                        VARCHAR(255),
    balance                    FLOAT,
    date_derniere_facture      VARCHAR(100),
    0jrs                       FLOAT,
    30jrs                      FLOAT,
    60jrs                      FLOAT,
    90jrs                      FLOAT,
    120jrs                     FLOAT,
    150jrs                     FLOAT,
    180jrs                     FLOAT,
    360jrs                     FLOAT,
    720jrs                     FLOAT,
    1080jrs                    FLOAT,
    1440jrs                    FLOAT,
    1800jrs                    FLOAT,
    plus_1800jrs               FLOAT,
    insert_date                TIMESTAMP,
    original_file_date         DATE,
    as_of_date                 DATE
);



create table CDR.SQ_IT_BALANCE_AGEE(
    original_file_name         VARCHAR(150),
    original_file_size         INT,
    original_file_line_count   INT,
    code_client                INT,
    account_number             VARCHAR(100),
    account_name               VARCHAR(255),
    categ                      VARCHAR(50),
    payment_term               INT,
    statut                     VARCHAR(50),
    nom                        VARCHAR(255),
    balance                    FLOAT,
    date_derniere_facture      VARCHAR(100),
    "0jrs"                     FLOAT,
    "30jrs"                    FLOAT,
    "60jrs"                    FLOAT,
    "90jrs"                    FLOAT,
    "120jrs"                   FLOAT,
    "150jrs"                   FLOAT,
    "180jrs"                   FLOAT,
    "360jrs"                   FLOAT,
    "720jrs"                   FLOAT,
    "1080jrs"                  FLOAT,
    "1440jrs"                  FLOAT,
    "1800jrs"                  FLOAT,
    plus_1800jrs               FLOAT,
    insert_date                TIMESTAMP,
    original_file_date         DATE,
    as_of_date                 DATE
);  