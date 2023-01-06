create external table tt.cbm_input_quickeval(
    msisdn varchar(50),
    temoin varchar(5),
    ipp1 varchar(50),
    ipp2 varchar(50),
    ipp3 varchar(50),
    problematique varchar(50),
    periode1 varchar(50),
    periode2 varchar(50),
    code_campagne varchar(50)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/CBM_INPUT_QUICKEVAL/'
TBLPROPERTIES ('SERIALIZATION.NULL.FORMAT'='')