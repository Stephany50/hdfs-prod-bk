---NAME : MON.SPARK_FT_CNI_EXPIREES
---DESC : contient l'ensemble des cni expirées pour un jour donné et les cni qui vont expirer dans
---       les "3" prochains jours au plus.
---FIELDS:
---      * PERIODE_EXPIRATION permet de decouper la population en segments tels que 'dans_3_jours_au_plus', '6_mois_et_plus',
---        '3_à_5_mois', 'moins_de_3_mois'.
---      * STATUS  permet de savoir si la cni est expirée ou pas. 'expired' ou 'not_expired' sont ses valeurs.


CREATE TABLE MON.SPARK_FT_CNI_EXPIREES (
    MSISDN            STRING,
    NOM               STRING,
    PRENOM            STRING,
    DATE_NAISSANCE    DATE,
    DATE_EXPIRATION    DATE,
    PERIODE_EXPIRATION VARCHAR(64),
    STATUS VARCHAR(32),
    INSERT_DATE TIMESTAMP
) COMMENT 'SPARK_FT_CNI_EXPIRES table'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');



CREATE TABLE TMP.SPARK_FT_CNI_EXPIREES (
    MSISDN            STRING,
    NOM               STRING,
    PRENOM            STRING,
    DATE_NAISSANCE    DATE,
    DATE_EXPIRATION    DATE,
    PERIODE_EXPIRATION VARCHAR(64),
    STATUS VARCHAR(32),
    INSERT_DATE TIMESTAMP
) COMMENT 'SPARK_FT_CNI_EXPIRES table'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');
