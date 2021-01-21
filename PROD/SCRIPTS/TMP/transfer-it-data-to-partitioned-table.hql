    add jar hdfs:///PROD/UDF/hive-udf-1.0.jar;
    create temporary function GENERATE_SEQUENCE_FROM_INTERVALE as 'cm.orange.bigdata.udf.GenerateSequenceFromIntervale';
    SELECT
    SEQUENCE
    FROM (
        SELECT GENERATE_SEQUENCE_FROM_INTERVALE(PREVIOUS+1,INDEX-1)  SEQ FROM (
            SELECT LAG(INDEX, 1) OVER (PARTITION BY MSC_TYPE ORDER BY INDEX) PREVIOUS,INDEX FROM (
                SELECT
                    DISTINCT
                    CAST(SUBSTRING(SOURCE,11,9) AS INT) INDEX,
                    SUBSTRING(SOURCE,5,11) MSC_TYPE
                FROM CDR.SPARK_IT_CRA_MSC_HUAWEI
                    WHERE CALLDATE = '2020-12-31' --AND TO_DATE(ORIGINAL_FILE_DATE)='2020-04-11'
            )A
        )D WHERE INDEX-PREVIOUS >1
    )R
    LATERAL VIEW EXPLODE(SPLIT(SEQ, ',')) SEQUENCE AS SEQUENCE

ll /ria/data/etl/landing/Airtime/SPOOL_EVD_FOR_MONITORING_OCM*
SHOW PARTITIONS mon.SPARK_FT_REFILL;
ll /ria/data/etl/landing/Airtime/SPOOL_ACTIVATIONSEVD_FOR_MONITORING*
SHOW PARTITIONS MON.SPARK_ACTIVATION_ALL_BY_DAY;
ll /ria/data/etl/landing/OM/Distributor_RIA_*

ll /ria/data/etl/landing/OM/SPOOL_TRANSACTIONS_FOR_MONITORING_OCM*
SHOW PARTITIONS CDR.SPARK_IT_OMNY_TRANSACTIONS;
ll /ria/data/etl/landing/OM/SPOOL_COMMISSIONS_FOR_MONITORING_OM_R*
SHOW PARTITIONS CDR.SPARK_IT_OMNY_COMMISSION_DETAILS ;

ll /ria/data/etl/landing/OM/SPOOL_ACTIVATIONSOM_FOR_MONITORING_OCM*
SHOW PARTITIONS cdr.spark_it_om_subscribers ;

ll /ria/data/etl/landing/OM/SPOOL_ACTIVATIONSALLOM_FOR_MONITORING_OCM* | grep -v BIG
SHOW PARTITIONS MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT;



 add jar hdfs:///PROD/UDF/hive-udf-1.0.jar;
    create temporary function GENERATE_SEQUENCE_FROM_INTERVALE as 'cm.orange.bigdata.udf.GenerateSequenceFromIntervale';

SELECT
concat('HUA_DWH-210104-',SEQUENCE)
FROM (
    SELECT GENERATE_SEQUENCE_FROM_INTERVALE(PREVIOUS+1,INDEX-1)  SEQ FROM (
        SELECT LAG(INDEX, 1) OVER (PARTITION BY MSC_TYPE ORDER BY INDEX) PREVIOUS,INDEX FROM (
            SELECT
                DISTINCT
                cast (substring(original_file_name,16,21) as int) INDEX,
                1 MSC_TYPE
            FROM CDR.SPARK_IT_CRA_MSC_HUAWEI
            WHERE file_date = '2021-01-04' --AND TO_DATE(ORIGINAL_FILE_DATE)='2020-04-11'
        )A
    )D WHERE INDEX-PREVIOUS >1
)R
LATERAL VIEW EXPLODE(SPLIT(SEQ, ',')) SEQUENCE AS SEQUENCE
--------------------- ----- ------------
select distinct concat( 'in_pr_adjustment_20200708_',substring(b.original_file_name,27,6),'.csv') from (select * from cdr.SPARK_IT_ZTE_ADJUSTMENT where file_date='2020-12-20' ) a left join (select * from  cdr.SPARK_IT_ZTE_ADJUSTMENT where file_date='2020-12-20' ) b on substring(a.original_file_name,27,6)=substring(b.original_file_name,27,6) where b.original_file_name is null;

gunzip HUA_DWH-200620-200079.gz ; mv HUA_DWH-200620-200079 /data/input/platine/msc/
gunzip HUA_DWH-200620-200081.gz ; mv HUA_DWH-200620-200081 /data/input/platine/msc/
gunzip HUA_DWH-200620-200083.gz ; mv HUA_DWH-200620-200083 /data/input/platine/msc/
gunzip HUA_DWH-200620-200085.gz ; mv HUA_DWH-200620-200085 /data/input/platine/msc/
gunzip HUA_DWH-200620-200095.gz ; mv HUA_DWH-200620-200095 /data/input/platine/msc/
gunzip HUA_DWH-200620-200106.gz ; mv HUA_DWH-200620-200106 /data/input/platine/msc/
gunzip HUA_DWH-200620-200109.gz ; mv HUA_DWH-200620-200109 /data/input/platine/msc/
gunzip HUA_DWH-200620-200111.gz ; mv HUA_DWH-200620-200111 /data/input/platine/msc/
gunzip HUA_DWH-200620-200113.gz ; mv HUA_DWH-200620-200113 /data/input/platine/msc/
gunzip HUA_DWH-200620-200115.gz ; mv HUA_DWH-200620-200115 /data/input/platine/msc/
gunzip HUA_DWH-200620-200117.gz ; mv HUA_DWH-200620-200117 /data/input/platine/msc/
gunzip HUA_DWH-200620-200119.gz ; mv HUA_DWH-200620-200119 /data/input/platine/msc/
gunzip HUA_DWH-200620-200121.gz ; mv HUA_DWH-200620-200121 /data/input/platine/msc/
gunzip HUA_DWH-200620-200123.gz ; mv HUA_DWH-200620-200123 /data/input/platine/msc/
gunzip HUA_DWH-200620-200125.gz ; mv HUA_DWH-200620-200125 /data/input/platine/msc/
gunzip HUA_DWH-200620-200126.gz ; mv HUA_DWH-200620-200126 /data/input/platine/msc/
gunzip HUA_DWH-200620-200129.gz ; mv HUA_DWH-200620-200129 /data/input/platine/msc/
gunzip HUA_DWH-200620-200131.gz ; mv HUA_DWH-200620-200131 /data/input/platine/msc/
gunzip HUA_DWH-200620-200133.gz ; mv HUA_DWH-200620-200133 /data/input/platine/msc/
gunzip HUA_DWH-200620-200135.gz ; mv HUA_DWH-200620-200135 /data/input/platine/msc/
gunzip HUA_DWH-200620-200138.gz ; mv HUA_DWH-200620-200138 /data/input/platine/msc/
gunzip HUA_DWH-200620-200140.gz ; mv HUA_DWH-200620-200140 /data/input/platine/msc/
gunzip HUA_DWH-200620-200141.gz ; mv HUA_DWH-200620-200141 /data/input/platine/msc/
gunzip HUA_DWH-200620-200142.gz ; mv HUA_DWH-200620-200142 /data/input/platine/msc/
gunzip HUA_DWH-200620-200145.gz ; mv HUA_DWH-200620-200145 /data/input/platine/msc/
gunzip HUA_DWH-200620-200147.gz ; mv HUA_DWH-200620-200147 /data/input/platine/msc/
gunzip HUA_DWH-200620-200149.gz ; mv HUA_DWH-200620-200149 /data/input/platine/msc/
gunzip HUA_DWH-200620-200151.gz ; mv HUA_DWH-200620-200151 /data/input/platine/msc/
gunzip HUA_DWH-200620-200152.gz ; mv HUA_DWH-200620-200152 /data/input/platine/msc/
gunzip HUA_DWH-200620-200155.gz ; mv HUA_DWH-200620-200155 /data/input/platine/msc/
gunzip HUA_DWH-200620-200157.gz ; mv HUA_DWH-200620-200157 /data/input/platine/msc/
gunzip HUA_DWH-200620-200159.gz ; mv HUA_DWH-200620-200159 /data/input/platine/msc/
gunzip HUA_DWH-200620-200162.gz ; mv HUA_DWH-200620-200162 /data/input/platine/msc/
gunzip HUA_DWH-200620-200163.gz ; mv HUA_DWH-200620-200163 /data/input/platine/msc/
gunzip HUA_DWH-200620-200165.gz ; mv HUA_DWH-200620-200165 /data/input/platine/msc/
gunzip HUA_DWH-200620-200166.gz ; mv HUA_DWH-200620-200166 /data/input/platine/msc/
gunzip HUA_DWH-200620-200168.gz ; mv HUA_DWH-200620-200168 /data/input/platine/msc/
gunzip HUA_DWH-200620-200171.gz ; mv HUA_DWH-200620-200171 /data/input/platine/msc/
gunzip HUA_DWH-200620-200174.gz ; mv HUA_DWH-200620-200174 /data/input/platine/msc/
gunzip HUA_DWH-200620-200175.gz ; mv HUA_DWH-200620-200175 /data/input/platine/msc/
gunzip HUA_DWH-200620-200177.gz ; mv HUA_DWH-200620-200177 /data/input/platine/msc/
gunzip HUA_DWH-200620-200180.gz ; mv HUA_DWH-200620-200180 /data/input/platine/msc/
gunzip HUA_DWH-200620-200181.gz ; mv HUA_DWH-200620-200181 /data/input/platine/msc/
gunzip HUA_DWH-200620-200184.gz ; mv HUA_DWH-200620-200184 /data/input/platine/msc/
gunzip HUA_DWH-200620-200185.gz ; mv HUA_DWH-200620-200185 /data/input/platine/msc/
gunzip HUA_DWH-200620-200186.gz ; mv HUA_DWH-200620-200186 /data/input/platine/msc/
gunzip HUA_DWH-200620-200189.gz ; mv HUA_DWH-200620-200189 /data/input/platine/msc/
gunzip HUA_DWH-200620-200190.gz ; mv HUA_DWH-200620-200190 /data/input/platine/msc/
gunzip HUA_DWH-200620-200193.gz ; mv HUA_DWH-200620-200193 /data/input/platine/msc/
gunzip HUA_DWH-200620-200195.gz ; mv HUA_DWH-200620-200195 /data/input/platine/msc/
gunzip HUA_DWH-200620-200197.gz ; mv HUA_DWH-200620-200197 /data/input/platine/msc/
gunzip HUA_DWH-200620-200200.gz ; mv HUA_DWH-200620-200200 /data/input/platine/msc/
gunzip HUA_DWH-200620-200201.gz ; mv HUA_DWH-200620-200201 /data/input/platine/msc/
gunzip HUA_DWH-200620-200205.gz ; mv HUA_DWH-200620-200205 /data/input/platine/msc/
gunzip HUA_DWH-200620-200206.gz ; mv HUA_DWH-200620-200206 /data/input/platine/msc/
gunzip HUA_DWH-200620-200207.gz ; mv HUA_DWH-200620-200207 /data/input/platine/msc/
gunzip HUA_DWH-200620-200209.gz ; mv HUA_DWH-200620-200209 /data/input/platine/msc/
gunzip HUA_DWH-200620-200211.gz ; mv HUA_DWH-200620-200211 /data/input/platine/msc/
gunzip HUA_DWH-200620-200212.gz ; mv HUA_DWH-200620-200212 /data/input/platine/msc/
gunzip HUA_DWH-200620-200215.gz ; mv HUA_DWH-200620-200215 /data/input/platine/msc/
gunzip HUA_DWH-200620-200216.gz ; mv HUA_DWH-200620-200216 /data/input/platine/msc/
gunzip HUA_DWH-200620-200221.gz ; mv HUA_DWH-200620-200221 /data/input/platine/msc/
gunzip HUA_DWH-200620-200224.gz ; mv HUA_DWH-200620-200224 /data/input/platine/msc/
gunzip HUA_DWH-200620-200225.gz ; mv HUA_DWH-200620-200225 /data/input/platine/msc/
gunzip HUA_DWH-200620-200227.gz ; mv HUA_DWH-200620-200227 /data/input/platine/msc/
gunzip HUA_DWH-200620-200229.gz ; mv HUA_DWH-200620-200229 /data/input/platine/msc/
gunzip HUA_DWH-200620-200231.gz ; mv HUA_DWH-200620-200231 /data/input/platine/msc/
gunzip HUA_DWH-200620-200232.gz ; mv HUA_DWH-200620-200232 /data/input/platine/msc/
gunzip HUA_DWH-200620-200234.gz ; mv HUA_DWH-200620-200234 /data/input/platine/msc/
gunzip HUA_DWH-200620-200238.gz ; mv HUA_DWH-200620-200238 /data/input/platine/msc/
gunzip HUA_DWH-200620-200240.gz ; mv HUA_DWH-200620-200240 /data/input/platine/msc/
gunzip HUA_DWH-200620-200241.gz ; mv HUA_DWH-200620-200241 /data/input/platine/msc/
gunzip HUA_DWH-200620-200245.gz ; mv HUA_DWH-200620-200245 /data/input/platine/msc/
gunzip HUA_DWH-200620-200246.gz ; mv HUA_DWH-200620-200246 /data/input/platine/msc/
gunzip HUA_DWH-200620-200248.gz ; mv HUA_DWH-200620-200248 /data/input/platine/msc/
gunzip HUA_DWH-200620-200250.gz ; mv HUA_DWH-200620-200250 /data/input/platine/msc/
gunzip HUA_DWH-200620-200251.gz ; mv HUA_DWH-200620-200251 /data/input/platine/msc/
gunzip HUA_DWH-200620-200252.gz ; mv HUA_DWH-200620-200252 /data/input/platine/msc/
gunzip HUA_DWH-200620-200255.gz ; mv HUA_DWH-200620-200255 /data/input/platine/msc/
gunzip HUA_DWH-200620-200257.gz ; mv HUA_DWH-200620-200257 /data/input/platine/msc/
gunzip HUA_DWH-200620-200259.gz ; mv HUA_DWH-200620-200259 /data/input/platine/msc/
gunzip HUA_DWH-200620-200261.gz ; mv HUA_DWH-200620-200261 /data/input/platine/msc/
gunzip HUA_DWH-200620-200264.gz ; mv HUA_DWH-200620-200264 /data/input/platine/msc/
gunzip HUA_DWH-200620-200265.gz ; mv HUA_DWH-200620-200265 /data/input/platine/msc/
gunzip HUA_DWH-200620-200267.gz ; mv HUA_DWH-200620-200267 /data/input/platine/msc/
gunzip HUA_DWH-200620-200270.gz ; mv HUA_DWH-200620-200270 /data/input/platine/msc/
gunzip HUA_DWH-200620-200272.gz ; mv HUA_DWH-200620-200272 /data/input/platine/msc/
gunzip HUA_DWH-200620-200274.gz ; mv HUA_DWH-200620-200274 /data/input/platine/msc/
gunzip HUA_DWH-200620-200276.gz ; mv HUA_DWH-200620-200276 /data/input/platine/msc/
gunzip HUA_DWH-200620-200278.gz ; mv HUA_DWH-200620-200278 /data/input/platine/msc/
gunzip HUA_DWH-200620-200280.gz ; mv HUA_DWH-200620-200280 /data/input/platine/msc/
gunzip HUA_DWH-200620-200281.gz ; mv HUA_DWH-200620-200281 /data/input/platine/msc/
gunzip HUA_DWH-200620-200283.gz ; mv HUA_DWH-200620-200283 /data/input/platine/msc/
gunzip HUA_DWH-200620-200285.gz ; mv HUA_DWH-200620-200285 /data/input/platine/msc/
gunzip HUA_DWH-200620-200288.gz ; mv HUA_DWH-200620-200288 /data/input/platine/msc/
gunzip HUA_DWH-200620-200289.gz ; mv HUA_DWH-200620-200289 /data/input/platine/msc/
gunzip HUA_DWH-200620-200290.gz ; mv HUA_DWH-200620-200290 /data/input/platine/msc/
gunzip HUA_DWH-200620-200291.gz ; mv HUA_DWH-200620-200291 /data/input/platine/msc/
gunzip HUA_DWH-200620-200294.gz ; mv HUA_DWH-200620-200294 /data/input/platine/msc/
gunzip HUA_DWH-200620-200298.gz ; mv HUA_DWH-200620-200298 /data/input/platine/msc/
gunzip HUA_DWH-200620-200300.gz ; mv HUA_DWH-200620-200300 /data/input/platine/msc/
gunzip HUA_DWH-200620-200301.gz ; mv HUA_DWH-200620-200301 /data/input/platine/msc/
gunzip HUA_DWH-200620-200302.gz ; mv HUA_DWH-200620-200302 /data/input/platine/msc/
gunzip HUA_DWH-200620-200306.gz ; mv HUA_DWH-200620-200306 /data/input/platine/msc/
gunzip HUA_DWH-200620-200307.gz ; mv HUA_DWH-200620-200307 /data/input/platine/msc/
gunzip HUA_DWH-200620-200309.gz ; mv HUA_DWH-200620-200309 /data/input/platine/msc/
gunzip HUA_DWH-200620-200311.gz ; mv HUA_DWH-200620-200311 /data/input/platine/msc/
gunzip HUA_DWH-200620-200312.gz ; mv HUA_DWH-200620-200312 /data/input/platine/msc/
gunzip HUA_DWH-200620-200315.gz ; mv HUA_DWH-200620-200315 /data/input/platine/msc/
gunzip HUA_DWH-200620-200317.gz ; mv HUA_DWH-200620-200317 /data/input/platine/msc/
gunzip HUA_DWH-200620-200320.gz ; mv HUA_DWH-200620-200320 /data/input/platine/msc/
gunzip HUA_DWH-200620-200321.gz ; mv HUA_DWH-200620-200321 /data/input/platine/msc/
gunzip HUA_DWH-200620-200322.gz ; mv HUA_DWH-200620-200322 /data/input/platine/msc/
gunzip HUA_DWH-200620-200325.gz ; mv HUA_DWH-200620-200325 /data/input/platine/msc/
gunzip HUA_DWH-200620-200326.gz ; mv HUA_DWH-200620-200326 /data/input/platine/msc/
gunzip HUA_DWH-200620-200329.gz ; mv HUA_DWH-200620-200329 /data/input/platine/msc/
gunzip HUA_DWH-200620-200331.gz ; mv HUA_DWH-200620-200331 /data/input/platine/msc/
gunzip HUA_DWH-200620-200333.gz ; mv HUA_DWH-200620-200333 /data/input/platine/msc/
gunzip HUA_DWH-200620-200335.gz ; mv HUA_DWH-200620-200335 /data/input/platine/msc/
gunzip HUA_DWH-200620-200336.gz ; mv HUA_DWH-200620-200336 /data/input/platine/msc/
gunzip HUA_DWH-200620-200340.gz ; mv HUA_DWH-200620-200340 /data/input/platine/msc/
gunzip HUA_DWH-200620-200341.gz ; mv HUA_DWH-200620-200341 /data/input/platine/msc/
gunzip HUA_DWH-200620-200344.gz ; mv HUA_DWH-200620-200344 /data/input/platine/msc/
gunzip HUA_DWH-200620-200345.gz ; mv HUA_DWH-200620-200345 /data/input/platine/msc/
gunzip HUA_DWH-200620-200347.gz ; mv HUA_DWH-200620-200347 /data/input/platine/msc/
gunzip HUA_DWH-200620-200349.gz ; mv HUA_DWH-200620-200349 /data/input/platine/msc/
gunzip HUA_DWH-200620-200351.gz ; mv HUA_DWH-200620-200351 /data/input/platine/msc/
gunzip HUA_DWH-200620-200353.gz ; mv HUA_DWH-200620-200353 /data/input/platine/msc/
gunzip HUA_DWH-200620-200355.gz ; mv HUA_DWH-200620-200355 /data/input/platine/msc/
gunzip HUA_DWH-200620-200357.gz ; mv HUA_DWH-200620-200357 /data/input/platine/msc/
gunzip HUA_DWH-200620-200358.gz ; mv HUA_DWH-200620-200358 /data/input/platine/msc/
gunzip HUA_DWH-200620-200362.gz ; mv HUA_DWH-200620-200362 /data/input/platine/msc/
gunzip HUA_DWH-200620-200363.gz ; mv HUA_DWH-200620-200363 /data/input/platine/msc/
gunzip HUA_DWH-200620-200365.gz ; mv HUA_DWH-200620-200365 /data/input/platine/msc/
gunzip HUA_DWH-200620-200368.gz ; mv HUA_DWH-200620-200368 /data/input/platine/msc/
gunzip HUA_DWH-200620-200370.gz ; mv HUA_DWH-200620-200370 /data/input/platine/msc/
gunzip HUA_DWH-200620-200371.gz ; mv HUA_DWH-200620-200371 /data/input/platine/msc/
gunzip HUA_DWH-200620-200372.gz ; mv HUA_DWH-200620-200372 /data/input/platine/msc/
gunzip HUA_DWH-200620-200376.gz ; mv HUA_DWH-200620-200376 /data/input/platine/msc/
gunzip HUA_DWH-200620-200379.gz ; mv HUA_DWH-200620-200379 /data/input/platine/msc/
gunzip HUA_DWH-200620-200380.gz ; mv HUA_DWH-200620-200380 /data/input/platine/msc/
gunzip HUA_DWH-200620-200382.gz ; mv HUA_DWH-200620-200382 /data/input/platine/msc/
gunzip HUA_DWH-200620-200383.gz ; mv HUA_DWH-200620-200383 /data/input/platine/msc/
gunzip HUA_DWH-200620-200385.gz ; mv HUA_DWH-200620-200385 /data/input/platine/msc/
gunzip HUA_DWH-200620-200387.gz ; mv HUA_DWH-200620-200387 /data/input/platine/msc/
gunzip HUA_DWH-200620-200389.gz ; mv HUA_DWH-200620-200389 /data/input/platine/msc/
gunzip HUA_DWH-200620-200390.gz ; mv HUA_DWH-200620-200390 /data/input/platine/msc/
gunzip HUA_DWH-200620-200392.gz ; mv HUA_DWH-200620-200392 /data/input/platine/msc/
gunzip HUA_DWH-200620-200396.gz ; mv HUA_DWH-200620-200396 /data/input/platine/msc/
gunzip HUA_DWH-200620-200398.gz ; mv HUA_DWH-200620-200398 /data/input/platine/msc/
gunzip HUA_DWH-200620-200399.gz ; mv HUA_DWH-200620-200399 /data/input/platine/msc/
gunzip HUA_DWH-200620-200400.gz ; mv HUA_DWH-200620-200400 /data/input/platine/msc/
gunzip HUA_DWH-200620-200402.gz ; mv HUA_DWH-200620-200402 /data/input/platine/msc/
gunzip HUA_DWH-200620-200404.gz ; mv HUA_DWH-200620-200404 /data/input/platine/msc/
gunzip HUA_DWH-200620-200407.gz ; mv HUA_DWH-200620-200407 /data/input/platine/msc/
gunzip HUA_DWH-200620-200409.gz ; mv HUA_DWH-200620-200409 /data/input/platine/msc/
gunzip HUA_DWH-200620-200410.gz ; mv HUA_DWH-200620-200410 /data/input/platine/msc/
gunzip HUA_DWH-200620-200413.gz ; mv HUA_DWH-200620-200413 /data/input/platine/msc/
gunzip HUA_DWH-200620-200414.gz ; mv HUA_DWH-200620-200414 /data/input/platine/msc/
gunzip HUA_DWH-200620-200417.gz ; mv HUA_DWH-200620-200417 /data/input/platine/msc/
gunzip HUA_DWH-200620-200419.gz ; mv HUA_DWH-200620-200419 /data/input/platine/msc/
gunzip HUA_DWH-200620-200421.gz ; mv HUA_DWH-200620-200421 /data/input/platine/msc/
gunzip HUA_DWH-200620-200423.gz ; mv HUA_DWH-200620-200423 /data/input/platine/msc/
gunzip HUA_DWH-200620-200424.gz ; mv HUA_DWH-200620-200424 /data/input/platine/msc/
gunzip HUA_DWH-200620-200427.gz ; mv HUA_DWH-200620-200427 /data/input/platine/msc/
gunzip HUA_DWH-200620-200429.gz ; mv HUA_DWH-200620-200429 /data/input/platine/msc/
gunzip HUA_DWH-200620-200431.gz ; mv HUA_DWH-200620-200431 /data/input/platine/msc/
gunzip HUA_DWH-200620-200433.gz ; mv HUA_DWH-200620-200433 /data/input/platine/msc/
gunzip HUA_DWH-200620-200434.gz ; mv HUA_DWH-200620-200434 /data/input/platine/msc/
gunzip HUA_DWH-200620-200438.gz ; mv HUA_DWH-200620-200438 /data/input/platine/msc/
gunzip HUA_DWH-200620-200439.gz ; mv HUA_DWH-200620-200439 /data/input/platine/msc/
gunzip HUA_DWH-200620-200442.gz ; mv HUA_DWH-200620-200442 /data/input/platine/msc/
gunzip HUA_DWH-200620-200444.gz ; mv HUA_DWH-200620-200444 /data/input/platine/msc/
gunzip HUA_DWH-200620-200446.gz ; mv HUA_DWH-200620-200446 /data/input/platine/msc/
gunzip HUA_DWH-200620-200447.gz ; mv HUA_DWH-200620-200447 /data/input/platine/msc/
gunzip HUA_DWH-200620-200448.gz ; mv HUA_DWH-200620-200448 /data/input/platine/msc/
gunzip HUA_DWH-200620-200455.gz ; mv HUA_DWH-200620-200455 /data/input/platine/msc/
gunzip HUA_DWH-200620-200460.gz ; mv HUA_DWH-200620-200460 /data/input/platine/msc/
gunzip HUA_DWH-200620-200462.gz ; mv HUA_DWH-200620-200462 /data/input/platine/msc/
gunzip HUA_DWH-200620-200466.gz ; mv HUA_DWH-200620-200466 /data/input/platine/msc/
gunzip HUA_DWH-200620-200469.gz ; mv HUA_DWH-200620-200469 /data/input/platine/msc/
gunzip HUA_DWH-200620-200474.gz ; mv HUA_DWH-200620-200474 /data/input/platine/msc/
gunzip HUA_DWH-200620-200477.gz ; mv HUA_DWH-200620-200477 /data/input/platine/msc/
gunzip HUA_DWH-200620-200478.gz ; mv HUA_DWH-200620-200478 /data/input/platine/msc/
gunzip HUA_DWH-200620-200488.gz ; mv HUA_DWH-200620-200488 /data/input/platine/msc/
gunzip HUA_DWH-200620-200492.gz ; mv HUA_DWH-200620-200492 /data/input/platine/msc/
gunzip HUA_DWH-200620-200493.gz ; mv HUA_DWH-200620-200493 /data/input/platine/msc/
gunzip HUA_DWH-200620-200499.gz ; mv HUA_DWH-200620-200499 /data/input/platine/msc/
gunzip HUA_DWH-200620-200503.gz ; mv HUA_DWH-200620-200503 /data/input/platine/msc/
gunzip HUA_DWH-200620-200515.gz ; mv HUA_DWH-200620-200515 /data/input/platine/msc/
gunzip HUA_DWH-200620-200516.gz ; mv HUA_DWH-200620-200516 /data/input/platine/msc/
gunzip HUA_DWH-200620-200522.gz ; mv HUA_DWH-200620-200522 /data/input/platine/msc/
gunzip HUA_DWH-200620-200524.gz ; mv HUA_DWH-200620-200524 /data/input/platine/msc/
gunzip HUA_DWH-200620-200525.gz ; mv HUA_DWH-200620-200525 /data/input/platine/msc/
gunzip HUA_DWH-200620-200526.gz ; mv HUA_DWH-200620-200526 /data/input/platine/msc/
gunzip HUA_DWH-200620-200530.gz ; mv HUA_DWH-200620-200530 /data/input/platine/msc/
gunzip HUA_DWH-200620-200532.gz ; mv HUA_DWH-200620-200532 /data/input/platine/msc/
gunzip HUA_DWH-200620-200540.gz ; mv HUA_DWH-200620-200540 /data/input/platine/msc/
gunzip HUA_DWH-200620-200548.gz ; mv HUA_DWH-200620-200548 /data/input/platine/msc/
gunzip HUA_DWH-200620-200550.gz ; mv HUA_DWH-200620-200550 /data/input/platine/msc/
gunzip HUA_DWH-200620-200553.gz ; mv HUA_DWH-200620-200553 /data/input/platine/msc/
gunzip HUA_DWH-200620-200558.gz ; mv HUA_DWH-200620-200558 /data/input/platine/msc/
gunzip HUA_DWH-200620-200559.gz ; mv HUA_DWH-200620-200559 /data/input/platine/msc/
gunzip HUA_DWH-200620-200560.gz ; mv HUA_DWH-200620-200560 /data/input/platine/msc/
gunzip HUA_DWH-200620-200571.gz ; mv HUA_DWH-200620-200571 /data/input/platine/msc/
gunzip HUA_DWH-200620-200572.gz ; mv HUA_DWH-200620-200572 /data/input/platine/msc/
gunzip HUA_DWH-200620-200577.gz ; mv HUA_DWH-200620-200577 /data/input/platine/msc/
gunzip HUA_DWH-200620-200578.gz ; mv HUA_DWH-200620-200578 /data/input/platine/msc/
gunzip HUA_DWH-200620-200584.gz ; mv HUA_DWH-200620-200584 /data/input/platine/msc/
gunzip HUA_DWH-200620-200585.gz ; mv HUA_DWH-200620-200585 /data/input/platine/msc/
gunzip HUA_DWH-200620-200590.gz ; mv HUA_DWH-200620-200590 /data/input/platine/msc/
gunzip HUA_DWH-200620-200593.gz ; mv HUA_DWH-200620-200593 /data/input/platine/msc/
gunzip HUA_DWH-200620-200599.gz ; mv HUA_DWH-200620-200599 /data/input/platine/msc/
gunzip HUA_DWH-200620-200603.gz ; mv HUA_DWH-200620-200603 /data/input/platine/msc/
gunzip HUA_DWH-200620-200605.gz ; mv HUA_DWH-200620-200605 /data/input/platine/msc/
gunzip HUA_DWH-200620-200608.gz ; mv HUA_DWH-200620-200608 /data/input/platine/msc/
gunzip HUA_DWH-200620-200612.gz ; mv HUA_DWH-200620-200612 /data/input/platine/msc/
gunzip HUA_DWH-200620-200617.gz ; mv HUA_DWH-200620-200617 /data/input/platine/msc/
gunzip HUA_DWH-200620-200622.gz ; mv HUA_DWH-200620-200622 /data/input/platine/msc/
gunzip HUA_DWH-200620-200625.gz ; mv HUA_DWH-200620-200625 /data/input/platine/msc/
gunzip HUA_DWH-200620-200632.gz ; mv HUA_DWH-200620-200632 /data/input/platine/msc/
gunzip HUA_DWH-200620-200636.gz ; mv HUA_DWH-200620-200636 /data/input/platine/msc/
gunzip HUA_DWH-200620-200638.gz ; mv HUA_DWH-200620-200638 /data/input/platine/msc/
gunzip HUA_DWH-200620-200640.gz ; mv HUA_DWH-200620-200640 /data/input/platine/msc/
gunzip HUA_DWH-200620-200649.gz ; mv HUA_DWH-200620-200649 /data/input/platine/msc/
gunzip HUA_DWH-200620-200653.gz ; mv HUA_DWH-200620-200653 /data/input/platine/msc/
gunzip HUA_DWH-200620-200656.gz ; mv HUA_DWH-200620-200656 /data/input/platine/msc/
gunzip HUA_DWH-200620-200660.gz ; mv HUA_DWH-200620-200660 /data/input/platine/msc/
gunzip HUA_DWH-200620-200666.gz ; mv HUA_DWH-200620-200666 /data/input/platine/msc/
gunzip HUA_DWH-200620-200670.gz ; mv HUA_DWH-200620-200670 /data/input/platine/msc/
gunzip HUA_DWH-200620-200674.gz ; mv HUA_DWH-200620-200674 /data/input/platine/msc/
gunzip HUA_DWH-200620-200679.gz ; mv HUA_DWH-200620-200679 /data/input/platine/msc/
gunzip HUA_DWH-200620-200682.gz ; mv HUA_DWH-200620-200682 /data/input/platine/msc/
gunzip HUA_DWH-200620-200685.gz ; mv HUA_DWH-200620-200685 /data/input/platine/msc/
gunzip HUA_DWH-200620-200688.gz ; mv HUA_DWH-200620-200688 /data/input/platine/msc/
gunzip HUA_DWH-200620-200693.gz ; mv HUA_DWH-200620-200693 /data/input/platine/msc/
gunzip HUA_DWH-200620-200697.gz ; mv HUA_DWH-200620-200697 /data/input/platine/msc/
gunzip HUA_DWH-200620-200701.gz ; mv HUA_DWH-200620-200701 /data/input/platine/msc/
gunzip HUA_DWH-200620-200709.gz ; mv HUA_DWH-200620-200709 /data/input/platine/msc/
gunzip HUA_DWH-200620-200710.gz ; mv HUA_DWH-200620-200710 /data/input/platine/msc/
gunzip HUA_DWH-200620-200714.gz ; mv HUA_DWH-200620-200714 /data/input/platine/msc/
gunzip HUA_DWH-200620-200721.gz ; mv HUA_DWH-200620-200721 /data/input/platine/msc/
gunzip HUA_DWH-200620-200724.gz ; mv HUA_DWH-200620-200724 /data/input/platine/msc/
gunzip HUA_DWH-200620-200726.gz ; mv HUA_DWH-200620-200726 /data/input/platine/msc/
gunzip HUA_DWH-200620-200728.gz ; mv HUA_DWH-200620-200728 /data/input/platine/msc/
gunzip HUA_DWH-200620-200733.gz ; mv HUA_DWH-200620-200733 /data/input/platine/msc/
gunzip HUA_DWH-200620-200734.gz ; mv HUA_DWH-200620-200734 /data/input/platine/msc/
gunzip HUA_DWH-200620-200741.gz ; mv HUA_DWH-200620-200741 /data/input/platine/msc/
gunzip HUA_DWH-200620-200743.gz ; mv HUA_DWH-200620-200743 /data/input/platine/msc/
gunzip HUA_DWH-200620-200751.gz ; mv HUA_DWH-200620-200751 /data/input/platine/msc/
gunzip HUA_DWH-200620-200754.gz ; mv HUA_DWH-200620-200754 /data/input/platine/msc/
gunzip HUA_DWH-200620-200757.gz ; mv HUA_DWH-200620-200757 /data/input/platine/msc/
gunzip HUA_DWH-200620-200761.gz ; mv HUA_DWH-200620-200761 /data/input/platine/msc/
gunzip HUA_DWH-200620-200767.gz ; mv HUA_DWH-200620-200767 /data/input/platine/msc/
gunzip HUA_DWH-200620-200769.gz ; mv HUA_DWH-200620-200769 /data/input/platine/msc/
gunzip HUA_DWH-200620-200774.gz ; mv HUA_DWH-200620-200774 /data/input/platine/msc/
gunzip HUA_DWH-200620-200778.gz ; mv HUA_DWH-200620-200778 /data/input/platine/msc/
gunzip HUA_DWH-200620-200782.gz ; mv HUA_DWH-200620-200782 /data/input/platine/msc/
gunzip HUA_DWH-200620-200786.gz ; mv HUA_DWH-200620-200786 /data/input/platine/msc/
gunzip HUA_DWH-200620-200790.gz ; mv HUA_DWH-200620-200790 /data/input/platine/msc/
gunzip HUA_DWH-200620-200792.gz ; mv HUA_DWH-200620-200792 /data/input/platine/msc/
gunzip HUA_DWH-200620-200796.gz ; mv HUA_DWH-200620-200796 /data/input/platine/msc/
gunzip HUA_DWH-200620-200801.gz ; mv HUA_DWH-200620-200801 /data/input/platine/msc/
gunzip HUA_DWH-200620-200805.gz ; mv HUA_DWH-200620-200805 /data/input/platine/msc/
gunzip HUA_DWH-200620-200807.gz ; mv HUA_DWH-200620-200807 /data/input/platine/msc/
gunzip HUA_DWH-200620-200812.gz ; mv HUA_DWH-200620-200812 /data/input/platine/msc/
gunzip HUA_DWH-200620-200814.gz ; mv HUA_DWH-200620-200814 /data/input/platine/msc/
gunzip HUA_DWH-200620-200817.gz ; mv HUA_DWH-200620-200817 /data/input/platine/msc/
gunzip HUA_DWH-200620-200822.gz ; mv HUA_DWH-200620-200822 /data/input/platine/msc/
gunzip HUA_DWH-200620-200825.gz ; mv HUA_DWH-200620-200825 /data/input/platine/msc/
gunzip HUA_DWH-200620-200833.gz ; mv HUA_DWH-200620-200833 /data/input/platine/msc/
gunzip HUA_DWH-200620-200835.gz ; mv HUA_DWH-200620-200835 /data/input/platine/msc/
gunzip HUA_DWH-200620-200837.gz ; mv HUA_DWH-200620-200837 /data/input/platine/msc/
gunzip HUA_DWH-200620-200842.gz ; mv HUA_DWH-200620-200842 /data/input/platine/msc/
gunzip HUA_DWH-200620-200850.gz ; mv HUA_DWH-200620-200850 /data/input/platine/msc/
gunzip HUA_DWH-200620-200855.gz ; mv HUA_DWH-200620-200855 /data/input/platine/msc/
gunzip HUA_DWH-200620-200859.gz ; mv HUA_DWH-200620-200859 /data/input/platine/msc/
gunzip HUA_DWH-200620-200860.gz ; mv HUA_DWH-200620-200860 /data/input/platine/msc/
gunzip HUA_DWH-200620-200863.gz ; mv HUA_DWH-200620-200863 /data/input/platine/msc/
gunzip HUA_DWH-200620-200870.gz ; mv HUA_DWH-200620-200870 /data/input/platine/msc/
gunzip HUA_DWH-200620-200871.gz ; mv HUA_DWH-200620-200871 /data/input/platine/msc/
gunzip HUA_DWH-200620-200877.gz ; mv HUA_DWH-200620-200877 /data/input/platine/msc/
gunzip HUA_DWH-200620-200882.gz ; mv HUA_DWH-200620-200882 /data/input/platine/msc/
gunzip HUA_DWH-200620-200884.gz ; mv HUA_DWH-200620-200884 /data/input/platine/msc/
gunzip HUA_DWH-200620-200890.gz ; mv HUA_DWH-200620-200890 /data/input/platine/msc/
gunzip HUA_DWH-200620-200896.gz ; mv HUA_DWH-200620-200896 /data/input/platine/msc/
gunzip HUA_DWH-200620-200899.gz ; mv HUA_DWH-200620-200899 /data/input/platine/msc/
gunzip HUA_DWH-200620-200906.gz ; mv HUA_DWH-200620-200906 /data/input/platine/msc/
gunzip HUA_DWH-200620-200908.gz ; mv HUA_DWH-200620-200908 /data/input/platine/msc/
gunzip HUA_DWH-200620-200913.gz ; mv HUA_DWH-200620-200913 /data/input/platine/msc/
gunzip HUA_DWH-200620-200917.gz ; mv HUA_DWH-200620-200917 /data/input/platine/msc/
gunzip HUA_DWH-200620-200924.gz ; mv HUA_DWH-200620-200924 /data/input/platine/msc/
gunzip HUA_DWH-200620-200927.gz ; mv HUA_DWH-200620-200927 /data/input/platine/msc/
gunzip HUA_DWH-200620-200928.gz ; mv HUA_DWH-200620-200928 /data/input/platine/msc/
gunzip HUA_DWH-200620-200930.gz ; mv HUA_DWH-200620-200930 /data/input/platine/msc/

insert into  tmp.ft_group_subscriber_summary2
select
EVENT_DATE,
PROFILE,
STATUT,
ETAT,
BSCS_COMM_OFFER,
TRANCHE_AGE,
CUST_BILLCYCLE,
CREDIT,
EFFECTIF,
ACTIVATIONS,
RESILIATIONS,
SRC_TABLE,
INSERT_DATE,
PLATFORM_STATUS,
EFFECTIF_CLIENTS_OCM,
DECONNEXIONS,
CONNEXIONS,
RECONNEXIONS,
OPERATOR_CODE
from  mon.spark_ft_group_subscriber_summary where EVENT_DATE>"2020-04-10"
VIETTEL
CAMTEL_FIX

insert into  tmp.ft_commercial_subscrib_summary2
select
DATECODE,
NETWORK_DOMAIN,
NETWORK_TECHNOLOGY,
SUBSCRIBER_CATEGORY,
CUSTOMER_ID,
SUBSCRIBER_TYPE,
COMMERCIAL_OFFER,
ACCOUNT_STATUS,
LOCK_STATUS,
ACTIVATION_MONTH,
CITYZONE,
USAGE_TYPE,
TOTAL_COUNT,
TOTAL_ACTIVATION,
TOTAL_DEACTIVATION,
TOTAL_EXPIRATION,
TOTAL_PROVISIONNED,
TOTAL_MAIN_CREDIT,
TOTAL_PROMO_CREDIT,
TOTAL_SMS_CREDIT,
TOTAL_DATA_CREDIT,
SOURCE,
REFRESH_DATE,
PROFILE_NAME,
PLATFORM_ACCOUNT_STATUS,
PLATFORM_ACTIVATION_MONTH
from mon.spark_ft_commercial_subscrib_summary where datecode>'2020-04-09'

nvl(voix_onnet,0)+nvl(voix_offnet,0)+nvl(voix_inter,0)+nvl(voix_roaming,0)+nvl(sms_onnet,0)+nvl(sms_offnet,0)+nvl(sms_inter,0)+nvl(sms_roaming,0)+nvl(data_bundle,0)+nvl(sva,0)+


insert into  tmp.ft_a_subscriber_summary2
select
DATECODE,
NETWORK_DOMAIN,
NETWORK_TECHNOLOGY,
SUBSCRIBER_CATEGORY,
CUSTOMER_ID,
SUBSCRIBER_TYPE,
COMMERCIAL_OFFER,
ACCOUNT_STATUS,
LOCK_STATUS,
ACTIVATION_MONTH,
CITYZONE,
USAGE_TYPE,
TOTAL_COUNT,
TOTAL_ACTIVATION,
TOTAL_DEACTIVATION,
TOTAL_EXPIRATION,
TOTAL_PROVISIONNED,
TOTAL_MAIN_CREDIT,
TOTAL_PROMO_CREDIT,
TOTAL_SMS_CREDIT,
TOTAL_DATA_CREDIT,
SOURCE,
REFRESH_DATE,
PROFILE_NAME,
PROCESS_NAME
from agg.ft_a_subscriber_summary where datecode>='2020-04-10'





insert into tmp.ft_a_subscription2  select
    TRANSACTION_DATE,
    TRANSACTION_TIME,
    CONTRACT_TYPE,
    OPERATOR_CODE,
    MAIN_USAGE_SERVICE_CODE,
    COMMERCIAL_OFFER,
    PREVIOUS_COMMERCIAL_OFFER,
    SUBS_SERVICE,
    SUBS_BENEFIT_NAME,
    SUBS_CHANNEL,
    SUBS_RELATED_SERVICE,
    SUBS_TOTAL_COUNT,
    SUBS_AMOUNT,
    SOURCE_PLATFORM,
    SOURCE_DATA,
    INSERT_DATE,
    SERVICE_CODE,
    MSISDN_COUNT,
    SUBS_EVENT_RATED_COUNT,
    SUBS_PRICE_UNIT,
    AMOUNT_SVA,
    AMOUNT_VOICE_ONNET,
    AMOUNT_VOICE_OFFNET,
    AMOUNT_VOICE_INTER,
    AMOUNT_VOICE_ROAMING,
    AMOUNT_SMS_ONNET,
    AMOUNT_SMS_OFFNET,
    AMOUNT_SMS_INTER,
    AMOUNT_SMS_ROAMING,
    AMOUNT_DATA,
    COMBO
    FROM AGG.SPARK_FT_A_SUBSCRIPTION WHERE TRANSACTION_DATE >='2020-04-11' ;



insert into CDR.spark_iT_BDI_FLOTTE
select
g.MSISDN  MSISDN,
h.CUSTOMER_ID  CUSTOMER_ID,
h.CONTRACT_ID  CONTRACT_ID,
h.COMPTE_CLIENT  COMPTE_CLIENT,
'M2M'  TYPE_PERSONNE,
null  TYPE_PIECE,
null  NUMERO_PIECE,
null  ID_TYPE_PIECE,
null  NOM_PRENOM,
null  NOM,
null  PRENOM,
null  DATE_NAISSANCE,
null  DATE_EXPIRATION,
null  ADRESSE,
'DOUALA'  VILLE,
'AKWA'  QUARTIER,
h.DATE_SOUSCRIPTION  DATE_SOUSCRIPTION,
h.DATE_ACTIVATION  DATE_ACTIVATION,
h.STATUT  STATUT,
h.RAISON_STATUT  RAISON_STATUT,
h.DATE_CHANGEMENT_STATUT  DATE_CHANGEMENT_STATUT,
null  PLAN_LOCALISATION,
null  CONTRAT_SOUCRIPTION,
null  DISPONIBILITE_SCAN,
null  ACCEPTATION_CGV,
null  TYPE_PIECE_TUTEUR,
null  NUMERO_PIECE_TUTEUR,
null  NOM_TUTEUR,
null  PRENOM_TUTEUR,
null  DATE_NAISSANCE_TUTEUR,
null  DATE_EXPIRATION_TUTEUR,
null  ADRESSE_TUTEUR,
'4.4335'  COMPTE_CLIENT_STRUCTURE,
'FORIS TELECOM'  NOM_STRUCTURE,
'RC/DLA/2008/B/782' NUMERO_REGISTRE_COMMERCE,
'110046646' NUMERO_PIECE_REPRESENTANT_LEGAL,
h.IMEI  IMEI,
null  STATUT_DEROGATION,
h.REGION_ADMINISTRATIVE  REGION_ADMINISTRATIVE,
h.REGION_COMMERCIALE  REGION_COMMERCIALE,
h.SITE_NAME  SITE_NAME,
h.VILLE_SITE  VILLE_SITE,
h.OFFRE_COMMERCIALE  OFFRE_COMMERCIALE,
h.TYPE_CONTRAT  TYPE_CONTRAT,
h.SEGMENTATION  SEGMENTATION,
h.odbIncomingCalls  odbIncomingCalls,
h.odbOutgoingCalls  odbOutgoingCalls,
null  DEROGATION_IDENTIFICATION
    from (
            select e.*
            from (
                select MSISDN,NUMERO_PIECE
                from TMP.TT_BDI3
                where NUMERO_PIECE in  ( select NUMERO_PIECE from (
                SELECT NUMERO_PIECE, COUNT(*) AS NB
                FROM TMP.TT_BDI3
                WHERE (odbIncomingCalls = '0' ANd odbOutgoingCalls = '0') AND
                NUMERO_PIECE NOT LIKE "1122334455%" GROUP BY NUMERO_PIECE HAVING NB > 3
                    ) a
                )
            ) e
        left join
        (
            select msisdn, numero_piece
            from (
                select
                    msisdn, numero_piece,
                    row_number() over( partition by numero_piece order by msisdn ) AS RANG
                from (
                select MSISDN,NUMERO_PIECE
                from TMP.TT_BDI3
                where NUMERO_PIECE in (
                    select NUMERO_PIECE from (
                    SELECT NUMERO_PIECE, COUNT(*) AS NB
                    FROM TMP.TT_BDI3
                    WHERE (odbIncomingCalls = '0' ANd odbOutgoingCalls = '0') AND
                    NUMERO_PIECE NOT LIKE "1122334455%" GROUP BY NUMERO_PIECE HAVING NB > 3
                    ) a
                 )
                ) c
            ) d
            where RANG <= 1
        ) f on substr(trim(e.msisdn),-9,9) = substr(trim(f.msisdn),-9,9) where f.msisdn is null
) g
left join  TMP.tt_bdi3 h on substr(nvl(g.msisdn,'),-9,9) = substr(nvl(h.msisdn,'),-9,9)


select MSISDN,NUMERO_PIECE, count(distinct MSISDN) NB
from TMP.TT_BDI3 group by MSISDN,NUMERO_PIECE
WHERE odbIncomingCalls = '0' ANd odbOutgoingCalls = '0') AND
        NUMERO_PIECE NOT LIKE "1122334455%"

INSERT INTO AGG.SPARK_FT_CBM_AGREGAT
select
site_name,
MULTI,
OPERATEUR,
TENURE,
SEGMENT,
 MOU_OFNET,
MA_VOICE_ONNET,
 MA_VOICE_INTER,
MA_SMS_INTER,
MA_DATA,
MA_GOS_SVA,
MA_SMS_ROAMING,
 MA_SMS_SVA,
MA_VOICE_SVA,
user_voice,
bdle_name,
BDLE_COST,
volume_data,
user_data1,
user_data2 ,
MONTANT,
destination_type,
destination,
traffic_data,
rated_sms_total_count,
in_duration,
rated_duration,
paygo_data_revenue,
user_data,
paygo_voice_revenue,
og_total_call_duration,
data_type,
INSERT_DATE,
event_date
from
(
SELECT
site_name,
MULTI,
OPERATEUR,
TENURE,
SEGMENT,
sum(nvl(MOU_ONNET,0)) as MOU_ONNET , sum(nvl(MOU_OFNET,0)) as MOU_OFNET,
sum(nvl(MOU_INTER,0)) as MOU_INTER,sum(nvl(MA_VOICE_ONNET,0)) as MA_VOICE_ONNET,
sum(nvl(MA_VOICE_OFNET,0)) as MA_VOICE_OFNET,sum(nvl(MA_VOICE_INTER,0)) as MA_VOICE_INTER,
sum(nvl(MA_SMS_OFNET,0)) as MA_SMS_OFNET,sum(nvl(MA_SMS_INTER,0)) as MA_SMS_INTER,
sum(nvl(MA_SMS_ONNET,0)) as MA_SMS_ONNET, sum(nvl(MA_DATA,0)) as MA_DATA,
sum(nvl(MA_VAS,0)) as MA_VAS, sum(nvl(MA_GOS_SVA,0)) as MA_GOS_SVA,
sum(nvl(MA_VOICE_ROAMING,0)) as MA_VOICE_ROAMING, sum(nvl(MA_SMS_ROAMING,0)) as MA_SMS_ROAMING,
sum(nvl(MA_SMS_SVA,0)) as MA_SMS_SVA,
sum(nvl(MA_VOICE_SVA,0)) as MA_VOICE_SVA,
sum(is_user_voice) as user_voice,
null as bdle_name,
null as BDLE_COST,
null as volume_data,
null as user_data1,
null as  user_data2 ,
null as MONTANT,
null as destination_type,
null as destination,
null as traffic_data,
null as rated_sms_total_count,
null as in_duration,
null as rated_duration,
null as  paygo_data_revenue,
null as user_data,
null as paygo_voice_revenue,
null as og_total_call_duration,
'CI' AS  data_type,
CURRENT_TIMESTAMP  INSERT_DATE,
period AS event_date
FROM
(select *, case when (MOU_ONNET+MOU_OFNET+MOU_INTER) > 0 then 1 else 0 end as is_user_voice from mon.SPARK_FT_CBM_CUST_INSIGTH_DAILY
WHERE PERIOD  ='2020-04-05') A
left join
(SELECT EVENT_DATE, msisdn, site_name FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE  ='2020-04-05') B on A.msisdn=B.msisdn and A.PERIOD=B.EVENT_DATE
left join
(SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT FROM DIM.dt_Ref_cbm) C on A.msisdn=C.msisdn
group by A.period, B.site_name, multi, operateur, tenure, segment
UNION ALL
SELECT
site_name,
multi,
operateur,
tenure,
segment,
null as MOU_OFNET,
null as MA_VOICE_ONNET,
null as MA_VOICE_INTER,
null as MA_SMS_INTER,
null as MA_DATA,
null as MA_GOS_SVA,
null as MA_SMS_ROAMING,
null as MA_SMS_SVA,
null as MA_VOICE_SVA,
null as user_voice,
bdle_name,
sum(nvl(BDLE_COST,0))  BDLE_COST,
null as volume_data,
null as user_data1,
null as user_data2 ,
null as MONTANT,
null as destination_type,
null as destination,
null as traffic_data,
null as rated_sms_total_count,
null as in_duration,
null as rated_duration,
null as paygo_data_revenue,
null as user_data,
null as paygo_voice_revenue,
null as og_total_call_duration,
'SUBS' AS  data_type,
CURRENT_TIMESTAMP  INSERT_DATE,
period AS event_date
FROM
(select * from mon.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
WHERE PERIOD  ='2020-04-05') A
left join
(SELECT EVENT_DATE, msisdn, site_name FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE ='2020-04-05') B on A.msisdn=B.msisdn and A.PERIOD=B.EVENT_DATE
left join
(SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT FROM dim.dt_Ref_cbm) C on A.msisdn=C.msisdn
group by A.period, B.site_name, bdle_name, multi, operateur, tenure, segment
UNION ALL
SELECT
site_name,
multi,
operateur,
tenure,
segment,
null as MOU_OFNET,
null as MA_VOICE_ONNET,
null as MA_VOICE_INTER,
null as MA_SMS_INTER,
null as MA_DATA,
null as  MA_GOS_SVA,
null as MA_SMS_ROAMING,
null as MA_SMS_SVA,
null as MA_VOICE_SVA,
null as  user_voice,
bdle_name,
sum(nvl(BDLE_COST,0)) BDLE_COST,
null as volume_data,
null as user_data1,
null as  user_data2 ,
null as MONTANT,
null as destination_type,
null as destination,
null as traffic_data,
null as rated_sms_total_count,
null as in_duration,
null as rated_duration,
null as  paygo_data_revenue,
null as  user_data,
null as  paygo_voice_revenue,
null as og_total_call_duration,
'RETAIL' AS  data_type,
CURRENT_TIMESTAMP  INSERT_DATE,
period AS  event_date
FROM
(select Sdate as period, sub_msisdn as msisdn, offer_name as bdle_name,
sum(RECHARGE_AMOUNT) as BDLE_COST
from MON.SPARK_FT_VAS_RETAILLER_IRIS
where upper(offer_type) not in ('TOPUP') and sdate ='2020-04-05' and PRETUPS_STATUSCODE = '200'
group by Sdate, sub_msisdn, offer_name) A
left join
(SELECT EVENT_DATE, msisdn, site_name FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE ='2020-04-05') B on A.msisdn=B.msisdn and A.PERIOD=B.EVENT_DATE
left join
(SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT FROM dim.dt_Ref_cbm) C on A.msisdn=C.msisdn
group by A.period, B.site_name, bdle_name, multi, operateur, tenure, segment
UNION ALL
SELECT
site_name,
multi,
operateur,
tenure,
segment,
null as MOU_OFNET,
null as MA_VOICE_ONNET,
null as MA_VOICE_INTER,
null as MA_SMS_INTER,
null as MA_DATA,
null as  MA_GOS_SVA,
null as MA_SMS_ROAMING,
null as MA_SMS_SVA,
null as MA_VOICE_SVA,
null as  user_voice,
null as bdle_name,
null as BDLE_COST,
sum(nbytest)/(1024*1024) as volume_data,
sum(is_user_data1) as user_data1,
sum(is_user_data2) as user_data2  ,
null as MONTANT,
null as destination_type,
null as destination,
null as traffic_data,
null as rated_sms_total_count,
null as in_duration,
null as rated_duration,
null as  paygo_data_revenue,
null as  user_data,
null as  paygo_voice_revenue,
null as og_total_call_duration,
'TRAFFIC_DATA' AS  data_type,
CURRENT_TIMESTAMP  INSERT_DATE,
period AS  event_date
FROM
(select TRANSACTION_DATE as period,
msisdn, nbytest, case when nbytest/(1024*1024)>1  then 1 else 0 end as is_user_data1,
case when nbytest/(1024*1024)>5  then 1 else 0 end as is_user_data2  from MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
WHERE TRANSACTION_DATE ='2020-04-05') A
left join
(SELECT EVENT_DATE, msisdn, site_name FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE ='2020-04-05') B on A.msisdn=B.msisdn and A.PERIOD=B.EVENT_DATE
left join
(SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT FROM dim.dt_Ref_cbm) C on A.msisdn=C.msisdn
group by A.period, B.site_name, multi, operateur, tenure, segment
UNION ALL
SELECT
SITE_NAME,
MULTI,
OPERATEUR,
TENURE,
SEGMENT,
null as MOU_OFNET,
null as MA_VOICE_ONNET,
null as MA_VOICE_INTER,
null as MA_SMS_INTER,
null as MA_DATA,
null as  MA_GOS_SVA,
null as MA_SMS_ROAMING,
null as MA_SMS_SVA,
null as MA_VOICE_SVA,
null as  user_voice,
null as bdle_name,
null as BDLE_COST,
null as volume_data,
null as user_data1,
null as user_data2 ,
sum(MONTANT) as MONTANT,
null as destination_type,
null as destination,
null as traffic_data,
null as rated_sms_total_count,
null as in_duration,
null as rated_duration,
null as  paygo_data_revenue,
null as  user_data,
null as  paygo_voice_revenue,
null as og_total_call_duration,
'VAS' AS  data_type,
CURRENT_TIMESTAMP  INSERT_DATE,
CREATE_DATE as EVENT_DATE
FROM
(
SELECT
A.CREATE_DATE,
A.MSISDN,
A.SERVICE,
CASE WHEN C.ACCT_RES_RATING_UNIT = 'QM' THEN -A.CHARGE/100 ELSE -A.CHARGE END AS MONTANT
FROM
(
SELECT
CREATE_DATE,
if(substr(trim(ACC_NBR),1,3)=237,substr(trim(ACC_NBR),4,9),trim(ACC_NBR)) MSISDN,
CHANNEL_ID SERVICE,
ACCT_RES_CODE,
COUNT(1) NOMBRE_TRANSACTION,
SUM(NVL(CHARGE,0)) CHARGE
FROM CDR.SPARK_IT_ZTE_ADJUSTMENT
WHERE CREATE_DATE ='2020-04-05'
GROUP BY CREATE_DATE, if(substr(trim(ACC_NBR),1,3)=237,substr(trim(ACC_NBR),4,9),trim(ACC_NBR)), CHANNEL_ID, ACCT_RES_CODE
) A
LEFT JOIN DIM.DT_ZTE_USAGE_TYPE B ON A.SERVICE = B.USAGE_CODE
LEFT JOIN
(
SELECT DISTINCT
ACCT_RES_ID,
UPPER(ACCT_RES_NAME) ACCT_RES_NAME,
ACCT_RES_RATING_TYPE,
ACCT_RES_RATING_UNIT
FROM DIM.SPARK_DT_BALANCE_TYPE_ITEM
) C ON A.ACCT_RES_CODE = C.ACCT_RES_ID) FI
LEFT JOIN
(SELECT EVENT_DATE, msisdn, site_name FROM mon.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE ='2020-04-05') EN on FI.msisdn=EN.msisdn and FI.CREATE_DATE=EN.EVENT_DATE
left join
(SELECT msisdn, MULTI, OPERATEUR, TENURE, SEGMENT FROM dim.dt_Ref_cbm) ES on FI.msisdn=ES.msisdn
WHERE SERVICE IN (9,13,28,29,33)
group by FI.CREATE_DATE, EN.SITE_NAME, multi, operateur, tenure, segment
UNION ALL
select
site_name,
null as MULTI,
null as OPERATEUR,
null as TENURE,
null as SEGMENT,
null as MOU_OFNET,
null as MA_VOICE_ONNET,
null as MA_VOICE_INTER,
null as MA_SMS_INTER,
null as MA_DATA,
null as MA_GOS_SVA,
null as MA_SMS_ROAMING,
null as MA_SMS_SVA,
null as MA_VOICE_SVA,
null as user_voice,
null as bdle_name,
null as BDLE_COST,
null as volume_data,
null as user_data1,
null as user_data2 ,
null as MONTANT,
destination_type,
destination,
mbytes_used as traffic_data,
rated_sms_total_count,
in_duration,
rated_duration,
null as paygo_data_revenue,
null as user_data,
null as paygo_voice_revenue,
null as og_total_call_duration,
'GEOMART' AS  data_type,
CURRENT_TIMESTAMP  INSERT_DATE,
'2020-04-05' event_date
from mon.SPARK_FT_REVENU_LOCALISE
where event_date  = '2020-04-05'
UNION ALL
select
null as site_name,
null as MULTI,
null as OPERATEUR,
null as TENURE,
null as SEGMENT,
null as MOU_OFNET,
null as MA_VOICE_ONNET,
null as MA_VOICE_INTER,
null as MA_SMS_INTER,
null as MA_DATA,
null as MA_GOS_SVA,
null as MA_SMS_ROAMING,
null as MA_SMS_SVA,
null as MA_VOICE_SVA,
count(distinct case when og_total_call_duration>0 then msisdn else null end) as user_voice,
null as bdle_name,
null as BDLE_COST,
sum(data_bytes_received + data_bytes_sent)/(1024*1024) as volume_data,
null as user_data1,
null as user_data2 ,
null as MONTANT,
null as  destination_type,
null as  destination,
null as  traffic_data,
null as  rated_sms_total_count,
null as  in_duration,
null as  rated_duration,
sum(data_main_rated_amount) as paygo_data_revenue,
count(distinct case when (data_bytes_received + data_bytes_sent)/(1024*1024)> 1 then msisdn else null end) as user_data,
sum(main_rated_tel_amount) as paygo_voice_revenue,
sum(og_total_call_duration) as og_total_call_duration,
'REPORTING_DG' AS  data_type,
CURRENT_TIMESTAMP  INSERT_DATE,
'2020-04-05' event_date
from MON.SPARK_FT_MARKETING_DATAMART
where event_date = '2020-04-05'
) T



SELECT 
  TRANSACTION_DATE,
  TRANSACTION_TIME,
  CONTRACT_TYPE,
  OPERATOR_CODE,
  MAIN_USAGE_SERVICE_CODE,
  COMMERCIAL_OFFER,
  PREVIOUS_COMMERCIAL_OFFER,
  SUBS_SERVICE,
  SUBS_BENEFIT_NAME,
  SUBS_CHANNEL,
  SUBS_RELATED_SERVICE,
  SUM(SUBS_TOTAL_COUNT) SUBS_TOTAL_COUNT,
  SUM(SUBS_AMOUNT) SUBS_AMOUNT,
  SOURCE_PLATFORM,
  SOURCE_DATA,
  max(INSERT_DATE) INSERT_DATE,
  SERVICE_CODE,
  sum(MSISDN_COUNT) MSISDN_COUNT,
  sum(SUBS_EVENT_RATED_COUNT) SUBS_EVENT_RATED_COUNT,
  SUBS_PRICE_UNIT,
  sum(AMOUNT_SVA ) AMOUNT_SVA,
  sum(AMOUNT_VOICE_ONNET) AMOUNT_VOICE_ONNET,
  sum(AMOUNT_VOICE_OFFNET) AMOUNT_VOICE_OFFNET ,
  sum(AMOUNT_VOICE_INTER) AMOUNT_VOICE_INTER,
  sum(AMOUNT_VOICE_ROAMING) AMOUNT_VOICE_ROAMING,
  sum(AMOUNT_SMS_ONNET) AMOUNT_SMS_ONNET,
  sum(AMOUNT_SMS_OFFNET)AMOUNT_SMS_OFFNET,
  sum(AMOUNT_SMS_INTER) AMOUNT_SMS_INTER,
  sum(AMOUNT_SMS_ROAMING) AMOUNT_SMS_ROAMING,
  sum(AMOUNT_DATA) AMOUNT_DATA,
  COMBO 
FROM AGG.SPARK_FT_A_SUBSCRIPTION WHERE TRANSACTION_DATE='2020-09-01'
group by 
TRANSACTION_DATE,
TRANSACTION_TIME,
CONTRACT_TYPE,
OPERATOR_CODE,
MAIN_USAGE_SERVICE_CODE,
COMMERCIAL_OFFER,
PREVIOUS_COMMERCIAL_OFFER,
SUBS_SERVICE,
SUBS_BENEFIT_NAME,
SUBS_CHANNEL,
SUBS_RELATED_SERVICE,
SOURCE_PLATFORM,
SOURCE_DATA,
SERVICE_CODE,
SUBS_PRICE_UNIT,
AMOUNT_SVA,
AMOUNT_VOICE_ONNET,
AMOUNT_VOICE_OFFNET,
AMOUNT_VOICE_INTER,
AMOUNT_VOICE_ROAMING,
AMOUNT_SMS_ONNET,
AMOUNT_SMS_OFFNET,
AMOUNT_SMS_INTER,
AMOUNT_SMS_ROAMING,
AMOUNT_DATA,
COMBO

create table AGG.SPARK_FT_A_DATAMART_OM_MARKETING2   as
select
    site_name,
    service_type,
    sum(nvl(vol,0)) vol ,
    sum(nvl(val,0)) val ,
    sum(nvl(commission,0)) commission ,
    sum(nvl(revenu,0)) revenu,
    count(distinct msisdn) nb_numeros,
    sum(1) nb_lignes ,
    style,
    details,
    OPERATOR_CODE OPERATOR_CODE,
    PROFILE PROFILE_CODE,
    jour
from MON.SPARK_DATAMART_OM_MARKETING2  a
left join (
    SELECT
        event_date,
        A.ACCESS_KEY,
        max(PROFILE) PROFILE,
        MAX(OPERATOR_CODE) OPERATOR_CODE
    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
    WHERE EVENT_DATE>='2020-01-01'
    GROUP BY A.ACCESS_KEY, EVENT_DATE
) b ON a.jour=b.event_date and nvl(b.ACCESS_KEY,'INCONNU') = nvl(a.msisdn,'INCONNU')
group by site_name,service_type, style,details,jour ,OPERATOR_CODE ,PROFILE;


SELECT
     'BALANCE_OM' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'BALANCE_OM' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'IT_OM_ALL_BALANCE' SOURCE_TABLE
     , C.OPERATOR_CODE OPERATOR_CODE
     , SUM(nvl(BALANCE,0)) TOTAL_AMOUNT
     , SUM(nvl(BALANCE,0)) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , ORIGINAL_FILE_DATE TRANSACTION_DATE
FROM CDR.SPARK_IT_OM_ALL_BALANCE B2
LEFT JOIN (
SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
         LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                    WHERE EVENT_DATE between date_sub('2020-09-01',7) AND '2020-09-01'
                    GROUP BY ACCESS_KEY) B
                   ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
WHERE B.ACCESS_KEY IS NOT NULL
GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON C.ACCESS_KEY = B2.account_id
group by site_name,service_type, style,details,jour ,OPERATOR_CODE ,PROFILE;

create table junk.parc_om_30J  as
select
    site_name,
    max(service_type) service_type,
    count(distinct msisdn) parc_om_30j,
    max(details) details,
    max(OPERATOR_CODE) OPERATOR_CODE,
    max(PROFILE_CODE) PROFILE_CODE,
    jour
FROM (
    select
        site_name,
        service_type,
        msisdn,
        details,
        OPERATOR_CODE,
        PROFILE_CODE,
        ROW_NUMBER() OVER (PARTITION BY jour,site_name,msisdn ORDER BY OCC DESC) AS RG,
        jour
    FROM (

    ) T
)T  WHERE RG=1
GROUP BY
    site_name,
    jour


insert into junk.parc_om_30J2
select
    administrative_region,
    max(service_type) service_type,
    count(distinct t1.msisdn) parc_om_30j,
    max(details) details,
    max(OPERATOR_CODE) OPERATOR_CODE,
    max(PROFILE_CODE) PROFILE_CODE,
    jour
from (
    SELECT
            max(service_type) service_type,
            msisdn,
            max(details) details,
            max(OPERATOR_CODE) OPERATOR_CODE,
            max(PROFILE_CODE) PROFILE_CODE,
            a.jour jour
    from (
        select datecode jour FROM dim.dt_dates where datecode between '2020-09-02' and '2020-11-17'
     )  A
    LEFT JOIN  (
        select
            service_type,
            msisdn,
            details,
            OPERATOR_CODE OPERATOR_CODE,
            PROFILE PROFILE_CODE,
            jour
        from (select * from MON.SPARK_DATAMART_OM_MARKETING2 where  JOUR>='2020-09-01' AND STYLE NOT LIKE ('REC%') ) a
        left join (
            SELECT
                event_date,
                A.ACCESS_KEY,
                max(PROFILE) PROFILE,
                MAX(OPERATOR_CODE) OPERATOR_CODE
            FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
            WHERE EVENT_DATE>='2020-09-01'
            GROUP BY A.ACCESS_KEY, EVENT_DATE
        ) b ON a.jour=b.event_date and nvl(b.ACCESS_KEY,'INCONNU') = nvl(a.msisdn,'INCONNU')
        group by
            service_type,
            msisdn,
            details,
            OPERATOR_CODE ,
            PROFILE ,
            jour
    ) b   on b.JOUR BETWEEN date_sub(a.jour,30) and a.jour
    group by
            msisdn,
            a.jour
) T1
LEFT JOIN (select MSISDN,administrative_region,event_date from mon.spark_ft_client_last_site_day where event_date>='2020-09-01' ) T2 ON T1.MSISDN=T2.MSISDN and T1.JOUR =T2.event_date
group by administrative_region,jour



create table junk.parc_om_90J2  as
select
    administrative_region,
    max(service_type) service_type,
    count(distinct t1.msisdn) parc_om_90j,
    max(details) details,
    max(OPERATOR_CODE) OPERATOR_CODE,
    max(PROFILE_CODE) PROFILE_CODE,
    jour
from (
    SELECT
            max(service_type) service_type,
            msisdn,
            max(details) details,
            max(OPERATOR_CODE) OPERATOR_CODE,
            max(PROFILE_CODE) PROFILE_CODE,
            a.jour jour
    from (
        select datecode jour FROM dim.dt_dates where datecode between '2020-09-01' and '2020-09-01'
     )  A
    LEFT JOIN  (
        select
            service_type,
            msisdn,
            details,
            OPERATOR_CODE OPERATOR_CODE,
            PROFILE PROFILE_CODE,
            jour
        from (select * from MON.SPARK_DATAMART_OM_MARKETING2 where  JOUR>='2020-09-01' AND STYLE NOT LIKE ('REC%') ) a
        left join (
            SELECT
                event_date,
                A.ACCESS_KEY,
                max(PROFILE) PROFILE,
                MAX(OPERATOR_CODE) OPERATOR_CODE
            FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
            WHERE EVENT_DATE>='2020-09-01'
            GROUP BY A.ACCESS_KEY, EVENT_DATE
        ) b ON a.jour=b.event_date and nvl(b.ACCESS_KEY,'INCONNU') = nvl(a.msisdn,'INCONNU')
        group by
            service_type,
            msisdn,
            details,
            OPERATOR_CODE ,
            PROFILE ,
            jour
    ) b   on b.JOUR BETWEEN date_sub(a.jour,90) and a.jour
    group by
            msisdn,
            a.jour
) T1
LEFT JOIN (select MSISDN,administrative_region,event_date from mon.spark_ft_client_last_site_day where event_date>='2020-09-01' ) T2 ON T1.MSISDN=T2.MSISDN and T1.JOUR =T2.event_date
group by administrative_region,jour


create table junk.pdv_om_30J  as
select
    site_name,
    max(service_type) service_type,
    count(distinct msisdn) parc_om_30j,
    max(details) details,
    max(OPERATOR_CODE) OPERATOR_CODE,
    max(PROFILE_CODE) PROFILE_CODE,
    jour
FROM (
    select
        site_name,
        service_type,
        msisdn,
        details,
        OPERATOR_CODE,
        PROFILE_CODE,
        ROW_NUMBER() OVER (PARTITION BY jour,site_name,msisdn ORDER BY OCC DESC) AS RG,
        jour
    FROM (
        SELECT
                site_name,
                service_type,
                msisdn,
                details,
                OPERATOR_CODE,
                PROFILE_CODE,
                sum(1) occ,
                a.jour jour
        from (
            select datecode jour FROM dim.dt_dates where datecode between '2020-09-01' and '2020-09-01'
         )  A
        LEFT JOIN  (
            select
                service_type,
                msisdn,
                details,
                OPERATOR_CODE OPERATOR_CODE,
                PROFILE PROFILE_CODE,
                jour
            from (

                select DOD.* FROM
                    MON.SPARK_DATAMART_OM_DISTRIB DOD
                LEFT JOIN MON.SPARK_REF_OM_PRODUCTS2 RE ON DOD.MSISDN=RE.MSISDN AND  JOUR BETWEEN date_sub('2020-09-01',30) and '2020-09-01' AND RE.REF_DATE in (select max(ref_date) ref_date from MON.SPARK_REF_OM_PRODUCTS2 where ref_date<='2020-09-01' )  AND PRODUCT_LINE='DISTRIBUTION'AND SERVICE_TYPE NOT LIKE 'P2P%'

             ) a
            left join (
                SELECT
                    event_date,
                    A.ACCESS_KEY,
                    max(PROFILE) PROFILE,
                    MAX(OPERATOR_CODE) OPERATOR_CODE
                FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
                WHERE EVENT_DATE>='2020-09-01'
                GROUP BY A.ACCESS_KEY, EVENT_DATE
            ) b ON a.jour=b.event_date and nvl(b.ACCESS_KEY,'INCONNU') = nvl(a.msisdn,'INCONNU')
            group by
                service_type,
                msisdn,
                details,
                OPERATOR_CODE ,
                PROFILE ,
                jour
        ) b   on b.JOUR BETWEEN date_sub(a.jour,30) and a.jour
        group by
               site_name,
                service_type,
                msisdn,
                details,
                OPERATOR_CODE,
                PROFILE_CODE,
                a.jour

    ) T
)T  WHERE RG=1
GROUP BY
    site_name,
    jour


SELECT
     'PDV_OM_ACTIF_30Jrs' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'PDV_OM_ACTIF_30Jrs' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'DATAMART_OM_DISTRIB' SOURCE_TABLE
     , C.OPERATOR_CODE OPERATOR_CODE
     , COUNT (DISTINCT b2.MSISDN) TOTAL_AMOUNT
     , COUNT (DISTINCT b2.MSISDN) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , to_date('2020-09-01') TRANSACTION_DATE
FROM (select DOD.* FROM
    MON.SPARK_DATAMART_OM_DISTRIB DOD
LEFT JOIN MON.SPARK_REF_OM_PRODUCTS2 RE
            ON DOD.MSISDN=RE.MSISDN
WHERE JOUR BETWEEN date_sub('2020-09-01',30) and '2020-09-01' AND RE.REF_DATE in (select max(ref_date) ref_date from MON.SPARK_REF_OM_PRODUCTS2 where ref_date<='2020-09-01' )  AND PRODUCT_LINE='DISTRIBUTION'AND SERVICE_TYPE NOT LIKE 'P2P%'
)B2
LEFT JOIN (
SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
         LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                    WHERE EVENT_DATE between date_sub('2020-09-01',7) AND '2020-09-01'
                    GROUP BY ACCESS_KEY) B
                   ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
WHERE B.ACCESS_KEY IS NOT NULL
GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON C.ACCESS_KEY = B2.msisdn
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='2020-09-01'
    ) b on a.msisdn = b.msisdn
    where a.event_date='2020-09-01'
    group by a.msisdn
) site on  site.msisdn =B2.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
GROUP BY
    C.PROFILE,
    C.OPERATOR_CODE,
    REGION_ID


create table junk.parc_om_90J  as
select
    site_name,
    max(service_type) service_type,
    count(distinct msisdn) parc_om_90j,
    max(details) details,
    max(OPERATOR_CODE) OPERATOR_CODE,
    max(PROFILE_CODE) PROFILE_CODE,
    jour
FROM (
    select
        site_name,
        service_type,
        msisdn,
        details,
        OPERATOR_CODE,
        PROFILE_CODE,
        ROW_NUMBER() OVER (PARTITION BY jour,site_name,msisdn ORDER BY OCC DESC) AS RG,
        jour
    FROM (
        SELECT
                site_name,
                service_type,
                msisdn,
                details,
                OPERATOR_CODE,
                PROFILE_CODE,
                sum(1) occ,
                a.jour jour
        from (
            select datecode jour FROM dim.dt_dates where datecode between '2020-09-01' and '2020-09-01'
         )  A
        LEFT JOIN  (
            select
                site_name,
                service_type,
                msisdn,
                details,
                OPERATOR_CODE OPERATOR_CODE,
                PROFILE PROFILE_CODE,
                jour
            from (
                select * from MON.SPARK_DATAMART_OM_MARKETING2 where  JOUR>='2020-04-01' AND STYLE NOT LIKE ('REC%') ) a
            left join (
                SELECT
                    event_date,
                    A.ACCESS_KEY,
                    max(PROFILE) PROFILE,
                    MAX(OPERATOR_CODE) OPERATOR_CODE
                FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
                WHERE EVENT_DATE>='2020-04-01'
                GROUP BY A.ACCESS_KEY, EVENT_DATE
            ) b ON a.jour=b.event_date and nvl(b.ACCESS_KEY,'INCONNU') = nvl(a.msisdn,'INCONNU')
            group by
             site_name,
                service_type,
                msisdn,
                details,
                OPERATOR_CODE ,
                PROFILE ,
                jour
        ) b   on b.JOUR BETWEEN date_sub(a.jour,90) and a.jour
        group by
               site_name,
                service_type,
                msisdn,
                details,
                OPERATOR_CODE,
                PROFILE_CODE,
                a.jour

    ) T
)T  WHERE RG=1
GROUP BY
    site_name,
    jour



create table junk.parc_om_90J  as
SELECT
        site_name,
        service_type,
        COUNT (DISTINCT b.MSISDN) parc_90jrs,
        details,
        OPERATOR_CODE,
        PROFILE_CODE,
        a.jour jour
from (
    select datecode jour FROM dim.dt_dates where datecode between '2020-01-01' and '2020-09-01'
 )  A
LEFT JOIN  (
    select
        site_name,
        service_type,
        msisdn,
        details,
        OPERATOR_CODE OPERATOR_CODE,
        PROFILE PROFILE_CODE,
        jour
    from (select * from MON.SPARK_DATAMART_OM_MARKETING2 where  STYLE NOT LIKE ('REC%') ) a
    left join (
        SELECT
            event_date,
            A.ACCESS_KEY,
            max(PROFILE) PROFILE,
            MAX(OPERATOR_CODE) OPERATOR_CODE
        FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
        WHERE EVENT_DATE>='2020-01-01'
        GROUP BY A.ACCESS_KEY, EVENT_DATE
    ) b ON a.jour=b.event_date and nvl(b.ACCESS_KEY,'INCONNU') = nvl(a.msisdn,'INCONNU')
    group by
     site_name,
        service_type,
        msisdn,
        details,
        OPERATOR_CODE ,
        PROFILE ,
        jour
) b   on b.JOUR BETWEEN date_sub(a.jour,90) and a.jour
group by
        site_name,
        service_type,
        details,
        OPERATOR_CODE,
        PROFILE_CODE,
        a.jour


create table junk.pdv_om_30J  as
SELECT
        site_name,
        service_type,
        COUNT (DISTINCT b.MSISDN) pdv_90jrs,
        details,
        OPERATOR_CODE,
        PROFILE_CODE,
        a.jour jour
from (
    select datecode jour FROM dim.dt_dates where datecode between '2020-01-01' and '2020-09-01'
 )  A
LEFT JOIN  (
    select
        site_name,
        service_type,
        msisdn,
        details,
        OPERATOR_CODE OPERATOR_CODE,
        PROFILE PROFILE_CODE,
        jour
    from (select DOD.* FROM
            MON.SPARK_DATAMART_OM_DISTRIB DOD
        LEFT JOIN MON.SPARK_REF_OM_PRODUCTS2 RE
                    ON DOD.MSISDN=RE.MSISDN
        WHERE JOUR BETWEEN date_sub('2020-09-01',30) and '2020-09-01' AND RE.REF_DATE in (select max(ref_date) ref_date from MON.SPARK_REF_OM_PRODUCTS2 where ref_date<='2020-09-01' )  AND PRODUCT_LINE='DISTRIBUTION'AND SERVICE_TYPE NOT LIKE 'P2P%'
    ) a
    left join (
        SELECT
            event_date,
            A.ACCESS_KEY,
            max(PROFILE) PROFILE,
            MAX(OPERATOR_CODE) OPERATOR_CODE
        FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
        WHERE EVENT_DATE>='2020-01-01'
        GROUP BY A.ACCESS_KEY, EVENT_DATE
    ) b ON a.jour=b.event_date and nvl(b.ACCESS_KEY,'INCONNU') = nvl(a.msisdn,'INCONNU')
    group by
     site_name,
        service_type,
        msisdn,
        details,
        OPERATOR_CODE ,
        PROFILE ,
        jour
) b   on b.JOUR BETWEEN date_sub(a.jour,30) and a.jour
group by
        site_name,
        service_type,
        details,
        OPERATOR_CODE,
        PROFILE_CODE,
        a.jour


 create table junk.parc_om as  SELECT a.jour,COUNT (DISTINCT b.MSISDN) parc_30jrs from   (select datecode jour FROM dim.dt_dates where datecode between '2020-01-01' and '2020-09-01' )  A LEFT JOIN  (select * from MON.spark_datamart_om_marketing2  where  STYLE NOT LIKE ('REC%') )  b on b.JOUR BETWEEN date_sub(a.jour,30) and a.jour group by a.jour


xxx\|\s+(\w+)\s+\|\s+\w+\(?\d*\)?\s+\|\s+\|
event_inst_id|re_id|billing_nbr|billing_imsi|calling_nbr|called_nbr|third_part_nbr|start_time|duration|lac_a|cell_a|lac_b|cell_b|calling_imei|called_imei|price_id1|price_id2|price_id3|price_id4|price_plan_id1|price_plan_id2|price_plan_id3|price_plan_id4|acct_res_id1|acct_res_id2|acct_res_id3|acct_res_id4|charge1|charge2|charge3|charge4|bal_id1|bal_id2|bal_id3|bal_id4|acct_item_type_id1|acct_item_type_id2|acct_item_type_id3|acct_item_type_id4|prepay_flag|pre_balance1|balance1|pre_balance2|balance2|pre_balance3|balance3|pre_balance4|balance4|international_roaming_flag|call_type|byte_up|byte_down|bytes|price_plan_code|session_id|result_code|prod_spec_std_code|yzdiscount|byzcharge1|byzcharge2|byzcharge3|byzcharge4|onnet_offnet|provider_id|prod_spec_id|termination_cause|b_prod_spec_id|b_price_plan_code|callspetype|chargingratio|sgsn_address|ggsn_address|rating_group|called_station_id|pdp_address|gpp_pdp_type|gpp_user_location_info|charge_unit|ismp_product_offer_id|ismp_provide_id|mnp_prefix|file_tap_id|ismp_product_id|





SELECT
     'PARC_OM_30Jrs' DESTINATION_CODE
     , PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'PARC_OM_30Jrs' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'DATAMART_OM_MARKETING' SOURCE_TABLE
     , OPERATOR_CODE
     , COUNT (DISTINCT b2.MSISDN) TOTAL_AMOUNT
     , COUNT (DISTINCT b2.MSISDN) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , to_date('2020-09-01') TRANSACTION_DATE
FROM (select distinct msisdn from MON.spark_datamart_om_marketing2 WHERE JOUR BETWEEN date_sub('2020-09-01',30) and '2020-09-01' AND STYLE NOT LIKE ('REC%') )B2
LEFT JOIN (
SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
         LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                    WHERE EVENT_DATE between date_sub('2020-09-01',7) AND '2020-09-01'
                    GROUP BY ACCESS_KEY) B
                   ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
WHERE B.ACCESS_KEY IS NOT NULL
GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON C.ACCESS_KEY = B2.msisdn
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='2020-09-01'
    ) b on a.msisdn = b.msisdn
    where a.event_date='2020-09-01'
    group by a.msisdn
) site on  site.msisdn =B2.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON UPPER(nvl(site.administrative_region,'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
GROUP BY
    PROFILE_CODE,
    OPERATOR_CODE,
    REGION_ID


UNION ALL
-------- Users(90Jrs)


SELECT
     'PARC_OM_90Jrs' DESTINATION_CODE
     , PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'PARC_OM_90Jrs' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'DATAMART_OM_MARKETING' SOURCE_TABLE
     , OPERATOR_CODE
     , COUNT (DISTINCT b2.MSISDN) TOTAL_AMOUNT
     , COUNT (DISTINCT b2.MSISDN) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , to_date('2020-09-01') TRANSACTION_DATE
FROM (select distinct msisdn from AGG.SPARK_FT_A_DATAMART_OM_MARKETING2 WHERE JOUR BETWEEN date_sub('2020-09-01',90) and '2020-09-01' AND STYLE NOT LIKE ('REC%') )B2
LEFT JOIN (
SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
         LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                    WHERE EVENT_DATE between date_sub('2020-09-01',7) AND '2020-09-01'
                    GROUP BY ACCESS_KEY) B
                   ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
WHERE B.ACCESS_KEY IS NOT NULL
GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON C.ACCESS_KEY = B2.msisdn
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='2020-09-01'
    ) b on a.msisdn = b.msisdn
    where a.event_date='2020-09-01'
    group by a.msisdn
) site on  site.msisdn =B2.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON UPPER(nvl(site.administrative_region,'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
GROUP BY
    PROFILE_CODE,
    OPERATOR_CODE,
    REGION_ID

UNION ALL


---Nombre de Pos OM actif (MONDV_OM.DATAMART_OM_DISTRIB)

INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG
SELECT
     'PDV_OM_ACTIF_30Jrs' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'PDV_OM_ACTIF_30Jrs' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'DATAMART_OM_DISTRIB' SOURCE_TABLE
     , C.OPERATOR_CODE OPERATOR_CODE
     , COUNT (DISTINCT b2.MSISDN) TOTAL_AMOUNT
     , COUNT (DISTINCT b2.MSISDN) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , to_date('2020-09-01') TRANSACTION_DATE
FROM (select DOD.* FROM
    MON.SPARK_DATAMART_OM_DISTRIB DOD
LEFT JOIN MON.SPARK_REF_OM_PRODUCTS2 RE
            ON DOD.MSISDN=RE.MSISDN
WHERE JOUR BETWEEN date_sub('2020-09-01',30) and '2020-09-01' AND RE.REF_DATE in (select max(ref_date) ref_date from MON.SPARK_REF_OM_PRODUCTS2 where ref_date<='2020-09-01' )  AND PRODUCT_LINE='DISTRIBUTION'AND SERVICE_TYPE NOT LIKE 'P2P%'
)B2
LEFT JOIN (
SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
         LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                    WHERE EVENT_DATE between date_sub('2020-09-01',7) AND '2020-09-01'
                    GROUP BY ACCESS_KEY) B
                   ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
WHERE B.ACCESS_KEY IS NOT NULL
GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON C.ACCESS_KEY = B2.msisdn
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='2020-09-01'
    ) b on a.msisdn = b.msisdn
    where a.event_date='2020-09-01'
    group by a.msisdn
) site on  site.msisdn =B2.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
GROUP BY
    C.PROFILE,
    C.OPERATOR_CODE,
    REGION_ID



SELECT IF(T1.KPI_IS_LOAD=0 AND T2.CONTRACT_EXISTS>0 AND T4.OM_DIST>0 AND LAST_SITE>0 and SITE_TRAFFIC>0,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG  WHERE TRANSACTION_DATE='2020-09-01' AND SOURCE_TABLE IN (,'DATAMART_OM_DISTRIB'))T1,
(SELECT COUNT(*) CONTRACT_EXISTS FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE BETWEEN DATE_SUB('2020-09-01',7) AND '2020-09-01')T2,
(SELECT COUNT(*) LAST_SITE FROM MON.spark_ft_client_last_site_day WHERE EVENT_DATE ='2020-09-01')T3,
(SELECT COUNT(*) OM_DIST FROM MON.SPARK_DATAMART_OM_DISTRIB WHERE jour ='2020-09-01')T4,
(SELECT COUNT(*) SITE_TRAFFIC FROM MON.spark_ft_client_site_traffic_day WHERE EVENT_DATE = '2020-09-01')T6


CREATE TABLE MON.SPARK_KPIS_REG_FINAL (
REGION_ADMINISTRATIVE  VARCHAR2(100), 
REGION_COMMERCIALE  VARCHAR2(100), 
CATEGORY  VARCHAR2(100), 
KPI  VARCHAR2(100), 
AXE_REVENUE  VARCHAR2(100), 
AXE_SUBSCRIBER  VARCHAR2(100), 
AXE_REGIONALE  VARCHAR2(100), 
SOURCE_TABLE  VARCHAR2(100), 
VALEUR  FLOAT, 
VALEUR_LWEEK  FLOAT, 
V2WA  FLOAT, 
V3WA  FLOAT, 
V4WA  FLOAT, 
VALEUR_MTD  FLOAT, 
VALEUR_LMTD  FLOAT, 
BUDGET  FLOAT, 
BUDGET_LWEEK  FLOAT, 
BUDGET_2WA  FLOAT, 
BUDGET_3WA  FLOAT, 
BUDGET_4WA  FLOAT, 
VALEUR_BUDGET_MTD  FLOAT, 
VALEUR_MTD_LAST_YEAR  FLOAT, 
VSLWEEK  FLOAT, 
VS2WA  FLOAT, 
VS3WA  FLOAT, 
VS4WA  FLOAT, 
MTDVSLMDT  FLOAT, 
MDTVSBUDGET  FLOAT, 
WEEKVSBUDGET  FLOAT, 
LWEEKVSBLWEEK  FLOAT, 
V2WAVSB2WA  FLOAT, 
V3WAVSB3WA  FLOAT, 
V4WAVSB4WA  FLOAT, 
MTD_VS_LAST_YEAR  FLOAT, 
INSERT_DATE  TIMESTAMP, 
PROCESSING_DATE  DATE
)


REGION_ADMINISTRATIVE, 
REGION_COMMERCIALE, 
CATEGORY, 
KPI, 
AXE_REVENUE, 
AXE_SUBSCRIBER, 
AXE_REGIONALE, 
SOURCE_TABLE, 
VALEUR, 
VALEUR_LWEEK, 
V2WA, 
V3WA, 
V4WA, 
VALEUR_MTD, 
VALEUR_LMTD, 
BUDGET, 
BUDGET_LWEEK, 
BUDGET_2WA, 
BUDGET_3WA, 
BUDGET_4WA, 
VALEUR_BUDGET_MTD, 
VALEUR_MTD_LAST_YEAR, 
VSLWEEK, 
VS2WA, 
VS3WA, 
VS4WA, 
MTDVSLMDT, 
MDTVSBUDGET, 
WEEKVSBUDGET, 
LWEEKVSBLWEEK, 
V2WAVSB2WA, 
V3WAVSB3WA, 
V4WAVSB4WA, 
MTD_VS_LAST_YEAR, 
VALEUR/100 VALEUR_PER, 
VALEUR_LWEEK/100 VALEUR_LWEEK_PER, 
V2WA/100 V2WA_PER, 
V3WA/100 V3WA_PER, 
V4WA/100 V4WA_PER, 
VALEUR_MTD/100 VALEUR_MTD_PER, 
VALEUR_LMTD/100 VALEUR_LMTD_PER, 
BUDGET/100 BUDGET_PER, 
BUDGET_LWEEK/100 BUDGET_LWEEK_PER, 
BUDGET_2WA/100 BUDGET_2WA_PER, 
BUDGET_3WA/100 BUDGET_3WA_PER, 
BUDGET_4WA/100 BUDGET_4WA_PER, 
VALEUR_BUDGET_MTD/100 VALEUR_BUDGET_MTD_PER, 
VALEUR_MTD_LAST_YEAR/100 VALEUR_MTD_LAST_YEAR_PER, 
VSLWEEK/100 VSLWEEK_PER, 
VS2WA/100 VS2WA_PER, 
VS3WA/100 VS3WA_PER, 
VS4WA/100 VS4WA_PER, 
MTDVSLMDT/100 MTDVSLMDT_PER, 
MDTVSBUDGET/100 MDTVSBUDGET_PER, 
WEEKVSBUDGET/100 WEEKVSBUDGET_PER, 
LWEEKVSBLWEEK/100 LWEEKVSBLWEEK_PER, 
V2WAVSB2WA/100 V2WAVSB2WA_PER, 
V3WAVSB3WA/100 V3WAVSB3WA_PER, 
V4WAVSB4WA/100 V4WAVSB4WA_PER, 
MTD_VS_LAST_YEAR/100 MTD_VS_LAST_YEAR_PER, 
INSERT_DATE, 
PROCESSING_DATE



select
     null region_administrative,
     null region_commerciale,
     category,
     kpi,
     axe_revenue,
     axe_subscriber,
     axe_regionale,
     (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(valeur)
        when kpi='Revenue Data Mobile' then sum(valeur)
        when kpi='Tx users data' then avg(valeur)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(valeur)
        when kpi='Churn' then sum(valeur)
        when kpi='Net adds' then sum(valeur)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(valeur)
        when kpi='Cash In Valeur' then sum(valeur)
        when kpi='Subscriber base' then sum(valeur)
        when kpi='Payments(Bill, Merch)' then sum(valeur)
        when kpi='Tx users (30jrs)' then avg(valeur)
        when kpi='Self Top UP ratio (%)' then avg(valeur)
        when kpi='Users (30jrs, >1Mo)' then sum(valeur)
        when kpi='Durée moyenne de réponse numérique' then sum(valeur)
        when kpi='dont sortant (~recharges)' then sum(valeur)
        when kpi='Engagements' then sum(valeur)
        when kpi='Revenue Orange Money' then sum(valeur)
        when kpi='Nombre de Pos Airtime actif' then sum(valeur)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(valeur)
        when kpi='Cash Out Valeur' then sum(valeur)
        when kpi='Price Per Megas' then avg(valeur)
        when kpi='Gross Adds' then sum(valeur)
        when kpi='dont Voix' then sum(valeur)
        when kpi='Pourcentage traité en numérique' then sum(valeur)
        when kpi='Recharges (C2S, CAG, OM)' then sum(valeur)
        else sum(valeur)
    END) valeur,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(valeur_lweek)
        when kpi='Revenue Data Mobile' then sum(valeur_lweek)
        when kpi='Tx users data' then avg(valeur_lweek)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(valeur_lweek)
        when kpi='Churn' then sum(valeur_lweek)
        when kpi='Net adds' then sum(valeur_lweek)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(valeur_lweek)
        when kpi='Cash In valeur_lweek' then sum(valeur_lweek)
        when kpi='Subscriber base' then sum(valeur_lweek)
        when kpi='Payments(Bill, Merch)' then sum(valeur_lweek)
        when kpi='Tx users (30jrs)' then avg(valeur_lweek)
        when kpi='Self Top UP ratio (%)' then avg(valeur_lweek)
        when kpi='Users (30jrs, >1Mo)' then sum(valeur_lweek)
        when kpi='Durée moyenne de réponse numérique' then sum(valeur_lweek)
        when kpi='dont sortant (~recharges)' then sum(valeur_lweek)
        when kpi='Engagements' then sum(valeur_lweek)
        when kpi='Revenue Orange Money' then sum(valeur_lweek)
        when kpi='Nombre de Pos Airtime actif' then sum(valeur_lweek)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(valeur_lweek)
        when kpi='Cash Out valeur_lweek' then sum(valeur_lweek)
        when kpi='Price Per Megas' then avg(valeur_lweek)
        when kpi='Gross Adds' then sum(valeur_lweek)
        when kpi='dont Voix' then sum(valeur_lweek)
        when kpi='Pourcentage traité en numérique' then sum(valeur_lweek)
        when kpi='Recharges (C2S, CAG, OM)' then sum(valeur_lweek)
        else sum(valeur_lweek)
    END) lweek,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(v2wa)
        when kpi='Revenue Data Mobile' then sum(v2wa)
        when kpi='Tx users data' then avg(v2wa)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(v2wa)
        when kpi='Churn' then sum(v2wa)
        when kpi='Net adds' then sum(v2wa)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(v2wa)
        when kpi='Cash In v2wa' then sum(v2wa)
        when kpi='Subscriber base' then sum(v2wa)
        when kpi='Payments(Bill, Merch)' then sum(v2wa)
        when kpi='Tx users (30jrs)' then avg(v2wa)
        when kpi='Self Top UP ratio (%)' then avg(v2wa)
        when kpi='Users (30jrs, >1Mo)' then sum(v2wa)
        when kpi='Durée moyenne de réponse numérique' then sum(v2wa)
        when kpi='dont sortant (~recharges)' then sum(v2wa)
        when kpi='Engagements' then sum(v2wa)
        when kpi='Revenue Orange Money' then sum(v2wa)
        when kpi='Nombre de Pos Airtime actif' then sum(v2wa)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(v2wa)
        when kpi='Cash Out v2wa' then sum(v2wa)
        when kpi='Price Per Megas' then avg(v2wa)
        when kpi='Gross Adds' then sum(v2wa)
        when kpi='dont Voix' then sum(v2wa)
        when kpi='Pourcentage traité en numérique' then sum(v2wa)
        when kpi='Recharges (C2S, CAG, OM)' then sum(v2wa)
        else sum(v2wa)
    END)  wa2,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(v3wa)
        when kpi='Revenue Data Mobile' then sum(v3wa)
        when kpi='Tx users data' then avg(v3wa)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(v3wa)
        when kpi='Churn' then sum(v3wa)
        when kpi='Net adds' then sum(v3wa)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(v3wa)
        when kpi='Cash In v3wa' then sum(v3wa)
        when kpi='Subscriber base' then sum(v3wa)
        when kpi='Payments(Bill, Merch)' then sum(v3wa)
        when kpi='Tx users (30jrs)' then avg(v3wa)
        when kpi='Self Top UP ratio (%)' then avg(v3wa)
        when kpi='Users (30jrs, >1Mo)' then sum(v3wa)
        when kpi='Durée moyenne de réponse numérique' then sum(v3wa)
        when kpi='dont sortant (~recharges)' then sum(v3wa)
        when kpi='Engagements' then sum(v3wa)
        when kpi='Revenue Orange Money' then sum(v3wa)
        when kpi='Nombre de Pos Airtime actif' then sum(v3wa)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(v3wa)
        when kpi='Cash Out v3wa' then sum(v3wa)
        when kpi='Price Per Megas' then avg(v3wa)
        when kpi='Gross Adds' then sum(v3wa)
        when kpi='dont Voix' then sum(v3wa)
        when kpi='Pourcentage traité en numérique' then sum(v3wa)
        when kpi='Recharges (C2S, CAG, OM)' then sum(v3wa)
        else sum(v3wa)
    END)  wa3,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(v4wa)
        when kpi='Revenue Data Mobile' then sum(v4wa)
        when kpi='Tx users data' then avg(v4wa)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(v4wa)
        when kpi='Churn' then sum(v4wa)
        when kpi='Net adds' then sum(v4wa)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(v4wa)
        when kpi='Cash In v4wa' then sum(v4wa)
        when kpi='Subscriber base' then sum(v4wa)
        when kpi='Payments(Bill, Merch)' then sum(v4wa)
        when kpi='Tx users (30jrs)' then avg(v4wa)
        when kpi='Self Top UP ratio (%)' then avg(v4wa)
        when kpi='Users (30jrs, >1Mo)' then sum(v4wa)
        when kpi='Durée moyenne de réponse numérique' then sum(v4wa)
        when kpi='dont sortant (~recharges)' then sum(v4wa)
        when kpi='Engagements' then sum(v4wa)
        when kpi='Revenue Orange Money' then sum(v4wa)
        when kpi='Nombre de Pos Airtime actif' then sum(v4wa)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(v4wa)
        when kpi='Cash Out v4wa' then sum(v4wa)
        when kpi='Price Per Megas' then avg(v4wa)
        when kpi='Gross Adds' then sum(v4wa)
        when kpi='dont Voix' then sum(v4wa)
        when kpi='Pourcentage traité en numérique' then sum(v4wa)
        when kpi='Recharges (C2S, CAG, OM)' then sum(v4wa)
        else sum(v4wa)
    END)  wa4,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(valeur_mtd)
        when kpi='Revenue Data Mobile' then sum(valeur_mtd)
        when kpi='Tx users data' then avg(valeur_mtd)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(valeur_mtd)
        when kpi='Churn' then sum(valeur_mtd)
        when kpi='Net adds' then sum(valeur_mtd)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(valeur_mtd)
        when kpi='Cash In valeur_mtd' then sum(valeur_mtd)
        when kpi='Subscriber base' then sum(valeur_mtd)
        when kpi='Payments(Bill, Merch)' then sum(valeur_mtd)
        when kpi='Tx users (30jrs)' then avg(valeur_mtd)
        when kpi='Self Top UP ratio (%)' then avg(valeur_mtd)
        when kpi='Users (30jrs, >1Mo)' then sum(valeur_mtd)
        when kpi='Durée moyenne de réponse numérique' then sum(valeur_mtd)
        when kpi='dont sortant (~recharges)' then sum(valeur_mtd)
        when kpi='Engagements' then sum(valeur_mtd)
        when kpi='Revenue Orange Money' then sum(valeur_mtd)
        when kpi='Nombre de Pos Airtime actif' then sum(valeur_mtd)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(valeur_mtd)
        when kpi='Cash Out valeur_mtd' then sum(valeur_mtd)
        when kpi='Price Per Megas' then avg(valeur_mtd)
        when kpi='Gross Adds' then sum(valeur_mtd)
        when kpi='dont Voix' then sum(valeur_mtd)
        when kpi='Pourcentage traité en numérique' then sum(valeur_mtd)
        when kpi='Recharges (C2S, CAG, OM)' then sum(valeur_mtd)
        else sum(valeur_mtd)
    END) valeur_mtd,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(valeur_lmtd)
        when kpi='Revenue Data Mobile' then sum(valeur_lmtd)
        when kpi='Tx users data' then avg(valeur_lmtd)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(valeur_lmtd)
        when kpi='Churn' then sum(valeur_lmtd)
        when kpi='Net adds' then sum(valeur_lmtd)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(valeur_lmtd)
        when kpi='Cash In valeur_lmtd' then sum(valeur_lmtd)
        when kpi='Subscriber base' then sum(valeur_lmtd)
        when kpi='Payments(Bill, Merch)' then sum(valeur_lmtd)
        when kpi='Tx users (30jrs)' then avg(valeur_lmtd)
        when kpi='Self Top UP ratio (%)' then avg(valeur_lmtd)
        when kpi='Users (30jrs, >1Mo)' then sum(valeur_lmtd)
        when kpi='Durée moyenne de réponse numérique' then sum(valeur_lmtd)
        when kpi='dont sortant (~recharges)' then sum(valeur_lmtd)
        when kpi='Engagements' then sum(valeur_lmtd)
        when kpi='Revenue Orange Money' then sum(valeur_lmtd)
        when kpi='Nombre de Pos Airtime actif' then sum(valeur_lmtd)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(valeur_lmtd)
        when kpi='Cash Out valeur_lmtd' then sum(valeur_lmtd)
        when kpi='Price Per Megas' then avg(valeur_lmtd)
        when kpi='Gross Adds' then sum(valeur_lmtd)
        when kpi='dont Voix' then sum(valeur_lmtd)
        when kpi='Pourcentage traité en numérique' then sum(valeur_lmtd)
        when kpi='Recharges (C2S, CAG, OM)' then sum(valeur_lmtd)
        else sum(valeur_lmtd)
    END) valeur_lmtd,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(valeur_lweek)
        when kpi='Revenue Data Mobile' then sum(valeur_lweek)
        when kpi='Tx users data' then avg(valeur_lweek)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(valeur_lweek)
        when kpi='Churn' then sum(valeur_lweek)
        when kpi='Net adds' then sum(valeur_lweek)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(valeur_lweek)
        when kpi='Cash In valeur_lweek' then sum(valeur_lweek)
        when kpi='Subscriber base' then sum(valeur_lweek)
        when kpi='Payments(Bill, Merch)' then sum(valeur_lweek)
        when kpi='Tx users (30jrs)' then avg(valeur_lweek)
        when kpi='Self Top UP ratio (%)' then avg(valeur_lweek)
        when kpi='Users (30jrs, >1Mo)' then sum(valeur_lweek)
        when kpi='Durée moyenne de réponse numérique' then sum(valeur_lweek)
        when kpi='dont sortant (~recharges)' then sum(valeur_lweek)
        when kpi='Engagements' then sum(valeur_lweek)
        when kpi='Revenue Orange Money' then sum(valeur_lweek)
        when kpi='Nombre de Pos Airtime actif' then sum(valeur_lweek)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(valeur_lweek)
        when kpi='Cash Out valeur_lweek' then sum(valeur_lweek)
        when kpi='Price Per Megas' then avg(valeur_lweek)
        when kpi='Gross Adds' then sum(valeur_lweek)
        when kpi='dont Voix' then sum(valeur_lweek)
        when kpi='Pourcentage traité en numérique' then sum(valeur_lweek)
        when kpi='Recharges (C2S, CAG, OM)' then sum(valeur_lweek)
        else sum(valeur_lweek)
    END) budget,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(valeur_budget_mtd)
        when kpi='Revenue Data Mobile' then sum(valeur_budget_mtd)
        when kpi='Tx users data' then avg(valeur_budget_mtd)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(valeur_budget_mtd)
        when kpi='Churn' then sum(valeur_budget_mtd)
        when kpi='Net adds' then sum(valeur_budget_mtd)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(valeur_budget_mtd)
        when kpi='Cash In valeur_budget_mtd' then sum(valeur_budget_mtd)
        when kpi='Subscriber base' then sum(valeur_budget_mtd)
        when kpi='Payments(Bill, Merch)' then sum(valeur_budget_mtd)
        when kpi='Tx users (30jrs)' then avg(valeur_budget_mtd)
        when kpi='Self Top UP ratio (%)' then avg(valeur_budget_mtd)
        when kpi='Users (30jrs, >1Mo)' then sum(valeur_budget_mtd)
        when kpi='Durée moyenne de réponse numérique' then sum(valeur_budget_mtd)
        when kpi='dont sortant (~recharges)' then sum(valeur_budget_mtd)
        when kpi='Engagements' then sum(valeur_budget_mtd)
        when kpi='Revenue Orange Money' then sum(valeur_budget_mtd)
        when kpi='Nombre de Pos Airtime actif' then sum(valeur_budget_mtd)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(valeur_budget_mtd)
        when kpi='Cash Out valeur_budget_mtd' then sum(valeur_budget_mtd)
        when kpi='Price Per Megas' then avg(valeur_budget_mtd)
        when kpi='Gross Adds' then sum(valeur_budget_mtd)
        when kpi='dont Voix' then sum(valeur_budget_mtd)
        when kpi='Pourcentage traité en numérique' then sum(valeur_budget_mtd)
        when kpi='Recharges (C2S, CAG, OM)' then sum(valeur_budget_mtd)
        else sum(valeur_budget_mtd)
    END)   budget_mtd,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(valeur_mtd_last_year)
        when kpi='Revenue Data Mobile' then sum(valeur_mtd_last_year)
        when kpi='Tx users data' then avg(valeur_mtd_last_year)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(valeur_mtd_last_year)
        when kpi='Churn' then sum(valeur_mtd_last_year)
        when kpi='Net adds' then sum(valeur_mtd_last_year)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(valeur_mtd_last_year)
        when kpi='Cash In valeur_mtd_last_year' then sum(valeur_mtd_last_year)
        when kpi='Subscriber base' then sum(valeur_mtd_last_year)
        when kpi='Payments(Bill, Merch)' then sum(valeur_mtd_last_year)
        when kpi='Tx users (30jrs)' then avg(valeur_mtd_last_year)
        when kpi='Self Top UP ratio (%)' then avg(valeur_mtd_last_year)
        when kpi='Users (30jrs, >1Mo)' then sum(valeur_mtd_last_year)
        when kpi='Durée moyenne de réponse numérique' then sum(valeur_mtd_last_year)
        when kpi='dont sortant (~recharges)' then sum(valeur_mtd_last_year)
        when kpi='Engagements' then sum(valeur_mtd_last_year)
        when kpi='Revenue Orange Money' then sum(valeur_mtd_last_year)
        when kpi='Nombre de Pos Airtime actif' then sum(valeur_mtd_last_year)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(valeur_mtd_last_year)
        when kpi='Cash Out valeur_mtd_last_year' then sum(valeur_mtd_last_year)
        when kpi='Price Per Megas' then avg(valeur_mtd_last_year)
        when kpi='Gross Adds' then sum(valeur_mtd_last_year)
        when kpi='dont Voix' then sum(valeur_mtd_last_year)
        when kpi='Pourcentage traité en numérique' then sum(valeur_mtd_last_year)
        when kpi='Recharges (C2S, CAG, OM)' then sum(valeur_mtd_last_year)
        else sum(valeur_mtd_last_year)
    END)  mtd_last_year,
    avg(vslweek/100) vslweek,
    avg(vs2wa/100) vs2wa,
    avg(vs3wa/100) vs3wa,
    avg(vs4wa/100) vs4wa,
    avg(mtdvslmdt/100) mtdvslmdt,
    avg(mdtvsbudget/100) mdtvsbudget,
    avg(mtd_vs_last_year/100) mtd_vs_last_year ,
     processing_date
     from MON.SPARK_KPIS_REG_FINAL
     group by
     category,
     kpi,
     axe_revenue,
     axe_subscriber,
     axe_regionale,
     processing
     order by processing_date desc


     select
     region_administrative,
     region_commerciale,
     category,
     kpi,
     axe_revenue,
     axe_subscriber,
     axe_regionale,
     valeur,
     valeur_lweek lweek,
     v2wa wa2,
     v3wa wa3,
     v4wa wa4,
     valeur_mtd,
     valeur_lmtd,
     budget,
     valeur_budget_mtd  budget_mtd,
     valeur_mtd_last_year mtd_last_year,
     vslweek/100 vslweek,
     vs2wa/100 vs2wa,
     vs3wa/100 vs3wa,
     vs4wa/100 vs4wa,
     mtdvslmdt/100 mtdvslmdt,
     mdtvsbudget/100 mdtvsbudget,
     mtd_vs_last_year/100 mtd_vs_last_year ,
     processing_date
     from MON.SPARK_KPIS_REG_FINAL
     order by processing_date desc

IPP Orange Woila L",IPP Orange Woila M",IPP Orange Woila",IPP Magic 100",IPP Magic 150",IPP Magic 350",IPP Super Woila 2jrs",IPP Super Woila 3jrs",IPP Super Woila 30jrs",IPP Orange Woila CBM Glissant",IPP Orange Woila L Glissant",IPP Orange Woila M Glissant",IPP Orange Woila XL Glissant",IPP Orange Woila XXL Glissant",   
