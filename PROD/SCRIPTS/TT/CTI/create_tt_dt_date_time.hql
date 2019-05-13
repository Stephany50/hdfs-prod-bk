---***********************************************************---
------------ External Table-TT CTI.DT_DATE_TIME -----------
---***********************************************************---

CREATE EXTERNAL TABLE IF NOT EXISTS CTI.TT_DT_DATE_TIME (

DATE_TIME_KEY               VARCHAR(32),   
DATE_TIME_30MIN_KEY         VARCHAR(32),   
DATE_TIME_HOUR_KEY          VARCHAR(32),   
DATE_TIME_DAY_KEY           VARCHAR(32),   
DATE_TIME_WEEK_KEY          VARCHAR(32),   
DATE_TIME_MONTH_KEY         VARCHAR(32),   
DATE_TIME_QUARTER_KEY       VARCHAR(32),   
DATE_TIME_YEAR_KEY          VARCHAR(32),   
DATE_TIME_NEXT_KEY          VARCHAR(32),   
DATE_TIME_NEXT_30MIN_KEY    VARCHAR(32),   
DATE_TIME_NEXT_HOUR_KEY     VARCHAR(32),   
DATE_TIME_NEXT_DAY_KEY      VARCHAR(32),   
DATE_TIME_NEXT_WEEK_KEY     VARCHAR(32),   
DATE_TIME_NEXT_MONTH_KEY    VARCHAR(32),   
DATE_TIME_NEXT_QUARTER_KEY  VARCHAR(32),   
DATE_TIME_NEXT_YEAR_KEY     VARCHAR(32),   
CREATE_AUDIT_KEY            VARCHAR(32),   
UPDATE_AUDIT_KEY            VARCHAR(32),   
CAL_DATE                    VARCHAR(32),         
CAL_DAY_NAME                VARCHAR(32), 
CAL_MONTH_NAME              VARCHAR(32), 
CAL_DAY_NUM_IN_WEEK         VARCHAR(32),    
CAL_DAY_NUM_IN_MONTH        VARCHAR(32),    
CAL_DAY_NUM_IN_YEAR         VARCHAR(32),    
CAL_LAST_DAY_IN_WEEK        VARCHAR(32),    
CAL_LAST_DAY_IN_MONTH       VARCHAR(32),    
CAL_WEEK_NUM_IN_YEAR        VARCHAR(32),    
WEEK_YEAR                   VARCHAR(32),    
CAL_WEEK_START_DATE         VARCHAR(32),         
CAL_WEEK_END_DATE           VARCHAR(32),         
CAL_MONTH_NUM_IN_YEAR       VARCHAR(32),    
CAL_QUARTER_NUM_IN_YEAR     VARCHAR(32),    
CAL_HALF_NUM_IN_YEAR        VARCHAR(32),    
CAL_YEAR_NUM                VARCHAR(32),    
CAL_HOUR_NUM_IN_DAY         VARCHAR(32),    
CAL_HOUR_24_NUM_IN_DAY      VARCHAR(32),    
CAL_MINUTE_NUM_IN_HOUR      VARCHAR(32),    
CAL_30MINUTE_NUM_IN_HOUR    VARCHAR(32),    
LABEL_YYYY                  VARCHAR(32), 
LABEL_YYYY_QQ               VARCHAR(32), 
LABEL_YYYY_MM               VARCHAR(32), 
LABEL_YYYY_WE               VARCHAR(32), 
LABEL_YYYY_WE_D             VARCHAR(32), 
LABEL_YYYY_MM_DD            VARCHAR(32), 
LABEL_YYYY_MM_DD_HH         VARCHAR(32), 
LABEL_YYYY_MM_DD_HH24       VARCHAR(32), 
LABEL_YYYY_MM_DD_HH_30MI    VARCHAR(32), 
LABEL_YYYY_MM_DD_HH24_30MI  VARCHAR(32), 
LABEL_YYYY_MM_DD_HH_MI      VARCHAR(32), 
LABEL_YYYY_MM_DD_HH24_MI    VARCHAR(32), 
LABEL_YYYY_MM_DD_HH_15INT   VARCHAR(32), 
LABEL_YYYY_MM_DD_HH24_15INT VARCHAR(32), 
LABEL_YYYY_MM_DD_HH_30INT   VARCHAR(32), 
LABEL_YYYY_MM_DD_HH24_30INT VARCHAR(32), 
LABEL_QQ                    VARCHAR(32), 
LABEL_MM                    VARCHAR(32), 
LABEL_WE                    VARCHAR(32), 
LABEL_DD                    VARCHAR(32), 
LABEL_HH                    VARCHAR(32), 
LABEL_HH24                  VARCHAR(32), 
LABEL_30MI                  VARCHAR(32), 
LABEL_MI                    VARCHAR(32), 
LABEL_TZ                    VARCHAR(32), 
AMPM_INDICATOR              VARCHAR(32),  
RUNNING_YEAR_NUM            VARCHAR(32),   
RUNNING_QUARTER_NUM         VARCHAR(32),   
RUNNING_MONTH_NUM           VARCHAR(32),   
RUNNING_WEEK_NUM            VARCHAR(32),   
RUNNING_DAY_NUM             VARCHAR(32),   
RUNNING_HOUR_NUM            VARCHAR(32),   
RUNNING_30MIN_NUM           VARCHAR(32),   
ORIGINAL_FILE_NAME          VARCHAR(200) ,
ORIGINAL_FILE_SIZE               INT,
ORIGINAL_FILE_LINE_COUNT         INT
)
COMMENT 'EXP_DIM_CTL_AUDIT_LOG external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/CTI/EXP_DIM_DATE_TIME/'
TBLPROPERTIES ('serialization.null.format'='')