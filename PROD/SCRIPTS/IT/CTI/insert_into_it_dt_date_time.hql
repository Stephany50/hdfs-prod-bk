INSERT INTO CTI.IT_DT_DATE_TIME
SELECT
DATE_TIME_KEY               ,   
DATE_TIME_30MIN_KEY         ,   
DATE_TIME_HOUR_KEY          ,   
DATE_TIME_DAY_KEY           ,   
DATE_TIME_WEEK_KEY          ,   
DATE_TIME_MONTH_KEY         ,   
DATE_TIME_QUARTER_KEY       ,   
DATE_TIME_YEAR_KEY          ,   
DATE_TIME_NEXT_KEY          ,   
DATE_TIME_NEXT_30MIN_KEY    ,   
DATE_TIME_NEXT_HOUR_KEY     ,   
DATE_TIME_NEXT_DAY_KEY      ,   
DATE_TIME_NEXT_WEEK_KEY     ,   
DATE_TIME_NEXT_MONTH_KEY    ,   
DATE_TIME_NEXT_QUARTER_KEY  ,   
DATE_TIME_NEXT_YEAR_KEY     ,   
CREATE_AUDIT_KEY            ,   
UPDATE_AUDIT_KEY            ,   
CAL_DATE                    ,         
CAL_DAY_NAME               , 
CAL_MONTH_NAME             , 
CAL_DAY_NUM_IN_WEEK         ,    
CAL_DAY_NUM_IN_MONTH        ,    
CAL_DAY_NUM_IN_YEAR         ,    
CAL_LAST_DAY_IN_WEEK        ,    
CAL_LAST_DAY_IN_MONTH       ,    
CAL_WEEK_NUM_IN_YEAR        ,    
WEEK_YEAR                   ,    
CAL_WEEK_START_DATE         ,        
CAL_WEEK_END_DATE           ,         
CAL_MONTH_NUM_IN_YEAR       ,    
CAL_QUARTER_NUM_IN_YEAR     ,    
CAL_HALF_NUM_IN_YEAR        ,    
CAL_YEAR_NUM                ,    
CAL_HOUR_NUM_IN_DAY         ,    
CAL_HOUR_24_NUM_IN_DAY      ,    
CAL_MINUTE_NUM_IN_HOUR      ,    
CAL_30MINUTE_NUM_IN_HOUR    ,    
LABEL_YYYY                 , 
LABEL_YYYY_QQ              , 
LABEL_YYYY_MM              , 
LABEL_YYYY_WE              , 
LABEL_YYYY_WE_D            , 
LABEL_YYYY_MM_DD           , 
LABEL_YYYY_MM_DD_HH        , 
LABEL_YYYY_MM_DD_HH24      , 
LABEL_YYYY_MM_DD_HH_30MI   , 
LABEL_YYYY_MM_DD_HH24_30MI , 
LABEL_YYYY_MM_DD_HH_MI     , 
LABEL_YYYY_MM_DD_HH24_MI   , 
LABEL_YYYY_MM_DD_HH_15INT  , 
LABEL_YYYY_MM_DD_HH24_15INT, 
LABEL_YYYY_MM_DD_HH_30INT  , 
LABEL_YYYY_MM_DD_HH24_30INT, 
LABEL_QQ                   , 
LABEL_MM                   , 
LABEL_WE                   , 
LABEL_DD                   , 
LABEL_HH                   , 
LABEL_HH24                 , 
LABEL_30MI                 , 
LABEL_MI                   , 
LABEL_TZ                   , 
AMPM_INDICATOR             ,  
RUNNING_YEAR_NUM            ,   
RUNNING_QUARTER_NUM         ,   
RUNNING_MONTH_NUM           ,   
RUNNING_WEEK_NUM            ,   
RUNNING_DAY_NUM             ,   
RUNNING_HOUR_NUM            ,   
RUNNING_30MIN_NUM           , 	
ORIGINAL_FILE_NAME          ,
ORIGINAL_FILE_SIZE       ,
ORIGINAL_FILE_LINE_COUNT ,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
CURRENT_TIMESTAMP() INSERT_DATE
FROM CTI.TT_DT_DATE_TIME C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CTI.IT_DT_DATE_TIME)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL;