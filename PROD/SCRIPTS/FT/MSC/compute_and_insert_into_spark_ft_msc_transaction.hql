INSERT INTO MON.SPARK_FT_MSC_TRANSACTION PARTITION(TRANSACTION_DATE)
SELECT
    DATE_FORMAT(cra_datetime, 'HHmmss')  Transaction_Time
    , CALL_TYPE Transaction_Direction
    , transaction_type Transaction_Type
    , msisdn Served_MSISDN
    , imsi Served_IMSI
    , IMEI Served_IMEI
    , MS_LOCATION Served_Party_Location
    , (CASE
        WHEN subs_type <> 'ROAM'  AND  ( LENGTH (other_party) <= 6 OR other_party IN ('99900500', '99900999'))  THEN 'PREP/POST'
        ELSE subs_type
       END ) Subscriber_Type
    , other_party Other_Party
    , IF (FN_GET_NNP_MSISDN_SIMPLE_DESTN (other_party) = 'INTERNATIONAL', 0, 1) Other_Party_Is_National
    , PARTNER_GT Partner_GT
    , IF (FN_GET_NNP_MSISDN_SIMPLE_DESTN (PARTNER_GT) = 'INTERNATIONAL', 0, 1) Partner_GT_Is_National
    , duration Transaction_Duration
    , duration Transaction_Volume
    , 'ss' Measurement_unit
    , TERM_TYPE Transaction_Term_code
    , TRUNCK_IN Trunck_in
    , TRUNCK_OUT Trunck_out
    , CRA_MSC_ID Msc_Adress
    , CRA_FICHIER Msc_Source_File
    , 'MSCHUA' Data_source
    , servicecentre Service_Centre
    , basic_serv Transaction_Service_Code
    , rec_type Record_Type
    , CURRENT_TIMESTAMP FT_Insert_date
    , INSERT_DATE IT_Insert_date
    , ORIGINAL_FILE_NAME Origin_filename
    , CALLINGNUMBER Old_Calling_Number
    , CALLEDNUMBER Old_Called_Number
    , ROAMINGNUMBER Roaming_Number
    , doublon_count Initial_Doublon_Count
    , gsmscfaddr Raw_gsmscfaddr
    , userType Raw_usertype
    , levelCAMELSvc Raw_levelcamelsvc
    , OTHER_PARTY_TRANSLATED_NUM
    , NETCALLREF
    , CALLER_PFLAG
    , CALLED_PFLAG
    , RN
    , TO_DATE(cra_datetime) Transaction_Date
FROM
    (
      SELECT
        MAX ( FN_HUA_TRANSACTION_TYPE(teleservice , rectype , systemtype
            , callednumber , callingnumber, intrunkgroup, outtrunkgroup  ) ) transaction_type,
        MAX (FN_HUA_SUBS_TYPE(teleservice , rectype , usertype   , servedimsi , gsmscfaddr
                                          , levelcamelsvc , gsmscfaddr2, cmlmodif  )) subs_type ,
        MAX (FN_HUA_CALL_TYPE (rectype, intrunkgroup, outtrunkgroup ))  CALL_TYPE,
        FN_HUA_CALLER_SUBR(rectype, callingnumber, servedmsisdn
            , callednumber, intrunkgroup, outtrunkgroup) msisdn,
        FN_HUA_PARTNER_ID (rectype, destinanumber, origination, callednumber
            , callingnumber, intrunkgroup, outtrunkgroup) other_party,
        MAX (FN_HUA_PARTNER_GT (rectype , servicecentre, destinanumber, origination, callednumber, callingnumber, intrunkgroup, outtrunkgroup)) PARTNER_GT,
        FN_GET_NNP_MSISDN_9DIGITS (CALLEDNUMBER) CALLEDNUMBER,
        FN_GET_NNP_MSISDN_9DIGITS (CALLINGNUMBER) CALLINGNUMBER,
        FN_GET_NNP_MSISDN_9DIGITS (SERVICECENTRE) SERVICECENTRE,
        FN_GET_NNP_MSISDN_9DIGITS (ROAMINGNUMBER)  ROAMINGNUMBER,
        FN_GET_NNP_MSISDN_9DIGITS (TRANSLATEDNUM)  OTHER_PARTY_TRANSLATED_NUM,
        NQ_CALLDATE cra_datetime,
        RECTYPE rec_type,
        NVL (CALLDURATION, 0) duration ,
        SERVEDIMSI imsi,
        TELESERVICE basic_serv,
        MAX(NVL (CAUSEFORTERM, 0)) TERM_TYPE,
        NULL MSC_TYPE,
        MAX(MSCADDRESS) CRA_MSC_ID,
        MAX(GLOBALAREAID) MS_LOCATION,
        MAX(SERVEDIMEI) IMEI,
        MIN(OUTTRUNKGROUP ) TRUNCK_OUT,
        MAX(INTRUNKGROUP) TRUNCK_IN,
        MAX(SOURCE) CRA_FICHIER,
        NULL type_heure,
        MAX (INSERT_DATE) INSERT_DATE,
        MAX (ORIGINAL_FILE_NAME) ORIGINAL_FILE_NAME,
        MAX (gsmscfaddr) gsmscfaddr,
        MAX (userType) userType,
        MAX (levelCAMELSvc) levelCAMELSvc,
        SUM (1) doublon_count,
        NETCALLREF,
        CALLER_PFLAG,
        CALLED_PFLAG,
        RN
        FROM
           CDR.SPARK_IT_CRA_MSC_HUAWEI a
        WHERE
           CALLDATE = '###SLICE_VALUE###'
           AND SUBSTR(nvl(ROAMINGNUMBER,'99999999'),1,7) NOT IN ('2371601','2371603')
        GROUP BY
            FN_HUA_CALLER_SUBR(rectype, callingnumber, servedmsisdn
                , callednumber, intrunkgroup, outtrunkgroup) , 
            FN_HUA_PARTNER_ID (rectype, destinanumber, origination, callednumber
                , callingnumber, intrunkgroup, outtrunkgroup) , -- 5
            NQ_CALLDATE , 
            CALLDATE , 
            RECTYPE , 
            NVL (CALLDURATION, 0), 
            SERVEDIMSI, 
            TELESERVICE, 
            FN_GET_NNP_MSISDN_9DIGITS (CALLEDNUMBER)  ,
            FN_GET_NNP_MSISDN_9DIGITS (CALLINGNUMBER)  ,
            FN_GET_NNP_MSISDN_9DIGITS (SERVICECENTRE)  ,
            FN_GET_NNP_MSISDN_9DIGITS (ROAMINGNUMBER) ,
            FN_GET_NNP_MSISDN_9DIGITS (TRANSLATEDNUM),
            NETCALLREF,
            CALLER_PFLAG, 
            CALLED_PFLAG, 
            RN 
    ) a