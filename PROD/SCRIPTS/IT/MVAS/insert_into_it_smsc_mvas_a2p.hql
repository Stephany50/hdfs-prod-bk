INSERT INTO TABLE CDR.IT_SMSC_MVAS_A2P PARTITION(WRITE_DATE)
select
    SM_ID,
    ORGADDR,
    DESTADDR,
    SCADDR,
    MTMSCADDR,
    ESM_CLASS,
    PRI,
    MMS,
    RP,
    UDHI,
    SRR,
    PID,
    DCS,
    from_unixtime(unix_timestamp(trim(DEVERYDATE))) DEVERYDATE,
    UDL,
    RESULT,
    CS,
    FCS,
    MWS,
    nvl(from_unixtime(unix_timestamp(trim(nvl(WRITETIME, '')))), from_unixtime(unix_timestamp(substr(ORIGINAL_FILE_NAME, 4, 8), 'yyyyMMdd'))) WRITETIME,
    SERVICETYPE,
    DWDELIVERCOUNTS,
    STATUSREPORT,
    UCPPSUSER,
    FDBCOUNT,
    ORGACCOUNT,
    DESTACCOUNT,
    ULSERVICEFLAG,
    SZRAWORGADDRESS,
    SZRAWDESTADDRESS,
    ISLAST,
    SUB_ID,
    from_unixtime(unix_timestamp(trim(SUBMITTIME))) SUBMITTIME,
    UCSMSTATUS,
    DWTIMESPAN,
    UD,
    DWSUBMITMULTIID,
    RD,
    ORIGINALGROUP,
    PROFILE_ID,
    PROFILE_LEVEL,
    DWORGCOMMANDID,
    SCADDRTYPE,
    MOMSCADDRTYPE,
    MOMSCADDR,
    MOMSCTON,
    MOMSCNPI,
    MTMSCADDRTYPE,
    MTMSCTON,
    MTMSCNPI,
    ORGTON,
    ORGNPI,
    DESTTON,
    DESTNPI,
    RAWORGTON,
    RAWORGNPI,
    RAWDESTTON,
    RAWDESTNPI,
    LASTERROR1,
    LASTERROR2,
    LASTERROR3,
    LASTERROR4,
    LASTERROR5,
    DESTNETTYPE,
    DESTIFTYPE,
    IFFORWARD,
    ERRORSOURCE,
    DESTIMSIADDR,
    UDENCRYPTTYPE,
    MTSGSNADDR,
    MTSGSNTON,
    MTSGSNNPI,
    ISGETROUTINGSM,
    ORGOPID,
    DESTOPID,
    NETWORKERRORCODE,
    RNCHANGED,
    ORGOCS,
    DESTOCS,
    MESSAGETYPE,
    ORGIMSIADDR,
    BILLINGIDENTIFICATION,
    ANTISPAMMINGCHECKRESULT,
    FILTERIDAVP,
    SM_CONTENTKEYWORD,
    RN,
    MN,
    SN,
    JAMFLAG,
    JAMSMID,
    DESTSCCPADDR,
    PROFILE_MODE,
    EXPIRE,
    REALTIMERATED,
    SERVICECTRLRESULT,
    SERVICECONTROLRESULTCODE,
    CGIADDR,
    IMEIADDR,
    SUBMISSIONTIMEOPTIONAL,
    SMBUFFERINGFORANTISPAM,
    ORGSMID,
    MSGREFERENCENUMBER,
    TOTALSEGMENT,
    SEGMENTSEQUENCE,
    MTFORWARDTIME,
    MTFORWARDACKTIME,
    DELIVERYDURATION,
    CHARGENUMBER,
    CALLEDCELLID,
    ONDELIVERORGADDRESS,
    ONDELIVERORGTON,
    ONDELIVERORGNPI,
    ONDELIVERDESTADDRESS,
    ONDELIVERDESTTON,
    ONDELIVERDESTNPI,
    DST,
    ISORIGINALUSERROAMING,
    ISDESTUSERROAMING,
    ORIGINALDIAMETERERROR,
    ALPHA_TAG,
    ATBILLINGIDENTIFICATION,
    OUTPUTCAUSE,
    CALLINGPARTYSYSTEMTYPE,
    null CALLEDPARTYSYSTEMTYPE,
    null RESERVED,
    null FAKINGIMSIADDR,
    null SRICALLINGSCCP,
    null MTSUBMITCALLINGSCCP,
    null TARIFF_CLASS,
    null MNOID,
    null MVNOID,
    null EXPANDEDSMPPSMID,
    null ALERTSCFLAG,
    null ALERTSCTIME,
    null STATUSREPORTTYPE,
    null CALLERCONSOLIDATION,
    null CALLEECONSOLIDATION,
    null CSORIGMSG,
    MR,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
    ORIGINAL_FILE_NAME,
    to_date(from_unixtime(unix_timestamp(substr(ORIGINAL_FILE_NAME, 4, 8), 'yyyyMMdd'))) ORIGINAL_FILE_DATE,
    CURRENT_TIMESTAMP() INSERT_DATE,
    to_date(nvl(from_unixtime(unix_timestamp(trim(nvl(WRITETIME, '')))), from_unixtime(unix_timestamp(substr(ORIGINAL_FILE_NAME, 4, 8), 'yyyyMMdd')))) WRITE_DATE
from CDR.TT_SMSC_MVAS_A2P C
-- LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.IT_SMSC_MVAS_A2P WHERE WRITE_DATE BETWEEN DATE_SUB(CURRENT_DATE,3) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
-- WHERE T.FILE_NAME IS NULL;