CREATE TABLE CDR.IT_SMSC_MVAS_A2P (
    SM_ID VARCHAR(14), 
    ORGADDR VARCHAR(41), 
    DESTADDR VARCHAR(41), 
    SCADDR VARCHAR(41), 
    MTMSCADDR VARCHAR(41), 
    ESM_CLASS INT, 
    PRI INT, 
    MMS INT, 
    RP INT, 
    UDHI INT, 
    SRR INT, 
    PID INT, 
    DCS INT, 
    DEVERYDATE TIMESTAMP, 
    UDL VARCHAR(14), 
    RESULT INT, 
    CS INT, 
    FCS INT, 
    MWS INT, 
    WRITETIME TIMESTAMP, 
    SERVICETYPE VARCHAR(11), 
    DWDELIVERCOUNTS VARCHAR(14), 
    STATUSREPORT INT, 
    UCPPSUSER VARCHAR(7), 
    FDBCOUNT VARCHAR(14), 
    ORGACCOUNT VARCHAR(50), 
    DESTACCOUNT VARCHAR(50), 
    ULSERVICEFLAG VARCHAR(14), 
    SZRAWORGADDRESS VARCHAR(100), 
    SZRAWDESTADDRESS VARCHAR(50), 
    ISLAST INT, 
    SUB_ID INT, 
    SUBMITTIME TIMESTAMP, 
    UCSMSTATUS VARCHAR(5), 
    DWTIMESPAN INT, 
    UD VARCHAR(400), 
    DWSUBMITMULTIID VARCHAR(14), 
    RD INT, 
    ORIGINALGROUP INT, 
    PROFILE_ID VARCHAR(14), 
    PROFILE_LEVEL VARCHAR(14), 
    DWORGCOMMANDID VARCHAR(14), 
    SCADDRTYPE INT, 
    MOMSCADDRTYPE INT, 
    MOMSCADDR BIGINT, 
    MOMSCTON INT, 
    MOMSCNPI INT, 
    MTMSCADDRTYPE INT, 
    MTMSCTON INT, 
    MTMSCNPI VARCHAR(50), 
    ORGTON INT, 
    ORGNPI INT, 
    DESTTON INT, 
    DESTNPI INT, 
    RAWORGTON INT, 
    RAWORGNPI INT, 
    RAWDESTTON INT, 
    RAWDESTNPI INT, 
    LASTERROR1 VARCHAR(7), 
    LASTERROR2 VARCHAR(7), 
    LASTERROR3 VARCHAR(7), 
    LASTERROR4 VARCHAR(7), 
    LASTERROR5 VARCHAR(7), 
    DESTNETTYPE INT, 
    DESTIFTYPE INT, 
    IFFORWARD INT, 
    ERRORSOURCE INT, 
    DESTIMSIADDR BIGINT, 
    UDENCRYPTTYPE INT, 
    MTSGSNADDR INT, 
    MTSGSNTON INT, 
    MTSGSNNPI INT, 
    ISGETROUTINGSM INT, 
    ORGOPID VARCHAR(41), 
    DESTOPID VARCHAR(41), 
    NETWORKERRORCODE INT, 
    RNCHANGED INT, 
    ORGOCS VARCHAR(41), 
    DESTOCS VARCHAR(41), 
    MESSAGETYPE INT, 
    ORGIMSIADDR BIGINT, 
    BILLINGIDENTIFICATION VARCHAR(42), 
    ANTISPAMMINGCHECKRESULT INT, 
    FILTERIDAVP INT, 
    SM_CONTENTKEYWORD VARCHAR(50), 
    RN VARCHAR(50), 
    MN VARCHAR(50), 
    SN VARCHAR(50), 
    JAMFLAG VARCHAR(50), 
    JAMSMID VARCHAR(50), 
    DESTSCCPADDR VARCHAR(50), 
    PROFILE_MODE VARCHAR(50), 
    EXPIRE VARCHAR(50), 
    REALTIMERATED VARCHAR(50), 
    SERVICECTRLRESULT VARCHAR(50), 
    SERVICECONTROLRESULTCODE VARCHAR(50), 
    CGIADDR VARCHAR(50), 
    IMEIADDR VARCHAR(50), 
    SUBMISSIONTIMEOPTIONAL VARCHAR(50), 
    SMBUFFERINGFORANTISPAM VARCHAR(50), 
    ORGSMID VARCHAR(50), 
    MSGREFERENCENUMBER VARCHAR(50), 
    TOTALSEGMENT VARCHAR(50), 
    SEGMENTSEQUENCE VARCHAR(50), 
    MTFORWARDTIME VARCHAR(50), 
    MTFORWARDACKTIME VARCHAR(50), 
    DELIVERYDURATION VARCHAR(50), 
    CHARGENUMBER VARCHAR(50), 
    CALLEDCELLID VARCHAR(50), 
    ONDELIVERORGADDRESS VARCHAR(50), 
    ONDELIVERORGTON VARCHAR(50), 
    ONDELIVERORGNPI VARCHAR(50), 
    ONDELIVERDESTADDRESS VARCHAR(50), 
    ONDELIVERDESTTON VARCHAR(50), 
    ONDELIVERDESTNPI VARCHAR(50), 
    DST VARCHAR(50), 
    ISORIGINALUSERROAMING VARCHAR(50), 
    ISDESTUSERROAMING VARCHAR(50), 
    ORIGINALDIAMETERERROR VARCHAR(50), 
    ALPHA_TAG VARCHAR(50), 
    ATBILLINGIDENTIFICATION VARCHAR(50), 
    OUTPUTCAUSE VARCHAR(50), 
    CALLINGPARTYSYSTEMTYPE VARCHAR(50), 
    CALLEDPARTYSYSTEMTYPE VARCHAR(50), 
    RESERVED VARCHAR(50), 
    FAKINGIMSIADDR VARCHAR(50), 
    SRICALLINGSCCP VARCHAR(50), 
    MTSUBMITCALLINGSCCP VARCHAR(50), 
    TARIFF_CLASS VARCHAR(50), 
    MNOID VARCHAR(50), 
    MVNOID VARCHAR(50), 
    EXPANDEDSMPPSMID VARCHAR(50), 
    ALERTSCFLAG VARCHAR(50), 
    ALERTSCTIME VARCHAR(50), 
    STATUSREPORTTYPE VARCHAR(50), 
    CALLERCONSOLIDATION VARCHAR(50), 
    CALLEECONSOLIDATION VARCHAR(50), 
    CSORIGMSG VARCHAR(50), 
    MR VARCHAR(50),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_DATE  DATE,
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (WRITE_DATE DATE)
CLUSTERED BY(SCADDR, ORGADDR, DESTADDR) INTO 64 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
