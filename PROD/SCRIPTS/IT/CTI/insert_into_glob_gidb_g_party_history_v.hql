INSERT INTO CTI.GLOB_GIDB_G_PARTY_HISTORY_V
SELECT
    PHID,
    PARTYID,
    PSEQ,
    CSEQ,
    CHANGETYPE,
    CCEVENT,
    CCEVENTCAUSE,
    STATE,
    TYPE,
    PREVSTATE,
    PREVSENTER,
    PREVSENTER_TS,
    PARENTPARTYID,
    PARENTLINKTYPE,
    ENDPOINTID,
    ADDED,
    ADDED_TS,
    GSYS_DOMAIN,
    GSYS_SYS_ID,
    GSYS_EXT_VCH1,
    GSYS_EXT_VCH2,
    GSYS_EXT_INT1,
    GSYS_EXT_INT2,
    CREATE_AUDIT_KEY,
    ORIGINAL_FILE_NAME,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
	TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
	CURRENT_TIMESTAMP() INSERT_DATE
FROM CTI.TT_EXP_GLOB_GIDB_G_PARTY_HISTORY_V C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CTI.GLOB_GIDB_G_PARTY_HISTORY_V)T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL;