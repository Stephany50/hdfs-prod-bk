CREATE TABLE  MON.FT_CELL_TRAFIC_DAYLY
   (
	 MS_LOCATION  VARCHAR(60),
	 MSISDN_COUNT  bigint,
	 FAIL_IN  bigint,
	 FAIL_OUT  bigint,
	 DUREE_SORTANT  bigint,
	 NBRE_TEL_SORTANT  bigint,
	 DUREE_ENTRANT  bigint,
	 NBRE_TEL_ENTRANT  bigint,
	 NBRE_SMS_SORTANT  bigint,
	 NBRE_SMS_ENTRANT  bigint,
	 TOTAL_DUREE_SORTANT  bigint,
	 TOTAL_DUREE_ENTRANT  bigint,
	 TOTAL_NBRE_SORTANT  bigint,
	 TOTAL_NBRE_ENTRANT  bigint,
	 REFRESH_DATE  timestamp,
	 OPERATOR_CODE  string,
	 NBRE_TEL_MTN_SORTANT  bigint,
	 NBRE_TEL_CAMTEL_SORTANT  bigint,
	 NBRE_TEL_OCM_SORTANT  bigint,
	 DUREE_TEL_MTN_SORTANT  bigint,
	 DUREE_TEL_CAMTEL_SORTANT  bigint,
	 DUREE_TEL_OCM_SORTANT  bigint,
	 NBRE_SMS_MTN_SORTANT  bigint,
	 NBRE_SMS_CAMTEL_SORTANT  bigint,
	 NBRE_SMS_OCM_SORTANT  bigint,
	 NBRE_SMS_ZEBRA_SORTANT  bigint,
	 NBRE_TEL_MTN_ENTRANT  bigint,
	 NBRE_TEL_CAMTEL_ENTRANT  bigint,
	 NBRE_TEL_OCM_ENTRANT  bigint,
	 DUREE_TEL_MTN_ENTRANT  bigint,
	 DUREE_TEL_CAMTEL_ENTRANT  bigint,
	 DUREE_TEL_OCM_ENTRANT  bigint,
	 NBRE_SMS_MTN_ENTRANT  bigint,
	 NBRE_SMS_CAMTEL_ENTRANT  bigint,
	 NBRE_SMS_OCM_ENTRANT  bigint,
	 NBRE_SMS_ZEBRA_ENTRANT  bigint,
	 NBRE_TEL_NEXTTEL_SORTANT  bigint,
	 DUREE_TEL_NEXTTEL_SORTANT  bigint,
	 NBRE_SMS_NEXTTEL_SORTANT  bigint,
	 NBRE_TEL_NEXTTEL_ENTRANT  bigint,
	 DUREE_TEL_NEXTTEL_ENTRANT  bigint,
	 NBRE_SMS_NEXTTEL_ENTRANT  bigint,
	 PROFILE_NAME  VARCHAR(50),
	 CONTRACT_TYPE  VARCHAR(50)
   ) COMMENT 'FT_CELL_TRAFIC_DAYLY - FT'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS ORC TBLPROPERTIES ( orc.compress = ZLIB , orc.stripe.size = 67108864 )