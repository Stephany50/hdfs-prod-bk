create table MON.FT_CBM_BUNDLE_SUBS_DAILY
(

	 MSISDN  VARCHAR(40),

	 BDLE_COST  BIGINT,

	 NBER_PURCHASE  BIGINT,

	 BDLE_NAME  VARCHAR(50),

	 VALIDITY  BIGINT,

	 SUBSCRIPTION_CHANNEL  VARCHAR(200),

	 AMOUNT_VOICE_ONNET  BIGINT,

	 AMOUNT_VOICE_OFFNET  BIGINT,

	 AMOUNT_VOICE_INTER  BIGINT,

	 AMOUNT_VOICE_ROAMING  BIGINT,

	 AMOUNT_SMS_ONNET  BIGINT,

	 AMOUNT_SMS_OFFNET  BIGINT,

	 AMOUNT_SMS_INTER  BIGINT,

	 AMOUNT_SMS_ROAMING  BIGINT,

	 AMOUNT_DATA  BIGINT,

	 AMOUNT_SVA  BIGINT,
	 INSERT_DATE TIMESTAMP

   ) COMMENT 'FT_CBM_BUNDLE_SUBS_DAILY table'
PARTITIONED BY (PERIOD DATE)
TBLPROPERTIES ("orc.compress"="ZLIB","orc.stripe.size"="67108864")