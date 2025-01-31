INSERT INTO MON.SPARK_FT_CBM_CUST_INSIGTH_DAILY PARTITION(PERIOD)
SELECT
	COALESCE(a.MSISDN, b.MSISDN) MSISDN,
	COALESCE(NB_CALLS, 0) NB_CALLS,
	COALESCE(ACTUAL_DURATION, 0) ACTUAL_DURATION,
	COALESCE(MOU_ONNET, 0) MOU_ONNET,
	COALESCE(MOU_OFNET, 0) MOU_OFNET,
	COALESCE(MOU_INTER, 0) MOU_INTER,
	COALESCE(MOU_ON_PEAK, 0) MOU_ON_PEAK,
	COALESCE(MOU_OF_PEAK, 0) MOU_OF_PEAK,
	COALESCE(MOU_INT_PEAK, 0) MOU_INT_PEAK,
	COALESCE(MOU_ON_OFFPEAK, 0) MOU_ON_OFFPEAK,
	COALESCE(MOU_OF_OFFPEAK, 0) MOU_OF_OFFPEAK,
	COALESCE(MOU_INT_OFFPEAK, 0) MOU_INT_OFFPEAK,
	COALESCE(FOU_SMS, 0) FOU_SMS,
	COALESCE(FOU_SMS_ONNET, 0) FOU_SMS_ONNET,
	COALESCE(FOU_SMS_OFNET, 0) FOU_SMS_OFNET,
	COALESCE(FOU_SMS_INTERNAT, 0) FOU_SMS_INTERNAT,
	COALESCE(SOI_ONNET, 0) SOI_ONNET,
	COALESCE(SOI_OFNET, 0) SOI_OFNET,
	COALESCE(SOI_INTER, 0) SOI_INTER,
	COALESCE(SOR_ONNET, 0) SOR_ONNET,
	COALESCE(SOR_OFNET, 0) SOR_OFNET,
	COALESCE(SOR_INTER, 0) SOR_INTER,
	COALESCE(MA_VOICE_ONNET, 0) MA_VOICE_ONNET,
	COALESCE(MA_VOICE_OFNET, 0) MA_VOICE_OFNET,
	COALESCE(MA_VOICE_INTER, 0) MA_VOICE_INTER,
	COALESCE(MA_SMS_ONNET, 0) MA_SMS_ONNET,
	COALESCE(MA_SMS_OFNET, 0) MA_SMS_OFNET,
	COALESCE(MA_SMS_INTER, 0) MA_SMS_INTER,
	COALESCE(MA_DATA, 0) MA_DATA,
	COALESCE(MA_VAS ,0) MA_VAS,
	COALESCE(INC_NB_CALLS,0) INC_NB_CALLS,
	COALESCE(INC_ONNET_NB_CALLS,0) INC_ONNET_NB_CALLS,
	COALESCE(INC_OFNET_NB_CALLS,0) INC_OFNET_NB_CALLS,
	COALESCE(INC_INTER_NB_CALLS,0) INC_INTER_NB_CALLS,
	COALESCE(INC_NB_SMS,0) INC_NB_SMS,
	COALESCE(BYTES_DATA,0) BYTES_DATA,
	COALESCE(MA_GOS_SVA,0) MA_GOS_SVA,
	COALESCE(MA_VOICE_ROAMING, 0) MA_VOICE_ROAMING,
	COALESCE(MA_VOICE_SVA, 0) MA_VOICE_SVA,
	COALESCE(MA_SMS_ROAMING, 0) MA_SMS_ROAMING,
	COALESCE(MA_SMS_SVA, 0) MA_SMS_SVA,
	COALESCE(INC_CALL_DURATION,0) INC_CALL_DURATION,
	COALESCE(INC_ONNET_NB_DURATION,0) INC_ONNET_NB_DURATION,
	COALESCE(INC_OFNET_NB_DURATION,0)  INC_OFNET_NB_DURATION,
	COALESCE(INC_INTER_NB_DURATION,0) INC_INTER_NB_DURATION,
	CURRENT_TIMESTAMP INSERT_DATE,
	'###SLICE_VALUE###' PERIOD
FROM
(
	SELECT
		COALESCE(a.MSISDN, b.MSISDN) MSISDN,
		COALESCE(NB_CALLS,0) NB_CALLS,
		COALESCE(ACTUAL_DURATION,0) ACTUAL_DURATION,
		COALESCE(MOU_ONNET,0) MOU_ONNET,
		COALESCE(MOU_OFNET,0) MOU_OFNET,
		COALESCE(MOU_INTER,0) MOU_INTER,
		COALESCE(MOU_ON_PEAK,0) MOU_ON_PEAK,
		COALESCE(MOU_OF_PEAK,0) MOU_OF_PEAK,
		COALESCE(MOU_INT_PEAK,0) MOU_INT_PEAK,
		COALESCE(MOU_ON_OFFPEAK,0) MOU_ON_OFFPEAK,
		COALESCE(MOU_OF_OFFPEAK,0) MOU_OF_OFFPEAK,
		COALESCE(MOU_INT_OFFPEAK,0) MOU_INT_OFFPEAK,
		COALESCE(FOU_SMS,0) FOU_SMS,
		COALESCE(FOU_SMS_ONNET,0) FOU_SMS_ONNET,
		COALESCE(FOU_SMS_OFNET,0) FOU_SMS_OFNET,
		COALESCE(FOU_SMS_INTERNAT,0) FOU_SMS_INTERNAT,
		COALESCE(ONNET_OUT_USERS_COUNT,0) SOI_ONNET,
		COALESCE(OFFNET_OUT_USERS_COUNT,0) SOI_OFNET,
		COALESCE(INTER_OUT_USERS_COUNT,0) SOI_INTER,
		COALESCE(ONNET_IN_USERS_COUNT,0) SOR_ONNET, 
		COALESCE(OFFNET_IN_USERS_COUNT,0) SOR_OFNET, 
		COALESCE(INTER_IN_USERS_COUNT,0) SOR_INTER,
		COALESCE(MA_VOICE_ONNET,0) MA_VOICE_ONNET,
		COALESCE(MA_VOICE_OFNET,0) MA_VOICE_OFNET,
		COALESCE(MA_VOICE_INTER,0) MA_VOICE_INTER,
		COALESCE(MA_SMS_ONNET,0) MA_SMS_ONNET,
		COALESCE(MA_SMS_OFNET,0) MA_SMS_OFNET,
		COALESCE(MA_SMS_INTER,0) MA_SMS_INTER,
		COALESCE(MA_VAS,0) MA_VAS,
		COALESCE(INC_NB_CALLS,0) INC_NB_CALLS,
		COALESCE(INC_ONNET_NB_CALLS,0) INC_ONNET_NB_CALLS,
		COALESCE(INC_OFNET_NB_CALLS,0) INC_OFNET_NB_CALLS,
		COALESCE(INC_INTER_NB_CALLS,0) INC_INTER_NB_CALLS,
		COALESCE(INC_NB_SMS,0) INC_NB_SMS,
		COALESCE(MA_VOICE_ROAMING,0) MA_VOICE_ROAMING,
		COALESCE(MA_VOICE_SVA,0) MA_VOICE_SVA,
		COALESCE(MA_SMS_ROAMING,0) MA_SMS_ROAMING,
		COALESCE(MA_SMS_SVA,0) MA_SMS_SVA,
		COALESCE(INC_CALL_DURATION,0) INC_CALL_DURATION,
		COALESCE(INC_ONNET_NB_DURATION,0) INC_ONNET_NB_DURATION,
		COALESCE(INC_OFNET_NB_DURATION,0) INC_OFNET_NB_DURATION,
		COALESCE(INC_INTER_NB_DURATION,0) INC_INTER_NB_DURATION
	FROM
	(
		SELECT 
			CHARGED_PARTY AS    MSISDN,
			SUM (CASE    WHEN  SERVICE_CODE = 'VOI_VOX' THEN  1  ELSE  0    END ) AS    NB_CALLS,
			SUM (DURATION) / 60 AS    ACTUAL_DURATION,
			SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') THEN DURATION ELSE 0 END ) / 60 AS    MOU_onnet,
			SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_NAT_MOB_MVO','OUT_NAT_MOB_NEX') THEN DURATION ELSE 0 END ) / 60 AS    MOU_ofnet,
			SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_INT') THEN DURATION ELSE 0 END ) / 60 AS    MOU_Inter,
			SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') AND TIME_SLICE = 'PEAK' THEN DURATION ELSE 0 END ) / 60 AS    MOU_on_Peak,
			SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_NAT_MOB_MVO','OUT_NAT_MOB_NEX') AND TIME_SLICE = 'PEAK' THEN DURATION ELSE 0 END ) / 60 AS    MOU_of_Peak,
			SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_INT') AND TIME_SLICE = 'PEAK' THEN DURATION ELSE 0 END ) / 60 AS    MOU_Int_Peak,
			SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') AND TIME_SLICE = 'OFF_PEAK' THEN DURATION ELSE 0 END ) / 60    MOU_on_OffPeak,
			SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_NAT_MOB_MVO','OUT_NAT_MOB_NEX') AND TIME_SLICE = 'OFF_PEAK' THEN DURATION ELSE 0 END ) / 60 AS    MOU_of_OffPeak,
			SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_INT') AND TIME_SLICE = 'OFF_PEAK' THEN DURATION ELSE 0 END ) / 60 AS    MOU_Int_OffPeak,
			SUM (CASE    WHEN  SERVICE_CODE = 'NVX_SMS' THEN  1  ELSE  0    END ) AS    FOU_SMS,
			SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') THEN 1 ELSE 0 END ) AS    FOU_SMS_ONNET,
			SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_NAT_MOB_MVO','OUT_NAT_MOB_NEX') THEN 1 ELSE 0 END ) AS    FOU_SMS_OFNET,
			SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_INT') THEN 1 ELSE 0 END ) AS    FOU_SMS_INTERNAT,
			SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') THEN MAIN_RATED_AMOUNT ELSE 0 END ) AS    MA_voice_onnet,
			SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_NAT_MOB_MVO','OUT_NAT_MOB_NEX') THEN MAIN_RATED_AMOUNT ELSE 0 END ) AS    MA_voice_ofnet,
			SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_INT') THEN MAIN_RATED_AMOUNT ELSE 0 END ) AS    MA_voice_Inter,
			SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') THEN MAIN_RATED_AMOUNT ELSE 0 END ) AS    MA_sms_onnet,
			SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_NAT_MOB_MVO','OUT_NAT_MOB_NEX') THEN MAIN_RATED_AMOUNT ELSE 0 END ) AS    MA_sms_ofnet,
			SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_INT') THEN MAIN_RATED_AMOUNT ELSE 0 END ) AS    MA_sms_Inter,
			--    MA_Data,
			SUM (CASE WHEN DEST_OPERATOR IN ('OUT_SVA','OUT_CCSVA') THEN MAIN_RATED_AMOUNT ELSE 0 END ) AS    MA_VAS,
			SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_ROAM_MT','OUT_ROAM_MO') THEN MAIN_RATED_AMOUNT ELSE 0 END ) AS    MA_voice_ROAMING,
			SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_SVA','OUT_CCSVA') THEN MAIN_RATED_AMOUNT ELSE 0 END ) AS    MA_voice_SVA,
			SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_ROAM_MT','OUT_ROAM_MO') THEN MAIN_RATED_AMOUNT ELSE 0 END ) AS    MA_sms_ROAMING,
			SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_SVA','OUT_CCSVA') THEN MAIN_RATED_AMOUNT ELSE 0 END ) AS    MA_sms_SVA
		FROM 
		(
			SELECT 
				(CASE 
					WHEN TRANSACTION_TIME BETWEEN '050000'  AND '215959'
						THEN  'PEAK'
					ELSE 'OFF_PEAK'
				END) AS TIME_SLICE, 
				fn_nnp_remove_rn(CHARGED_PARTY)CHARGED_PARTY, 
				fn_nnp_remove_rn(OTHER_PARTY) other_party, 
				OPERATOR_CODE, 
				(CASE
					WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
					WHEN SERVICE_CODE = 'TEL' THEN 'VOI_VOX'
					WHEN SERVICE_CODE = 'USS' THEN 'NVX_USS'
					WHEN SERVICE_CODE = 'GPR' THEN 'NVX_DAT_GPR'
					WHEN SERVICE_CODE = 'DFX' THEN 'NVX_DFX'
					WHEN SERVICE_CODE = 'DAT' THEN 'NVX_DAT'
					WHEN SERVICE_CODE = 'VDT' THEN 'NVX_VDT'
					WHEN SERVICE_CODE = 'WEB' THEN 'NVX_WEB'
					WHEN UPPER(SERVICE_CODE) IN ('SMSMO','SMSRMG') THEN 'NVX_SMS'
					WHEN UPPER(SERVICE_CODE) IN ('OC','OCFWD','OCRMG','TCRMG') THEN 'VOI_VOX'
					WHEN UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' THEN 'VOI_VOX'
					WHEN UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN 'VOI_VOX'
					ELSE 'AUT'
				END ) SERVICE_CODE, 
				( CASE 
					WHEN Call_Destination_Code IN ('ONNET','ONNETFREE','OCM_D') THEN 'OUT_NAT_MOB_OCM'
					WHEN Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
					WHEN Call_Destination_Code IN ('CAM_D','CAM') THEN 'OUT_NAT_MOB_CAM'
					WHEN Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
					WHEN Call_Destination_Code IN ('NEXTTEL','NEXTTEL_D') THEN 'OUT_NAT_MOB_NEX' --NEXTTEL
					WHEN Call_Destination_Code = 'VAS' THEN 'OUT_SVA'
					WHEN Call_Destination_Code = 'EMERG' THEN 'OUT_CCSVA'
					WHEN Call_Destination_Code = 'OCRMG' THEN 'OUT_ROAM_MO'
					WHEN Call_Destination_Code = 'TCRMG' THEN 'OUT_ROAM_MT'
					WHEN Call_Destination_Code = 'INT' THEN 'OUT_INT'
					WHEN Call_Destination_Code = 'MVNO' THEN 'OUT_NAT_MOB_MVO'
					ELSE Call_Destination_Code 
				END ) DEST_OPERATOR, 
				PROMO_RATED_AMOUNT, 
				MAIN_RATED_AMOUNT, 
				CALL_PROCESS_TOTAL_DURATION DURATION, 
				RATED_DURATION RATED_DURATION
			FROM MON.SPARK_FT_BILLED_TRANSACTION_PREPAID
			WHERE 
				TRANSACTION_DATE = '###SLICE_VALUE###'
				AND Main_Rated_Amount >= 0
				AND Promo_Rated_Amount >= 0
		) T1
		GROUP BY CHARGED_PARTY
	) a
	FULL JOIN
	(
		SELECT 
			fn_nnp_remove_rn(SERVED_MSISDN) MSISDN,
			COUNT(
				DISTINCT (CASE 
							WHEN TRANSACTION_DIRECTION = 'Entrant' AND SUBSTR(TRANSACTION_TYPE, 1, 3) = 'TEL' AND fn_get_nnp_msisdn_simple_destn(OTHER_PARTY) IN ('OCM') 
								THEN fn_nnp_remove_rn(OTHER_PARTY)
							ELSE NULL 
						END)
			) ONNET_IN_USERS_COUNT,
			COUNT(
				DISTINCT (CASE 
							WHEN TRANSACTION_DIRECTION = 'Entrant' AND SUBSTR(TRANSACTION_TYPE, 1, 3) = 'TEL' AND fn_get_nnp_msisdn_simple_destn(OTHER_PARTY) IN ('VIETTEL', 'MTN', 'CAMTEL', 'SET') 
								THEN fn_nnp_remove_rn(OTHER_PARTY)
							ELSE NULL 
						END)
			) OFFNET_IN_USERS_COUNT,
			COUNT(
				DISTINCT (CASE 
							WHEN TRANSACTION_DIRECTION = 'Entrant' AND SUBSTR(TRANSACTION_TYPE, 1, 3) = 'TEL' AND fn_get_nnp_msisdn_simple_destn(OTHER_PARTY) IN ('INTERNATIONAL') 
								THEN fn_nnp_remove_rn(OTHER_PARTY)
							ELSE NULL 
						END)
			) INTER_IN_USERS_COUNT,
			COUNT(
				DISTINCT (CASE 
							WHEN TRANSACTION_DIRECTION = 'Sortant' AND SUBSTR(TRANSACTION_TYPE, 1, 3) = 'TEL' AND fn_get_nnp_msisdn_simple_destn(OTHER_PARTY) IN ('OCM') 
								THEN fn_nnp_remove_rn(OTHER_PARTY)
							ELSE NULL
						END)
			) ONNET_OUT_USERS_COUNT,
			COUNT(
				DISTINCT (CASE 
							WHEN TRANSACTION_DIRECTION = 'Sortant' AND SUBSTR(TRANSACTION_TYPE, 1, 3) = 'TEL' AND fn_get_nnp_msisdn_simple_destn(OTHER_PARTY) IN ('VIETTEL', 'MTN', 'CAMTEL', 'SET') 
								THEN fn_nnp_remove_rn(OTHER_PARTY)
							ELSE NULL 
						END)
			) OFFNET_OUT_USERS_COUNT,
			COUNT(
				DISTINCT (CASE 
							WHEN TRANSACTION_DIRECTION = 'Sortant' AND SUBSTR(TRANSACTION_TYPE, 1, 3) = 'TEL' AND fn_get_nnp_msisdn_simple_destn(OTHER_PARTY) IN ('INTERNATIONAL') 
								THEN fn_nnp_remove_rn(OTHER_PARTY)
							ELSE NULL 
						END)
			) INTER_OUT_USERS_COUNT,               
			SUM(CASE 
				WHEN TRANSACTION_DIRECTION = 'Entrant' AND SUBSTR(TRANSACTION_TYPE, 1, 3) = 'TEL'
					THEN 1
				ELSE 0 
			END) INC_NB_CALLS,
			SUM(CASE 
				WHEN TRANSACTION_DIRECTION = 'Entrant' AND SUBSTR(TRANSACTION_TYPE, 1, 3) = 'TEL' AND fn_get_nnp_msisdn_simple_destn(OTHER_PARTY) IN ('OCM') 
					THEN 1
				ELSE 0 
			END) INC_ONNET_NB_CALLS, 
			SUM(CASE 
				WHEN TRANSACTION_DIRECTION = 'Entrant' AND SUBSTR(TRANSACTION_TYPE, 1, 3) = 'TEL' AND fn_get_nnp_msisdn_simple_destn(OTHER_PARTY) IN ('VIETTEL', 'MTN', 'CAMTEL', 'SET') 
					THEN 1
				ELSE 0 
			END) INC_OFNET_NB_CALLS, 
			SUM(CASE 
				WHEN TRANSACTION_DIRECTION = 'Entrant' AND SUBSTR(TRANSACTION_TYPE, 1, 3) = 'TEL' AND fn_get_nnp_msisdn_simple_destn(OTHER_PARTY) IN ('INTERNATIONAL') 
					THEN 1
				ELSE 0 
			END) INC_INTER_NB_CALLS,
			SUM(CASE 
				WHEN TRANSACTION_DIRECTION = 'Entrant' AND SUBSTR(TRANSACTION_TYPE, 1, 3) = 'SMS'
					THEN 1
				ELSE 0 
			END) INC_NB_SMS,
			SUM(CASE 
				WHEN TRANSACTION_DIRECTION = 'Entrant' AND SUBSTR(TRANSACTION_TYPE, 1, 3) = 'TEL'
					THEN TRANSACTION_DURATION
				ELSE 0 
			END) INC_CALL_DURATION,
			SUM(CASE 
				WHEN TRANSACTION_DIRECTION = 'Entrant' AND SUBSTR(TRANSACTION_TYPE, 1, 3) = 'TEL' AND fn_get_nnp_msisdn_simple_destn(OTHER_PARTY) IN ('OCM') 
					THEN TRANSACTION_DURATION
				ELSE 0 
			END) INC_ONNET_NB_DURATION, 
			SUM(CASE 
				WHEN TRANSACTION_DIRECTION = 'Entrant' AND SUBSTR(TRANSACTION_TYPE, 1, 3) = 'TEL' AND fn_get_nnp_msisdn_simple_destn(OTHER_PARTY) IN ('VIETTEL', 'MTN', 'CAMTEL', 'SET') 
					THEN TRANSACTION_DURATION
				ELSE 0 
			END) INC_OFNET_NB_DURATION, 
			SUM(CASE 
				WHEN TRANSACTION_DIRECTION = 'Entrant' AND SUBSTR(TRANSACTION_TYPE, 1, 3) = 'TEL' AND fn_get_nnp_msisdn_simple_destn(OTHER_PARTY) IN ('INTERNATIONAL') 
					THEN TRANSACTION_DURATION
				ELSE 0 
			END) INC_INTER_NB_DURATION
		FROM 
			MON.SPARK_FT_MSC_TRANSACTION
		WHERE 
			TRANSACTION_DATE = '###SLICE_VALUE###'
			AND fn_get_nnp_msisdn_simple_destn(OTHER_PARTY) IN ('OCM', 'VIETTEL', 'MTN', 'CAMTEL', 'SET', 'INTERNATIONAL') 
			AND fn_get_nnp_msisdn_simple_destn(SERVED_MSISDN) IN ('OCM') 
			AND SERVED_MSISDN NOT IN ('699900999', '699900909', '699900808', '699900984', '699900762', '699900991')
		GROUP BY 
			fn_nnp_remove_rn(SERVED_MSISDN) 
	) b
	ON a.MSISDN = b.MSISDN
) a
FULL JOIN
(
	SELECT  
		fn_nnp_remove_rn(CHARGED_PARTY_MSISDN) AS MSISDN, 
		SUM(CASE WHEN SDP_GOS_SERV_NAME IS NULL THEN MAIN_COST ELSE 0 END) AS MA_DATA, 
		SUM(CASE WHEN SDP_GOS_SERV_NAME IS NOT NULL THEN MAIN_COST ELSE 0 END) AS MA_GOS_SVA, 
		SUM(NVL(BYTES_SENT + BYTES_RECEIVED, 0)) AS BYTES_DATA
	FROM 
		mon.SPARK_FT_CRA_GPRS
	WHERE 
		SESSION_DATE = '###SLICE_VALUE###'
		AND NVL(MAIN_COST, 0) >= 0 
	GROUP BY 
		fn_nnp_remove_rn(CHARGED_PARTY_MSISDN)
) b
ON a.MSISDN = b.MSISDN
