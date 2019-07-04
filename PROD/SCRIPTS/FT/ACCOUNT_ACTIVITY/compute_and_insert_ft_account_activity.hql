INSERT INTO MON.FT_ACCOUNT_ACTIVITY PARTITION(EVENT_DATE)
SELECT
    a.access_key  MSISDN,
    b.og_call OG_CALL,
    b.ic_call_1 IC_CALL_1,
    b.ic_call_2 IC_CALL_2,
    b.ic_call_3 IC_CALL_3,
    b.ic_call_4 IC_CALL_4,
    (CASE  
        WHEN  ( b.OG_CALL  > (DATE_SUB('2019-06-18',94)) OR NVL(b.IC_CALL_4, b.IC_CALL_3 )  >  DATE_SUB('###SLICE_VALUE###',94) ) AND a.OSP_STATUS IN ('ACTIVE', 'INACTIVE')
            THEN 'ACTI'
        ELSE 'INAC'
    END  ) STATUS,
    ( CASE
        WHEN NVL(b.OG_CALL,DATE_SUB('###SLICE_VALUE###', 100)) > DATE_SUB('###SLICE_VALUE###', 94) OR  NVL (b.IC_CALL_1, DATE_SUB('###SLICE_VALUE###',100))  > DATE_SUB('###SLICE_VALUE###', 94) AND b.IC_CALL_4 IS NOT NULL
			THEN 'ACTIF'
        ELSE 'INACT'
    END )     GP_STATUS,
    ( CASE
        WHEN c.gp_status IS NULL
            THEN '###SLICE_VALUE###'
        WHEN c.gp_status <> ( CASE
                                WHEN b.og_call > DATE_SUB('###SLICE_VALUE###',94) OR NVL (b.ic_call_1, b.ic_call_2) > DATE_SUB('###SLICE_VALUE###',94) AND b.ic_call_4 IS NOT NULL
                                    THEN'ACTIF'
                                ELSE 'INACT'
                            END )
            THEN '###SLICE_VALUE###'
        ELSE c.gp_status_date
    END )  GP_STATUS_DATE,
    ( CASE
        WHEN c.gp_first_active_date IS NOT NULL
            THEN c.gp_first_active_date
        WHEN 'ACTIF' = ( CASE
                            WHEN b.og_call > DATE_SUB('###SLICE_VALUE###',94) OR NVL(b.ic_call_1, b.ic_call_2) > DATE_SUB('###SLICE_VALUE###',94) AND b.ic_call_4 IS NOT NULL
                                THEN 'ACTIF'
                            ELSE 'INACT'
                        END )
            THEN NVL(Greatest (b.og_call, NVL (b.ic_call_1, b.ic_call_2)),b.og_call)
        ELSE NULL
    END )                                    GP_FIRST_ACTIVE_DATE,
    a.activation_date                        ACTIVATION_DATE,
    a.deactivation_date                      RESILIATION_DATE,
    a.provisioning_date                      PROVISION_DATE,
    a.PROFILE                                FORMULE,
    a.osp_status                             PLATFORM_STATUS,
    a.main_credit                            REMAIN_CREDIT_MAIN,
    a.promo_credit                           REMAIN_CREDIT_PROMO,
    a.lang                                   LANGUAGE_ACC,
    a.src_table  SRC_TABLE,
    a.contract_id                            CONTRACT_ID,
    a.customer_id    CUSTOMER_ID,
    a.account_id   ACCOUNT_ID,
    a.login   LOGIN,
    a.commercial_offer                       ICC_COMM_OFFER,
    a.bscs_comm_offer  BSCS_COMM_OFFER,
    a.current_status                         BSCS_STATUS,
    a.osp_account_type   OSP_ACCOUNT_TYPE,
    a.cust_group   CUST_GROUP,
    a.cust_billcycle   CUST_BILLCYCLE,
    a.status_date                            BSCS_STATUS_DATE,
    a.inactivity_begin_date   INACTIVITY_BEGIN_DATE,
    ( CASE
        WHEN b.og_call > DATE_SUB('###SLICE_VALUE###',94) OR NVL(b.ic_call_4, NVL(b.ic_call_3, NVL(b.ic_call_2, NVL(b.ic_call_1, '###SLICE_VALUE###')))) > DATE_SUB('###SLICE_VALUE###',94)
            THEN 'ACTIF'
        ELSE 'INACT'
    END )     COMGP_STATUS,
    ( CASE
        WHEN c.comgp_status_date IS NULL AND c.gp_status_date IS NOT NULL
            THEN c.gp_status_date
        WHEN c.comgp_status_date IS NULL
            THEN '###SLICE_VALUE###'
        WHEN c.comgp_status IS NULL OR c.comgp_status <> ( CASE
                                                            WHEN b.og_call > DATE_SUB('###SLICE_VALUE###',94) OR NVL(b.ic_call_4, NVL (b.ic_call_3, NVL (b.ic_call_2, NVL (b.ic_call_1,'###SLICE_VALUE###')))) > DATE_SUB('###SLICE_VALUE###',94)
                                                                THEN 'ACTIF'
                                                            ELSE 'INACT'
                                                        END )
            THEN '###SLICE_VALUE###'
        ELSE c.comgp_status_date
    END ) COMGP_STATUS_DATE,
    ( CASE
        WHEN c.comgp_first_active_date IS NOT NULL
            THEN c.comgp_first_active_date
        WHEN c.comgp_first_active_date IS NULL AND c.gp_first_active_date IS NOT NULL
            THEN c.gp_first_active_date
        WHEN 'ACTIF' = ( CASE
                            WHEN b.og_call > DATE_SUB('###SLICE_VALUE###',94) OR NVL (b.ic_call_4, NVL (b.ic_call_3, NVL (b.ic_call_2, NVL (b.ic_call_1, '1970-01-01')))) > DATE_SUB('###SLICE_VALUE###',94)
                                THEN 'ACTIF'
                            ELSE 'INACT'
                        END )
            THEN Greatest (b.og_call, NVL(b.ic_call_4, NVL(b.ic_call_3, NVL (b.ic_call_2, NVL(b.ic_call_1, '1970-01-01')))))
        ELSE NULL
    END )     COMGP_FIRST_ACTIVE_DATE,
    current_timestamp INSERT_DATE,
    '###SLICE_VALUE###' EVENT_DATE

FROM

	( SELECT * FROM mon.ft_contract_snapshot
	WHERE  event_date = '###SLICE_VALUE###'
	AND src_table = 'IT_ICC_ACCOUNT') a

	LEFT JOIN (SELECT FN_FORMAT_MSISDN_TO_9DIGITS(MSISDN) MSISDN9, * FROM   mon.ft_og_ic_call_snapshot
	WHERE  event_date = '###SLICE_VALUE###') b ON (a.access_key = b.MSISDN9)

	LEFT JOIN (SELECT FN_FORMAT_MSISDN_TO_9DIGITS(MSISDN) MSISDN9, * FROM   mon.ft_account_activity
	WHERE  event_date = ( DATE_SUB('###SLICE_VALUE###',1))) c ON (a.access_key = c.MSISDN9)

UNION ALL

SELECT
	c.MSISDN9 MSISDN,
	b.OG_CALL,
	b.IC_CALL_1,
	b.IC_CALL_2,
	b.IC_CALL_3,
	b.IC_CALL_4,
	c.STATUS,
	(CASE
		WHEN  NVL(b.OG_CALL,DATE_SUB('###SLICE_VALUE###', 100) )  > DATE_SUB('###SLICE_VALUE###', 94) OR NVL(b.IC_CALL_1, DATE_SUB('###SLICE_VALUE###', 100))  > DATE_SUB('###SLICE_VALUE###', 94) AND b.IC_CALL_4 IS NOT NULL
			THEN 'ACTIF'
		ELSE    'INACT'
	END  ) GP_STATUS,
	( CASE
		WHEN c.GP_STATUS IS NULL
			THEN '###SLICE_VALUE###'
		WHEN c.GP_STATUS <> (CASE
								WHEN b.OG_CALL  > DATE_SUB('###SLICE_VALUE###', 94) OR NVL(b.IC_CALL_1, b.IC_CALL_2)  > DATE_SUB('###SLICE_VALUE###', 94) AND b.IC_CALL_4 IS NOT NULL
									THEN 'ACTIF'
								ELSE    'INACT'
							END  )
			THEN '###SLICE_VALUE###'
		ELSE c.GP_STATUS_DATE
	END ) GP_STATUS_DATE     ,
	(CASE
		WHEN c.GP_FIRST_ACTIVE_DATE IS NOT NULL
			THEN c.GP_FIRST_ACTIVE_DATE
		WHEN  'ACTIF'  = (CASE
							WHEN  b.OG_CALL  > DATE_SUB('###SLICE_VALUE###', 94)  OR NVL (b.IC_CALL_1, b.IC_CALL_2)  > DATE_SUB('###SLICE_VALUE###', 94) AND b.IC_CALL_4 IS NOT NULL
								THEN 'ACTIF'
							ELSE    'INACT'
						END  )
			THEN  NVL( GREATEST (b.OG_CALL, NVL (b.IC_CALL_1, b.IC_CALL_2)), b.OG_CALL)
		ELSE NULL
	END )  GP_FIRST_ACTIVE_DATE,
	c.ACTIVATION_DATE,
	c.RESILIATION_DATE,
	c.PROVISION_DATE,
	c.FORMULE,
	c.PLATFORM_STATUS,
	c.REMAIN_CREDIT_MAIN,
	c.REMAIN_CREDIT_PROMO,
	c.LANGUAGE_ACC,
	c.SRC_TABLE ,
	c.CONTRACT_ID,
	c.CUSTOMER_ID ,
	c.ACCOUNT_ID ,
	c.LOGIN ,
	c.ICC_COMM_OFFER,
	c.BSCS_COMM_OFFER ,
	c.BSCS_STATUS,
	c.OSP_ACCOUNT_TYPE,
	c.CUST_GROUP,
	c.CUST_BILLCYCLE,
	c.BSCS_STATUS_DATE,
	c.INACTIVITY_BEGIN_DATE,
	(CASE
		WHEN  b.OG_CALL  > DATE_SUB('###SLICE_VALUE###', 94)  OR NVL (b.IC_CALL_4, NVL (b.IC_CALL_3, NVL (b.IC_CALL_2, NVL (b.IC_CALL_1, '1970-01-01'))))  > DATE_SUB('###SLICE_VALUE###', 94)
			THEN 'ACTIF'
		ELSE    'INACT'
	END  ) COMGP_STATUS,
	( CASE
		WHEN c.COMGP_STATUS_DATE IS NULL  AND c.GP_STATUS_DATE IS NOT NULL
			THEN c.GP_STATUS_DATE
		WHEN c.COMGP_STATUS_DATE IS NULL
			THEN '###SLICE_VALUE###'
		WHEN c.COMGP_STATUS IS NULL OR c.COMGP_STATUS <> (CASE
															WHEN  b.OG_CALL  > DATE_SUB('###SLICE_VALUE###', 94) OR NVL (b.IC_CALL_4, NVL (b.IC_CALL_3, NVL (b.IC_CALL_2, NVL (b.IC_CALL_1, '1970-01-01'))))  > DATE_SUB('###SLICE_VALUE###', 94)
																THEN 'ACTIF'
															ELSE    'INACT'
														END  )
			THEN '###SLICE_VALUE###'
		ELSE c.COMGP_STATUS_DATE
	END ) COMGP_STATUS_DATE,
	(CASE
		WHEN c.COMGP_FIRST_ACTIVE_DATE IS NOT NULL
			THEN c.COMGP_FIRST_ACTIVE_DATE
		WHEN c.COMGP_FIRST_ACTIVE_DATE IS NULL AND c.GP_FIRST_ACTIVE_DATE IS NOT NULL
			THEN c.GP_FIRST_ACTIVE_DATE
		WHEN  'ACTIF'  = (CASE
							WHEN  b.OG_CALL  > DATE_SUB('###SLICE_VALUE###', 94) OR NVL (b.IC_CALL_4, NVL (b.IC_CALL_3, NVL (b.IC_CALL_2, NVL (b.IC_CALL_1, '1970-01-01'))))  > DATE_SUB('###SLICE_VALUE###', 94)
								THEN 'ACTIF'
							ELSE    'INACT'
						END  )
			THEN  GREATEST (b.OG_CALL, NVL (b.IC_CALL_4, NVL (b.IC_CALL_3, NVL (b.IC_CALL_2, NVL (b.IC_CALL_1, '1970-01-01')))))
		ELSE NULL
	END) COMGP_FIRST_ACTIVE_DATE,
    current_timestamp INSERT_DATE,
    '###SLICE_VALUE###' EVENT_DATE
FROM
	(
		SELECT FN_FORMAT_MSISDN_TO_9DIGITS(MSISDN )MSISDN
		FROM MON.FT_ACCOUNT_ACTIVITY    t1
		WHERE t1.EVENT_DATE = DATE_SUB('###SLICE_VALUE###', 1)
		MINUS
		SELECT ACCESS_KEY MSISDN
		FROM MON.FT_CONTRACT_SNAPSHOT t2
		WHERE t2.EVENT_DATE = '###SLICE_VALUE###'
		AND t2.SRC_TABLE = 'IT_ICC_ACCOUNT'
	) a
	LEFT JOIN (SELECT FN_FORMAT_MSISDN_TO_9DIGITS(MSISDN)MSISDN9,* FROM  MON.FT_OG_IC_CALL_SNAPSHOT WHERE EVENT_DATE = '###SLICE_VALUE###'  ) b  ON (a.MSISDN = b.MSISDN9)
	INNER JOIN( SELECT FN_FORMAT_MSISDN_TO_9DIGITS(MSISDN)MSISDN9,* FROM MON.FT_ACCOUNT_ACTIVITY WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###', 1) ) c   ON (a.MSISDN = c.MSISDN9)
