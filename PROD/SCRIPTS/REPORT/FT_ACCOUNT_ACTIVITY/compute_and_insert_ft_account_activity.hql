INSERT INTO MON.FT_ACCOUNT_ACTIVITY PARTITION(EVENT_DATE)
SELECT
    a.access_key  MSISDN,
    b.og_call OG_CALL,
    b.ic_call_1 IC_CALL_1,
    b.ic_call_2 IC_CALL_2,
    b.ic_call_3 IC_CALL_3,
    b.ic_call_4 IC_CALL_4,
    (CASE  WHEN  ( ( b.OG_CALL  > (DATE_SUB('2019-06-18',94))  ) OR ( NVL (b.IC_CALL_4, b.IC_CALL_3 )  >  (DATE_SUB('2019-06-18',94)  ))) AND a.OSP_STATUS IN ('ACTIVE', 'INACTIVE')
    THEN 'ACTI'  ELSE 'INAC' END  ) STATUS,
    ( CASE WHEN ( ( b.og_call > (DATE_SUB('2019-06-18',94) ))OR ( NVL (b.ic_call_1, b.ic_call_2) >
    ( DATE_SUB('2019-06-18',94))) AND b.ic_call_4 IS NOT NULL )  THEN 'ACTIF' ELSE 'INACT' END )     GP_STATUS,
    ( CASE WHEN c.gp_status IS NULL THEN '2019-06-18' WHEN c.gp_status <> ( CASE WHEN ( ( b.og_call > (DATE_SUB('2019-06-18',94))) OR ( NVL (b.ic_call_1, b.ic_call_2) > ( DATE_SUB('2019-06-18',94) ) AND b.ic_call_4 IS NOT NULL ) )
    THEN'ACTIF'
    ELSE 'INACT'
    END ) THEN '2019-06-18' ELSE c.gp_status_date END )  GP_STATUS_DATE,
    ( CASE WHEN c.gp_first_active_date IS NOT NULL THEN c.gp_first_active_date WHEN 'ACTIF' = ( CASE WHEN ( ( b.og_call > ( DATE_SUB('2019-06-18',94))) OR
    ( NVL (b.ic_call_1, b.ic_call_2) > (DATE_SUB('2019-06-18',94))
    AND b.ic_call_4 IS NOT NULL ) )
    THEN 'ACTIF'
    ELSE 'INACT'
    END ) THEN NVL(Greatest (b.og_call,
    NVL (b.ic_call_1, b.ic_call_2)),b.og_call)
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
    ( CASE WHEN ( ( b.og_call > ( DATE_SUB('2019-06-18',94) )) OR ( NVL (b.ic_call_4, NVL (b.ic_call_3, NVL (b.ic_call_2, NVL (b.ic_call_1, '2019-06-18')))) > ( DATE_SUB('2019-06-18',94) ) ) )
    THEN 'ACTIF'
    ELSE 'INACT'
    END )     COMGP_STATUS,
    ( CASE WHEN ( c.comgp_status_date IS NULL ) AND ( c.gp_status_date IS NOT NULL ) THEN c.gp_status_date WHEN c.comgp_status_date IS NULL THEN '2019-06-18'
    WHEN ( c.comgp_status IS NULL ) OR c.comgp_status <> ( CASE
    WHEN (
    ( b.og_call >
    ( DATE_SUB('2019-06-18',94))
    )
    OR ( NVL (b.ic_call_4,
    NVL (b.ic_call_3, NVL
    (b.ic_call_2,
    NVL (b.ic_call_1,'2019-06-18')))) >
    ( DATE_SUB('2019-06-18',94)) ) )
    THEN 'ACTIF'

    ELSE 'INACT'
    END ) THEN '2019-06-18'
    ELSE c.comgp_status_date
    END ) COMGP_STATUS_DATE,
    ( CASE
    WHEN ( c.comgp_first_active_date IS NOT NULL ) THEN
    c.comgp_first_active_date
    WHEN ( c.comgp_first_active_date IS NULL )
    AND ( c.gp_first_active_date IS NOT NULL ) THEN
    c.gp_first_active_date
    WHEN 'ACTIF' = ( CASE
    WHEN ( ( b.og_call >
    ( DATE_SUB('2019-06-18',94))
    )
    OR ( NVL (b.ic_call_4, NVL (b.ic_call_3,
    NVL (b.ic_call_2,
    NVL (b.ic_call_1,
    TO_DATE(FROM_UTC_TIMESTAMP(19700101,'Africa/Douala'))
    )))) >
    ( DATE_SUB('2019-06-18',94) ) ) )
    THEN 'ACTIF'

    ELSE 'INACT'
    END ) THEN Greatest (b.og_call,
    NVL (b.ic_call_4, NVL
    (b.ic_call_3, NVL (b.ic_call_2, NVL
    (
    b.ic_call_1, TO_DATE(FROM_UTC_TIMESTAMP(19700101,'Africa/Douala'))
    )))))
    ELSE NULL
    END )     COMGP_FIRST_ACTIVE_DATE,
	current_timestamp INSERT_DATE,
    '2019-06-18' EVENT_DATE

FROM

( SELECT * FROM mon.ft_contract_snapshot
WHERE  event_date = '2019-06-18'
AND src_table = 'IT_ICC_ACCOUNT'
AND osp_status <> 'TERMINATED') a


LEFT JOIN (SELECT * FROM   mon.ft_og_ic_call_snapshot
WHERE  event_date = '2019-06-18') b ON (a.access_key = b.msisdn)


LEFT JOIN (SELECT * FROM   mon.ft_account_activity
WHERE  event_date = ( DATE_SUB('2019-06-18',1))) c ON (a.access_key = c.msisdn)