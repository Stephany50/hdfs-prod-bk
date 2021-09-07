SELECT IF(
    T_1.FT_EXIST = 0
    AND T_2.nb_event_dates + datediff(concat(substr('###SLICE_VALUE###', 1, 7), '-01'), '###SLICE_VALUE###') - 1 = 0 
    AND T_2.nb_kpi_names = 4
    , 'OK'
    , 'NOK'
)
FROM
(
    SELECT COUNT(*) FT_EXIST 
    FROM MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    WHERE EVENT_DATE='###SLICE_VALUE###' AND 
        KPI_NAME IN (
            'TRAFIC_VOIX', 
            'TRAFIC_DATA', 
            'TRAFIC_SMS',
            'REVENU_VOIX_PYG', 
            'REVENU_SMS_PYG',
            'RECHARGES',
            'CASHIN', 
            'CASHOUT',
            'REVENU_OM'
        )
) T_1,
(SELECT COUNT(DISTINCT event_date) nb_event_dates, COUNT(DISTINCT KPI_NAME) nb_kpi_names FROM MON.SPARK_FT_GEOMARKETING_REPORT_360 WHERE EVENT_DATE between concat(substr('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###' and kpi_name in ('TRAFIC_DATA', 'RECHARGES', 'CASHIN', 'REVENU_OM') ) T_2