SELECT IF(
    T_1.FT_EXIST = 0
    AND T_2.nb_event_dates + datediff(concat('###SLICE_VALUE###', '-01'), last_day(concat('###SLICE_VALUE###', '-01'))) - 1 = 0 
    AND T_2.nb_kpi_names = 4
    , 'OK'
    , 'NOK'
)
FROM
(
    SELECT COUNT(*) FT_EXIST 
    FROM MON.SPARK_FT_GEOMARKETING_REPORT_360_MONTH 
    WHERE EVENT_MONTH='###SLICE_VALUE###' AND 
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
(SELECT COUNT(DISTINCT event_date) nb_event_dates, COUNT(DISTINCT KPI_NAME) nb_kpi_names FROM MON.SPARK_FT_GEOMARKETING_REPORT_360 WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) and kpi_name in ('TRAFIC_DATA', 'RECHARGES', 'CASHIN', 'REVENU_OM') ) T_2