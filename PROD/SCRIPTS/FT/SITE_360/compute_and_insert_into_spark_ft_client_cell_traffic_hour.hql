insert into mon.spark_ft_client_cell_traffic_hour
select
    msisdn,
    hour_period,
    (
        case
            when hour_period = '00' then last_ci_00
            when hour_period = '01' then last_ci_01
            when hour_period = '02' then last_ci_02
            when hour_period = '03' then last_ci_03
            when hour_period = '04' then last_ci_04
            when hour_period = '05' then last_ci_05
            when hour_period = '06' then last_ci_06
            when hour_period = '07' then last_ci_07
            when hour_period = '08' then last_ci_08
            when hour_period = '09' then last_ci_09
            when hour_period = '10' then last_ci_10
            when hour_period = '11' then last_ci_11
            when hour_period = '12' then last_ci_12
            when hour_period = '13' then last_ci_13
            when hour_period = '14' then last_ci_14
            when hour_period = '15' then last_ci_15
            when hour_period = '16' then last_ci_16
            when hour_period = '17' then last_ci_17
            when hour_period = '18' then last_ci_18
            when hour_period = '19' then last_ci_19
            when hour_period = '20' then last_ci_20
            when hour_period = '21' then last_ci_21
            when hour_period = '22' then last_ci_22
            when hour_period = '23' then last_ci_23
        end
    ) last_ci,
    (
        case
            when hour_period = '00' then last_lac_00
            when hour_period = '01' then last_lac_01
            when hour_period = '02' then last_lac_02
            when hour_period = '03' then last_lac_03
            when hour_period = '04' then last_lac_04
            when hour_period = '05' then last_lac_05
            when hour_period = '06' then last_lac_06
            when hour_period = '07' then last_lac_07
            when hour_period = '08' then last_lac_08
            when hour_period = '09' then last_lac_09
            when hour_period = '10' then last_lac_10
            when hour_period = '11' then last_lac_11
            when hour_period = '12' then last_lac_12
            when hour_period = '13' then last_lac_13
            when hour_period = '14' then last_lac_14
            when hour_period = '15' then last_lac_15
            when hour_period = '16' then last_lac_16
            when hour_period = '17' then last_lac_17
            when hour_period = '18' then last_lac_18
            when hour_period = '19' then last_lac_19
            when hour_period = '20' then last_lac_20
            when hour_period = '21' then last_lac_21
            when hour_period = '22' then last_lac_22
            when hour_period = '23' then last_lac_23
        end
    ) last_lac,
    (
        case
            when hour_period = '00' then last_transaction_time_00
            when hour_period = '01' then last_transaction_time_01
            when hour_period = '02' then last_transaction_time_02
            when hour_period = '03' then last_transaction_time_03
            when hour_period = '04' then last_transaction_time_04
            when hour_period = '05' then last_transaction_time_05
            when hour_period = '06' then last_transaction_time_06
            when hour_period = '07' then last_transaction_time_07
            when hour_period = '08' then last_transaction_time_08
            when hour_period = '09' then last_transaction_time_09
            when hour_period = '10' then last_transaction_time_10
            when hour_period = '11' then last_transaction_time_11
            when hour_period = '12' then last_transaction_time_12
            when hour_period = '13' then last_transaction_time_13
            when hour_period = '14' then last_transaction_time_14
            when hour_period = '15' then last_transaction_time_15
            when hour_period = '16' then last_transaction_time_16
            when hour_period = '17' then last_transaction_time_17
            when hour_period = '18' then last_transaction_time_18
            when hour_period = '19' then last_transaction_time_19
            when hour_period = '20' then last_transaction_time_20
            when hour_period = '21' then last_transaction_time_21
            when hour_period = '22' then last_transaction_time_22
            when hour_period = '23' then last_transaction_time_23
        end
    ) last_transaction_time,
    (
        case
            when hour_period = '00' then last_transaction_type_00
            when hour_period = '01' then last_transaction_type_01
            when hour_period = '02' then last_transaction_type_02
            when hour_period = '03' then last_transaction_type_03
            when hour_period = '04' then last_transaction_type_04
            when hour_period = '05' then last_transaction_type_05
            when hour_period = '06' then last_transaction_type_06
            when hour_period = '07' then last_transaction_type_07
            when hour_period = '08' then last_transaction_type_08
            when hour_period = '09' then last_transaction_type_09
            when hour_period = '10' then last_transaction_type_10
            when hour_period = '11' then last_transaction_type_11
            when hour_period = '12' then last_transaction_type_12
            when hour_period = '13' then last_transaction_type_13
            when hour_period = '14' then last_transaction_type_14
            when hour_period = '15' then last_transaction_type_15
            when hour_period = '16' then last_transaction_type_16
            when hour_period = '17' then last_transaction_type_17
            when hour_period = '18' then last_transaction_type_18
            when hour_period = '19' then last_transaction_type_19
            when hour_period = '20' then last_transaction_type_20
            when hour_period = '21' then last_transaction_type_21
            when hour_period = '22' then last_transaction_type_22
            when hour_period = '23' then last_transaction_type_23
        end
    ) last_transaction_type,
    current_timestamp insert_date,
    '###SLICE_VALUE###' event_date
from
(
    select '00' hour_period union
    select '01' hour_period union
    select '02' hour_period union
    select '03' hour_period union
    select '04' hour_period union
    select '05' hour_period union
    select '06' hour_period union
    select '07' hour_period union
    select '08' hour_period union
    select '09' hour_period union
    select '10' hour_period union
    select '11' hour_period union
    select '12' hour_period union
    select '13' hour_period union
    select '14' hour_period union
    select '15' hour_period union
    select '16' hour_period union
    select '17' hour_period union
    select '18' hour_period union
    select '19' hour_period union
    select '20' hour_period union
    select '21' hour_period union
    select '22' hour_period union
    select '23' hour_period
) y,
(
    select
        z0.msisdn,
        nvl(last_ci_00, last_ci_in_past) last_ci_00,
        nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past)) last_ci_01,
        nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past))) last_ci_02,
        nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past)))) last_ci_03,
        nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past))))) last_ci_04,
        nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past)))))) last_ci_05,
        nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past))))))) last_ci_06,
        nvl(last_ci_07, nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past)))))))) last_ci_07,
        nvl(last_ci_08, nvl(last_ci_07, nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past))))))))) last_ci_08,
        nvl(last_ci_09, nvl(last_ci_08, nvl(last_ci_07, nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past)))))))))) last_ci_09,
        nvl(last_ci_10, nvl(last_ci_09, nvl(last_ci_08, nvl(last_ci_07, nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past))))))))))) last_ci_10,
        nvl(last_ci_11, nvl(last_ci_10, nvl(last_ci_09, nvl(last_ci_08, nvl(last_ci_07, nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past)))))))))))) last_ci_11,
        nvl(last_ci_12, nvl(last_ci_11, nvl(last_ci_10, nvl(last_ci_09, nvl(last_ci_08, nvl(last_ci_07, nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past))))))))))))) last_ci_12,
        nvl(last_ci_13, nvl(last_ci_12, nvl(last_ci_11, nvl(last_ci_10, nvl(last_ci_09, nvl(last_ci_08, nvl(last_ci_07, nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past)))))))))))))) last_ci_13,
        nvl(last_ci_14, nvl(last_ci_13, nvl(last_ci_12, nvl(last_ci_11, nvl(last_ci_10, nvl(last_ci_09, nvl(last_ci_08, nvl(last_ci_07, nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past))))))))))))))) last_ci_14,
        nvl(last_ci_15, nvl(last_ci_14, nvl(last_ci_13, nvl(last_ci_12, nvl(last_ci_11, nvl(last_ci_10, nvl(last_ci_09, nvl(last_ci_08, nvl(last_ci_07, nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past)))))))))))))))) last_ci_15,
        nvl(last_ci_16, nvl(last_ci_15, nvl(last_ci_14, nvl(last_ci_13, nvl(last_ci_12, nvl(last_ci_11, nvl(last_ci_10, nvl(last_ci_09, nvl(last_ci_08, nvl(last_ci_07, nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past))))))))))))))))) last_ci_16,
        nvl(last_ci_17, nvl(last_ci_16, nvl(last_ci_15, nvl(last_ci_14, nvl(last_ci_13, nvl(last_ci_12, nvl(last_ci_11, nvl(last_ci_10, nvl(last_ci_09, nvl(last_ci_08, nvl(last_ci_07, nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past)))))))))))))))))) last_ci_17,
        nvl(last_ci_18, nvl(last_ci_17, nvl(last_ci_16, nvl(last_ci_15, nvl(last_ci_14, nvl(last_ci_13, nvl(last_ci_12, nvl(last_ci_11, nvl(last_ci_10, nvl(last_ci_09, nvl(last_ci_08, nvl(last_ci_07, nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past))))))))))))))))))) last_ci_18,
        nvl(last_ci_19, nvl(last_ci_18, nvl(last_ci_17, nvl(last_ci_16, nvl(last_ci_15, nvl(last_ci_14, nvl(last_ci_13, nvl(last_ci_12, nvl(last_ci_11, nvl(last_ci_10, nvl(last_ci_09, nvl(last_ci_08, nvl(last_ci_07, nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past)))))))))))))))))))) last_ci_19,
        nvl(last_ci_20, nvl(last_ci_19, nvl(last_ci_18, nvl(last_ci_17, nvl(last_ci_16, nvl(last_ci_15, nvl(last_ci_14, nvl(last_ci_13, nvl(last_ci_12, nvl(last_ci_11, nvl(last_ci_10, nvl(last_ci_09, nvl(last_ci_08, nvl(last_ci_07, nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past))))))))))))))))))))) last_ci_20,
        nvl(last_ci_21, nvl(last_ci_20, nvl(last_ci_19, nvl(last_ci_18, nvl(last_ci_17, nvl(last_ci_16, nvl(last_ci_15, nvl(last_ci_14, nvl(last_ci_13, nvl(last_ci_12, nvl(last_ci_11, nvl(last_ci_10, nvl(last_ci_09, nvl(last_ci_08, nvl(last_ci_07, nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past)))))))))))))))))))))) last_ci_21,
        nvl(last_ci_22, nvl(last_ci_21, nvl(last_ci_20, nvl(last_ci_19, nvl(last_ci_18, nvl(last_ci_17, nvl(last_ci_16, nvl(last_ci_15, nvl(last_ci_14, nvl(last_ci_13, nvl(last_ci_12, nvl(last_ci_11, nvl(last_ci_10, nvl(last_ci_09, nvl(last_ci_08, nvl(last_ci_07, nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past))))))))))))))))))))))) last_ci_22,
        nvl(last_ci_23, nvl(last_ci_22, nvl(last_ci_21, nvl(last_ci_20, nvl(last_ci_19, nvl(last_ci_18, nvl(last_ci_17, nvl(last_ci_16, nvl(last_ci_15, nvl(last_ci_14, nvl(last_ci_13, nvl(last_ci_12, nvl(last_ci_11, nvl(last_ci_10, nvl(last_ci_09, nvl(last_ci_08, nvl(last_ci_07, nvl(last_ci_06, nvl(last_ci_05, nvl(last_ci_04, nvl(last_ci_03, nvl(last_ci_02, nvl(last_ci_01, nvl(last_ci_00, last_ci_in_past)))))))))))))))))))))))) last_ci_23,

        nvl(last_lac_00, last_lac_in_past) last_lac_00,
        nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past)) last_lac_01,
        nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past))) last_lac_02,
        nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past)))) last_lac_03,
        nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past))))) last_lac_04,
        nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past)))))) last_lac_05,
        nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past))))))) last_lac_06,
        nvl(last_lac_07, nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past)))))))) last_lac_07,
        nvl(last_lac_08, nvl(last_lac_07, nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past))))))))) last_lac_08,
        nvl(last_lac_09, nvl(last_lac_08, nvl(last_lac_07, nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past)))))))))) last_lac_09,
        nvl(last_lac_10, nvl(last_lac_09, nvl(last_lac_08, nvl(last_lac_07, nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past))))))))))) last_lac_10,
        nvl(last_lac_11, nvl(last_lac_10, nvl(last_lac_09, nvl(last_lac_08, nvl(last_lac_07, nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past)))))))))))) last_lac_11,
        nvl(last_lac_12, nvl(last_lac_11, nvl(last_lac_10, nvl(last_lac_09, nvl(last_lac_08, nvl(last_lac_07, nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past))))))))))))) last_lac_12,
        nvl(last_lac_13, nvl(last_lac_12, nvl(last_lac_11, nvl(last_lac_10, nvl(last_lac_09, nvl(last_lac_08, nvl(last_lac_07, nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past)))))))))))))) last_lac_13,
        nvl(last_lac_14, nvl(last_lac_13, nvl(last_lac_12, nvl(last_lac_11, nvl(last_lac_10, nvl(last_lac_09, nvl(last_lac_08, nvl(last_lac_07, nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past))))))))))))))) last_lac_14,
        nvl(last_lac_15, nvl(last_lac_14, nvl(last_lac_13, nvl(last_lac_12, nvl(last_lac_11, nvl(last_lac_10, nvl(last_lac_09, nvl(last_lac_08, nvl(last_lac_07, nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past)))))))))))))))) last_lac_15,
        nvl(last_lac_16, nvl(last_lac_15, nvl(last_lac_14, nvl(last_lac_13, nvl(last_lac_12, nvl(last_lac_11, nvl(last_lac_10, nvl(last_lac_09, nvl(last_lac_08, nvl(last_lac_07, nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past))))))))))))))))) last_lac_16,
        nvl(last_lac_17, nvl(last_lac_16, nvl(last_lac_15, nvl(last_lac_14, nvl(last_lac_13, nvl(last_lac_12, nvl(last_lac_11, nvl(last_lac_10, nvl(last_lac_09, nvl(last_lac_08, nvl(last_lac_07, nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past)))))))))))))))))) last_lac_17,
        nvl(last_lac_18, nvl(last_lac_17, nvl(last_lac_16, nvl(last_lac_15, nvl(last_lac_14, nvl(last_lac_13, nvl(last_lac_12, nvl(last_lac_11, nvl(last_lac_10, nvl(last_lac_09, nvl(last_lac_08, nvl(last_lac_07, nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past))))))))))))))))))) last_lac_18,
        nvl(last_lac_19, nvl(last_lac_18, nvl(last_lac_17, nvl(last_lac_16, nvl(last_lac_15, nvl(last_lac_14, nvl(last_lac_13, nvl(last_lac_12, nvl(last_lac_11, nvl(last_lac_10, nvl(last_lac_09, nvl(last_lac_08, nvl(last_lac_07, nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past)))))))))))))))))))) last_lac_19,
        nvl(last_lac_20, nvl(last_lac_19, nvl(last_lac_18, nvl(last_lac_17, nvl(last_lac_16, nvl(last_lac_15, nvl(last_lac_14, nvl(last_lac_13, nvl(last_lac_12, nvl(last_lac_11, nvl(last_lac_10, nvl(last_lac_09, nvl(last_lac_08, nvl(last_lac_07, nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past))))))))))))))))))))) last_lac_20,
        nvl(last_lac_21, nvl(last_lac_20, nvl(last_lac_19, nvl(last_lac_18, nvl(last_lac_17, nvl(last_lac_16, nvl(last_lac_15, nvl(last_lac_14, nvl(last_lac_13, nvl(last_lac_12, nvl(last_lac_11, nvl(last_lac_10, nvl(last_lac_09, nvl(last_lac_08, nvl(last_lac_07, nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past)))))))))))))))))))))) last_lac_21,
        nvl(last_lac_22, nvl(last_lac_21, nvl(last_lac_20, nvl(last_lac_19, nvl(last_lac_18, nvl(last_lac_17, nvl(last_lac_16, nvl(last_lac_15, nvl(last_lac_14, nvl(last_lac_13, nvl(last_lac_12, nvl(last_lac_11, nvl(last_lac_10, nvl(last_lac_09, nvl(last_lac_08, nvl(last_lac_07, nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past))))))))))))))))))))))) last_lac_22,
        nvl(last_lac_23, nvl(last_lac_22, nvl(last_lac_21, nvl(last_lac_20, nvl(last_lac_19, nvl(last_lac_18, nvl(last_lac_17, nvl(last_lac_16, nvl(last_lac_15, nvl(last_lac_14, nvl(last_lac_13, nvl(last_lac_12, nvl(last_lac_11, nvl(last_lac_10, nvl(last_lac_09, nvl(last_lac_08, nvl(last_lac_07, nvl(last_lac_06, nvl(last_lac_05, nvl(last_lac_04, nvl(last_lac_03, nvl(last_lac_02, nvl(last_lac_01, nvl(last_lac_00, last_lac_in_past)))))))))))))))))))))))) last_lac_23,
        nvl(last_transaction_time_00, last_transaction_time_in_past) last_transaction_time_00,

        nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past)) last_transaction_time_01,
        nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past))) last_transaction_time_02,
        nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past)))) last_transaction_time_03,
        nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past))))) last_transaction_time_04,
        nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past)))))) last_transaction_time_05,
        nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past))))))) last_transaction_time_06,
        nvl(last_transaction_time_07, nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past)))))))) last_transaction_time_07,
        nvl(last_transaction_time_08, nvl(last_transaction_time_07, nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past))))))))) last_transaction_time_08,
        nvl(last_transaction_time_09, nvl(last_transaction_time_08, nvl(last_transaction_time_07, nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past)))))))))) last_transaction_time_09,
        nvl(last_transaction_time_10, nvl(last_transaction_time_09, nvl(last_transaction_time_08, nvl(last_transaction_time_07, nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past))))))))))) last_transaction_time_10,
        nvl(last_transaction_time_11, nvl(last_transaction_time_10, nvl(last_transaction_time_09, nvl(last_transaction_time_08, nvl(last_transaction_time_07, nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past)))))))))))) last_transaction_time_11,
        nvl(last_transaction_time_12, nvl(last_transaction_time_11, nvl(last_transaction_time_10, nvl(last_transaction_time_09, nvl(last_transaction_time_08, nvl(last_transaction_time_07, nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past))))))))))))) last_transaction_time_12,
        nvl(last_transaction_time_13, nvl(last_transaction_time_12, nvl(last_transaction_time_11, nvl(last_transaction_time_10, nvl(last_transaction_time_09, nvl(last_transaction_time_08, nvl(last_transaction_time_07, nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past)))))))))))))) last_transaction_time_13,
        nvl(last_transaction_time_14, nvl(last_transaction_time_13, nvl(last_transaction_time_12, nvl(last_transaction_time_11, nvl(last_transaction_time_10, nvl(last_transaction_time_09, nvl(last_transaction_time_08, nvl(last_transaction_time_07, nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past))))))))))))))) last_transaction_time_14,
        nvl(last_transaction_time_15, nvl(last_transaction_time_14, nvl(last_transaction_time_13, nvl(last_transaction_time_12, nvl(last_transaction_time_11, nvl(last_transaction_time_10, nvl(last_transaction_time_09, nvl(last_transaction_time_08, nvl(last_transaction_time_07, nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past)))))))))))))))) last_transaction_time_15,
        nvl(last_transaction_time_16, nvl(last_transaction_time_15, nvl(last_transaction_time_14, nvl(last_transaction_time_13, nvl(last_transaction_time_12, nvl(last_transaction_time_11, nvl(last_transaction_time_10, nvl(last_transaction_time_09, nvl(last_transaction_time_08, nvl(last_transaction_time_07, nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past))))))))))))))))) last_transaction_time_16,
        nvl(last_transaction_time_17, nvl(last_transaction_time_16, nvl(last_transaction_time_15, nvl(last_transaction_time_14, nvl(last_transaction_time_13, nvl(last_transaction_time_12, nvl(last_transaction_time_11, nvl(last_transaction_time_10, nvl(last_transaction_time_09, nvl(last_transaction_time_08, nvl(last_transaction_time_07, nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past)))))))))))))))))) last_transaction_time_17,
        nvl(last_transaction_time_18, nvl(last_transaction_time_17, nvl(last_transaction_time_16, nvl(last_transaction_time_15, nvl(last_transaction_time_14, nvl(last_transaction_time_13, nvl(last_transaction_time_12, nvl(last_transaction_time_11, nvl(last_transaction_time_10, nvl(last_transaction_time_09, nvl(last_transaction_time_08, nvl(last_transaction_time_07, nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past))))))))))))))))))) last_transaction_time_18,
        nvl(last_transaction_time_19, nvl(last_transaction_time_18, nvl(last_transaction_time_17, nvl(last_transaction_time_16, nvl(last_transaction_time_15, nvl(last_transaction_time_14, nvl(last_transaction_time_13, nvl(last_transaction_time_12, nvl(last_transaction_time_11, nvl(last_transaction_time_10, nvl(last_transaction_time_09, nvl(last_transaction_time_08, nvl(last_transaction_time_07, nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past)))))))))))))))))))) last_transaction_time_19,
        nvl(last_transaction_time_20, nvl(last_transaction_time_19, nvl(last_transaction_time_18, nvl(last_transaction_time_17, nvl(last_transaction_time_16, nvl(last_transaction_time_15, nvl(last_transaction_time_14, nvl(last_transaction_time_13, nvl(last_transaction_time_12, nvl(last_transaction_time_11, nvl(last_transaction_time_10, nvl(last_transaction_time_09, nvl(last_transaction_time_08, nvl(last_transaction_time_07, nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past))))))))))))))))))))) last_transaction_time_20,
        nvl(last_transaction_time_21, nvl(last_transaction_time_20, nvl(last_transaction_time_19, nvl(last_transaction_time_18, nvl(last_transaction_time_17, nvl(last_transaction_time_16, nvl(last_transaction_time_15, nvl(last_transaction_time_14, nvl(last_transaction_time_13, nvl(last_transaction_time_12, nvl(last_transaction_time_11, nvl(last_transaction_time_10, nvl(last_transaction_time_09, nvl(last_transaction_time_08, nvl(last_transaction_time_07, nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past)))))))))))))))))))))) last_transaction_time_21,
        nvl(last_transaction_time_22, nvl(last_transaction_time_21, nvl(last_transaction_time_20, nvl(last_transaction_time_19, nvl(last_transaction_time_18, nvl(last_transaction_time_17, nvl(last_transaction_time_16, nvl(last_transaction_time_15, nvl(last_transaction_time_14, nvl(last_transaction_time_13, nvl(last_transaction_time_12, nvl(last_transaction_time_11, nvl(last_transaction_time_10, nvl(last_transaction_time_09, nvl(last_transaction_time_08, nvl(last_transaction_time_07, nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past))))))))))))))))))))))) last_transaction_time_22,
        nvl(last_transaction_time_23, nvl(last_transaction_time_22, nvl(last_transaction_time_21, nvl(last_transaction_time_20, nvl(last_transaction_time_19, nvl(last_transaction_time_18, nvl(last_transaction_time_17, nvl(last_transaction_time_16, nvl(last_transaction_time_15, nvl(last_transaction_time_14, nvl(last_transaction_time_13, nvl(last_transaction_time_12, nvl(last_transaction_time_11, nvl(last_transaction_time_10, nvl(last_transaction_time_09, nvl(last_transaction_time_08, nvl(last_transaction_time_07, nvl(last_transaction_time_06, nvl(last_transaction_time_05, nvl(last_transaction_time_04, nvl(last_transaction_time_03, nvl(last_transaction_time_02, nvl(last_transaction_time_01, nvl(last_transaction_time_00, last_transaction_time_in_past)))))))))))))))))))))))) last_transaction_time_23,

        nvl(last_transaction_type_00, last_transaction_type_in_past) last_transaction_type_00,
        nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past)) last_transaction_type_01,
        nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past))) last_transaction_type_02,
        nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past)))) last_transaction_type_03,
        nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past))))) last_transaction_type_04,
        nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past)))))) last_transaction_type_05,
        nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past))))))) last_transaction_type_06,
        nvl(last_transaction_type_07, nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past)))))))) last_transaction_type_07,
        nvl(last_transaction_type_08, nvl(last_transaction_type_07, nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past))))))))) last_transaction_type_08,
        nvl(last_transaction_type_09, nvl(last_transaction_type_08, nvl(last_transaction_type_07, nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past)))))))))) last_transaction_type_09,
        nvl(last_transaction_type_10, nvl(last_transaction_type_09, nvl(last_transaction_type_08, nvl(last_transaction_type_07, nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past))))))))))) last_transaction_type_10,
        nvl(last_transaction_type_11, nvl(last_transaction_type_10, nvl(last_transaction_type_09, nvl(last_transaction_type_08, nvl(last_transaction_type_07, nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past)))))))))))) last_transaction_type_11,
        nvl(last_transaction_type_12, nvl(last_transaction_type_11, nvl(last_transaction_type_10, nvl(last_transaction_type_09, nvl(last_transaction_type_08, nvl(last_transaction_type_07, nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past))))))))))))) last_transaction_type_12,
        nvl(last_transaction_type_13, nvl(last_transaction_type_12, nvl(last_transaction_type_11, nvl(last_transaction_type_10, nvl(last_transaction_type_09, nvl(last_transaction_type_08, nvl(last_transaction_type_07, nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past)))))))))))))) last_transaction_type_13,
        nvl(last_transaction_type_14, nvl(last_transaction_type_13, nvl(last_transaction_type_12, nvl(last_transaction_type_11, nvl(last_transaction_type_10, nvl(last_transaction_type_09, nvl(last_transaction_type_08, nvl(last_transaction_type_07, nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past))))))))))))))) last_transaction_type_14,
        nvl(last_transaction_type_15, nvl(last_transaction_type_14, nvl(last_transaction_type_13, nvl(last_transaction_type_12, nvl(last_transaction_type_11, nvl(last_transaction_type_10, nvl(last_transaction_type_09, nvl(last_transaction_type_08, nvl(last_transaction_type_07, nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past)))))))))))))))) last_transaction_type_15,
        nvl(last_transaction_type_16, nvl(last_transaction_type_15, nvl(last_transaction_type_14, nvl(last_transaction_type_13, nvl(last_transaction_type_12, nvl(last_transaction_type_11, nvl(last_transaction_type_10, nvl(last_transaction_type_09, nvl(last_transaction_type_08, nvl(last_transaction_type_07, nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past))))))))))))))))) last_transaction_type_16,
        nvl(last_transaction_type_17, nvl(last_transaction_type_16, nvl(last_transaction_type_15, nvl(last_transaction_type_14, nvl(last_transaction_type_13, nvl(last_transaction_type_12, nvl(last_transaction_type_11, nvl(last_transaction_type_10, nvl(last_transaction_type_09, nvl(last_transaction_type_08, nvl(last_transaction_type_07, nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past)))))))))))))))))) last_transaction_type_17,
        nvl(last_transaction_type_18, nvl(last_transaction_type_17, nvl(last_transaction_type_16, nvl(last_transaction_type_15, nvl(last_transaction_type_14, nvl(last_transaction_type_13, nvl(last_transaction_type_12, nvl(last_transaction_type_11, nvl(last_transaction_type_10, nvl(last_transaction_type_09, nvl(last_transaction_type_08, nvl(last_transaction_type_07, nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past))))))))))))))))))) last_transaction_type_18,
        nvl(last_transaction_type_19, nvl(last_transaction_type_18, nvl(last_transaction_type_17, nvl(last_transaction_type_16, nvl(last_transaction_type_15, nvl(last_transaction_type_14, nvl(last_transaction_type_13, nvl(last_transaction_type_12, nvl(last_transaction_type_11, nvl(last_transaction_type_10, nvl(last_transaction_type_09, nvl(last_transaction_type_08, nvl(last_transaction_type_07, nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past)))))))))))))))))))) last_transaction_type_19,
        nvl(last_transaction_type_20, nvl(last_transaction_type_19, nvl(last_transaction_type_18, nvl(last_transaction_type_17, nvl(last_transaction_type_16, nvl(last_transaction_type_15, nvl(last_transaction_type_14, nvl(last_transaction_type_13, nvl(last_transaction_type_12, nvl(last_transaction_type_11, nvl(last_transaction_type_10, nvl(last_transaction_type_09, nvl(last_transaction_type_08, nvl(last_transaction_type_07, nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past))))))))))))))))))))) last_transaction_type_20,
        nvl(last_transaction_type_21, nvl(last_transaction_type_20, nvl(last_transaction_type_19, nvl(last_transaction_type_18, nvl(last_transaction_type_17, nvl(last_transaction_type_16, nvl(last_transaction_type_15, nvl(last_transaction_type_14, nvl(last_transaction_type_13, nvl(last_transaction_type_12, nvl(last_transaction_type_11, nvl(last_transaction_type_10, nvl(last_transaction_type_09, nvl(last_transaction_type_08, nvl(last_transaction_type_07, nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past)))))))))))))))))))))) last_transaction_type_21,
        nvl(last_transaction_type_22, nvl(last_transaction_type_21, nvl(last_transaction_type_20, nvl(last_transaction_type_19, nvl(last_transaction_type_18, nvl(last_transaction_type_17, nvl(last_transaction_type_16, nvl(last_transaction_type_15, nvl(last_transaction_type_14, nvl(last_transaction_type_13, nvl(last_transaction_type_12, nvl(last_transaction_type_11, nvl(last_transaction_type_10, nvl(last_transaction_type_09, nvl(last_transaction_type_08, nvl(last_transaction_type_07, nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past))))))))))))))))))))))) last_transaction_type_22,
        nvl(last_transaction_type_23, nvl(last_transaction_type_22, nvl(last_transaction_type_21, nvl(last_transaction_type_20, nvl(last_transaction_type_19, nvl(last_transaction_type_18, nvl(last_transaction_type_17, nvl(last_transaction_type_16, nvl(last_transaction_type_15, nvl(last_transaction_type_14, nvl(last_transaction_type_13, nvl(last_transaction_type_12, nvl(last_transaction_type_11, nvl(last_transaction_type_10, nvl(last_transaction_type_09, nvl(last_transaction_type_08, nvl(last_transaction_type_07, nvl(last_transaction_type_06, nvl(last_transaction_type_05, nvl(last_transaction_type_04, nvl(last_transaction_type_03, nvl(last_transaction_type_02, nvl(last_transaction_type_01, nvl(last_transaction_type_00, last_transaction_type_in_past)))))))))))))))))))))))) last_transaction_type_23
    from
    (
        select
            msisdn,
            max(case when hour_period = '00' then last_ci end) last_ci_00,
            max(case when hour_period = '01' then last_ci end) last_ci_01,
            max(case when hour_period = '02' then last_ci end) last_ci_02,
            max(case when hour_period = '03' then last_ci end) last_ci_03,
            max(case when hour_period = '04' then last_ci end) last_ci_04,
            max(case when hour_period = '05' then last_ci end) last_ci_05,
            max(case when hour_period = '06' then last_ci end) last_ci_06,
            max(case when hour_period = '07' then last_ci end) last_ci_07,
            max(case when hour_period = '08' then last_ci end) last_ci_08,
            max(case when hour_period = '09' then last_ci end) last_ci_09,
            max(case when hour_period = '10' then last_ci end) last_ci_10,
            max(case when hour_period = '11' then last_ci end) last_ci_11,
            max(case when hour_period = '12' then last_ci end) last_ci_12,
            max(case when hour_period = '13' then last_ci end) last_ci_13,
            max(case when hour_period = '14' then last_ci end) last_ci_14,
            max(case when hour_period = '15' then last_ci end) last_ci_15,
            max(case when hour_period = '16' then last_ci end) last_ci_16,
            max(case when hour_period = '17' then last_ci end) last_ci_17,
            max(case when hour_period = '18' then last_ci end) last_ci_18,
            max(case when hour_period = '19' then last_ci end) last_ci_19,
            max(case when hour_period = '20' then last_ci end) last_ci_20,
            max(case when hour_period = '21' then last_ci end) last_ci_21,
            max(case when hour_period = '22' then last_ci end) last_ci_22,
            max(case when hour_period = '23' then last_ci end) last_ci_23,

            max(case when hour_period = '00' then last_lac end) last_lac_00,
            max(case when hour_period = '01' then last_lac end) last_lac_01,
            max(case when hour_period = '02' then last_lac end) last_lac_02,
            max(case when hour_period = '03' then last_lac end) last_lac_03,
            max(case when hour_period = '04' then last_lac end) last_lac_04,
            max(case when hour_period = '05' then last_lac end) last_lac_05,
            max(case when hour_period = '06' then last_lac end) last_lac_06,
            max(case when hour_period = '07' then last_lac end) last_lac_07,
            max(case when hour_period = '08' then last_lac end) last_lac_08,
            max(case when hour_period = '09' then last_lac end) last_lac_09,
            max(case when hour_period = '10' then last_lac end) last_lac_10,
            max(case when hour_period = '11' then last_lac end) last_lac_11,
            max(case when hour_period = '12' then last_lac end) last_lac_12,
            max(case when hour_period = '13' then last_lac end) last_lac_13,
            max(case when hour_period = '14' then last_lac end) last_lac_14,
            max(case when hour_period = '15' then last_lac end) last_lac_15,
            max(case when hour_period = '16' then last_lac end) last_lac_16,
            max(case when hour_period = '17' then last_lac end) last_lac_17,
            max(case when hour_period = '18' then last_lac end) last_lac_18,
            max(case when hour_period = '19' then last_lac end) last_lac_19,
            max(case when hour_period = '20' then last_lac end) last_lac_20,
            max(case when hour_period = '21' then last_lac end) last_lac_21,
            max(case when hour_period = '22' then last_lac end) last_lac_22,
            max(case when hour_period = '23' then last_lac end) last_lac_23,

            max(case when hour_period = '00' then last_transaction_time end) last_transaction_time_00,
            max(case when hour_period = '01' then last_transaction_time end) last_transaction_time_01,
            max(case when hour_period = '02' then last_transaction_time end) last_transaction_time_02,
            max(case when hour_period = '03' then last_transaction_time end) last_transaction_time_03,
            max(case when hour_period = '04' then last_transaction_time end) last_transaction_time_04,
            max(case when hour_period = '05' then last_transaction_time end) last_transaction_time_05,
            max(case when hour_period = '06' then last_transaction_time end) last_transaction_time_06,
            max(case when hour_period = '07' then last_transaction_time end) last_transaction_time_07,
            max(case when hour_period = '08' then last_transaction_time end) last_transaction_time_08,
            max(case when hour_period = '09' then last_transaction_time end) last_transaction_time_09,
            max(case when hour_period = '10' then last_transaction_time end) last_transaction_time_10,
            max(case when hour_period = '11' then last_transaction_time end) last_transaction_time_11,
            max(case when hour_period = '12' then last_transaction_time end) last_transaction_time_12,
            max(case when hour_period = '13' then last_transaction_time end) last_transaction_time_13,
            max(case when hour_period = '14' then last_transaction_time end) last_transaction_time_14,
            max(case when hour_period = '15' then last_transaction_time end) last_transaction_time_15,
            max(case when hour_period = '16' then last_transaction_time end) last_transaction_time_16,
            max(case when hour_period = '17' then last_transaction_time end) last_transaction_time_17,
            max(case when hour_period = '18' then last_transaction_time end) last_transaction_time_18,
            max(case when hour_period = '19' then last_transaction_time end) last_transaction_time_19,
            max(case when hour_period = '20' then last_transaction_time end) last_transaction_time_20,
            max(case when hour_period = '21' then last_transaction_time end) last_transaction_time_21,
            max(case when hour_period = '22' then last_transaction_time end) last_transaction_time_22,
            max(case when hour_period = '23' then last_transaction_time end) last_transaction_time_23,

            max(case when hour_period = '00' then last_transaction_type end) last_transaction_type_00,
            max(case when hour_period = '01' then last_transaction_type end) last_transaction_type_01,
            max(case when hour_period = '02' then last_transaction_type end) last_transaction_type_02,
            max(case when hour_period = '03' then last_transaction_type end) last_transaction_type_03,
            max(case when hour_period = '04' then last_transaction_type end) last_transaction_type_04,
            max(case when hour_period = '05' then last_transaction_type end) last_transaction_type_05,
            max(case when hour_period = '06' then last_transaction_type end) last_transaction_type_06,
            max(case when hour_period = '07' then last_transaction_type end) last_transaction_type_07,
            max(case when hour_period = '08' then last_transaction_type end) last_transaction_type_08,
            max(case when hour_period = '09' then last_transaction_type end) last_transaction_type_09,
            max(case when hour_period = '10' then last_transaction_type end) last_transaction_type_10,
            max(case when hour_period = '11' then last_transaction_type end) last_transaction_type_11,
            max(case when hour_period = '12' then last_transaction_type end) last_transaction_type_12,
            max(case when hour_period = '13' then last_transaction_type end) last_transaction_type_13,
            max(case when hour_period = '14' then last_transaction_type end) last_transaction_type_14,
            max(case when hour_period = '15' then last_transaction_type end) last_transaction_type_15,
            max(case when hour_period = '16' then last_transaction_type end) last_transaction_type_16,
            max(case when hour_period = '17' then last_transaction_type end) last_transaction_type_17,
            max(case when hour_period = '18' then last_transaction_type end) last_transaction_type_18,
            max(case when hour_period = '19' then last_transaction_type end) last_transaction_type_19,
            max(case when hour_period = '20' then last_transaction_type end) last_transaction_type_20,
            max(case when hour_period = '21' then last_transaction_type end) last_transaction_type_21,
            max(case when hour_period = '22' then last_transaction_type end) last_transaction_type_22,
            max(case when hour_period = '23' then last_transaction_type end) last_transaction_type_23

        from
        (
            select
                nvl(a.msisdn, b.msisdn) msisdn,
                nvl(a.hour_period, b.hour_period) hour_period,
                (
                    case
                        when a.last_transaction_time>=b.last_transaction_time then a.last_ci
                        else b.last_ci
                    end
                ) last_ci,
                (
                    case
                        when a.last_transaction_time>=b.last_transaction_time then a.last_lac
                        else b.last_lac
                    end
                ) last_lac,
                (
                    case
                        when a.last_transaction_time>=b.last_transaction_time then a.last_transaction_time
                        else b.last_transaction_time
                    end
                ) last_transaction_time,
                (
                    case
                        when a.last_transaction_time>=b.last_transaction_time then 'DATA'
                        else b.last_transaction_type
                    end
                ) last_transaction_type
            from
            (
                select
                    served_party_msisdn msisdn,
                    hour_period,
                    max(last_transaction_time) last_transaction_time,
                    max(lpad(last_ci, 5, 0)) last_ci,
                    max(lpad(last_lac, 5, 0)) last_lac
                from
                (
                    select
                        served_party_msisdn,
                        substr(session_time, 1, 2) hour_period,
                        max(session_time) over(partition by served_party_msisdn, substr(session_time, 1, 2)) last_transaction_time,
                        first_value(location_ci) over(partition by served_party_msisdn, substr(session_time, 1, 2) order by session_time desc) last_ci,
                        first_value(location_lac) over(partition by served_party_msisdn, substr(session_time, 1, 2) order by session_time desc) last_lac
                    from mon.spark_ft_cra_gprs
                    where session_date = '###SLICE_VALUE###' and nvl(bytes_sent, 0) + nvl(bytes_received, 0) > 0
                ) a0
                group by served_party_msisdn,
                    hour_period
            ) a
            full join
            (
                select
                    served_msisdn msisdn,
                    hour_period,
                    max(last_transaction_time) last_transaction_time,
                    max(last_transaction_type) last_transaction_type,
                    max(lpad(last_ci, 5, 0)) last_ci,
                    max(lpad(last_lac, 5, 0)) last_lac
                from
                (
                    select
                        served_msisdn,
                        substr(transaction_time, 1, 2) hour_period,
                        max(transaction_time) over(partition by served_msisdn, substr(transaction_time, 1, 2)) last_transaction_time,
                        first_value(substr(transaction_type, 1, 3)) over(partition by served_msisdn, substr(transaction_time, 1, 2) order by transaction_time desc) last_transaction_type,
                        first_value(Substr(served_party_location, 14, 5)) over(partition by served_msisdn, substr(transaction_time, 1, 2) order by transaction_time desc) last_ci,
                        first_value(Substr(served_party_location, -11, 5)) over(partition by served_msisdn, substr(transaction_time, 1, 2) order by transaction_time desc) last_lac
                    from mon.spark_ft_msc_transaction
                    where transaction_date = '###SLICE_VALUE###'
                        and served_party_location LIKE '624-02-%'
                ) b0
                group by served_msisdn,
                    hour_period
            ) b
            on a.msisdn = b.msisdn and a.hour_period = b.hour_period
        ) z0
        group by msisdn
    ) Z0
    left JOIN
    (
        select
            msisdn,
            last_ci last_ci_in_past,
            last_lac last_lac_in_past,
            last_transaction_time last_transaction_time_in_past,
            last_transaction_type last_transaction_type_in_past
        from
        (
            select
                msisdn,
                last_ci,
                last_lac,
                last_transaction_time,
                last_transaction_type,
                row_number() over(partition by msisdn order by event_date desc) row_number_for_msisdn
            from mon.spark_ft_client_cell_traffic_hour
            where event_date between add_months('###SLICE_VALUE###', -3) and date_sub('###SLICE_VALUE###', 1) and hour_period = '23'
        ) z10 where row_number_for_msisdn = 1
    ) Z1 on z0.msisdn = z1.msisdn
) Z
