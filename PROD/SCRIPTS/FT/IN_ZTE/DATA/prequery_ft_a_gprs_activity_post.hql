SELECT IF(T_1.FT_A_COUNT = 0 and T_2.FT_COUNT > 1,"OK","NOK") FT_EXIST
FROM
(SELECT COUNT(*) FT_A_COUNT FROM MON.FT_A_GPRS_ACTIVITY_POST WHERE DATECODE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_COUNT FROM MON.FT_CRA_GPRS_POST WHERE SESSION_DATE='###SLICE_VALUE###') T_2;
