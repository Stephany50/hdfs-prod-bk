SELECT IF(T_1.FT_EXIST = 0 and T_2.FT_MONTH1>0 and T_3.FT_MONTH2>0 and T_4.FT_MONTH3>0 ,"OK","NOK") IT_CTI_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.FT_MSISDN_PREMIUM_MONTH WHERE EVENT_MONTH = '###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_MONTH1 FROM MON.FT_MARKETING_DATAMART_MONTH WHERE EVENT_MONTH = date_format(add_months(from_unixtime(unix_timestamp('###SLICE_VALUE###' , 'yyyyMM')), -1), 'yyyyMM')) T_2,
(SELECT COUNT(*) FT_MONTH2 FROM MON.FT_MARKETING_DATAMART_MONTH WHERE EVENT_MONTH = date_format(add_months(from_unixtime(unix_timestamp('###SLICE_VALUE###' , 'yyyyMM')), -2), 'yyyyMM')) T_3,
(SELECT COUNT(*) FT_MONTH3 FROM MON.FT_MARKETING_DATAMART_MONTH WHERE EVENT_MONTH = date_format(add_months(from_unixtime(unix_timestamp('###SLICE_VALUE###' , 'yyyyMM')), -3), 'yyyyMM')) T_4
