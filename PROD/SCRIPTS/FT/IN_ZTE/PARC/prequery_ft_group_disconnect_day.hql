SELECT IF(FT_EXISTS=0 AND FT_SUBS_SUM_EXISTS>0 AND FT_SUBS_SUM_EXISTS_PREV>0 AND FT_A_SUBS_SUM_EXISTS>0,'OK','NOK' )
FROM
(SELECT COUNT(*) FT_SUBS_SUM_EXISTS FROM MON.ft_group_subscriber_summary WHERE EVENT_DATE='###SLICE_VALUE###')T1,
(SELECT COUNT(*) FT_SUBS_SUM_EXISTS_PREV FROM MON.ft_group_subscriber_summary WHERE EVENT_DATE=DATE_SUB('###SLICE_VALUE###',1))T2,
(SELECT COUNT(*) FT_A_SUBS_SUM_EXISTS FROM AGG.ft_a_subscriber_summary WHERE DATECODE=DATE_SUB('###SLICE_VALUE###',1))T3,
(SELECT COUNT(*) FT_EXISTS FROM MON.FT_GROUP_DISCONNECT_DAY WHERE SDATE='###SLICE_VALUE###')T4