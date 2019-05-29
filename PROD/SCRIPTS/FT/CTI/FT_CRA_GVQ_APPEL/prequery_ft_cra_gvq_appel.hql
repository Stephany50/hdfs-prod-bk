SELECT IF(T_1.FT_EXIST = 0 and T_2.FT_MEDIATION_SEGMENT_FACT>0 and T_3.IT_INTERACTION_RESOURCE_FACT>0 and T_4.FT_IRF_USER_DATA_KEYS>0 and T_5.FT_IRF_USER_DATA_CUST_1>0 and T_6.ft_resource_group_fact>0 and T_7.ft_resource_group_fact>0,"OK","NOK") IT_CTI_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM CTI.FT_CRA_GVQ_APPEL WHERE CALL_DATE='2019-05-27') T_1,
(SELECT COUNT(*) FT_MEDIATION_SEGMENT_FACT FROM CTI.FT_MEDIATION_SEGMENT_FACT WHERE ORIGINAL_FILE_DATE='2019-05-27') T_2,
(SELECT COUNT(*) IT_INTERACTION_RESOURCE_FACT FROM CTI.IT_INTERACTION_RESOURCE_FACT WHERE ORIGINAL_FILE_DATE='2019-05-27') T_3,
(SELECT COUNT(*) FT_IRF_USER_DATA_KEYS FROM CTI.FT_IRF_USER_DATA_KEYS WHERE ORIGINAL_FILE_DATE='2019-05-27') T_4,
(SELECT COUNT(*) FT_IRF_USER_DATA_CUST_1 FROM CTI.FT_IRF_USER_DATA_CUST_1 WHERE ORIGINAL_FILE_DATE='2019-05-27') T_5,
(SELECT COUNT(*) ft_resource_group_fact FROM CTI.ft_resource_group_fact WHERE ORIGINAL_FILE_DATE='2019-05-27') T_6,
(SELECT COUNT(*) ft_resource_group_fact FROM CTI.ft_resource_group_fact WHERE ORIGINAL_FILE_DATE='2019-05-27') T_7

