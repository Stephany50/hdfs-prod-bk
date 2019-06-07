
SELECT IF(T_1.FT_EXIST = 0 and T_2.IT_INTERACTION_RESOURCE_FACT>0 and T_3.FT_IRF_USER_DATA_KEYS>0 and T_4.FT_IRF_USER_DATA_CUST_1>0,"OK","NOK") IT_CTI_EXIST
FROM
    (SELECT COUNT(*) FT_EXIST FROM CTI.FT_CALL_AGENT WHERE SDATE='###SLICE_VALUE###') T_1,
    (SELECT COUNT(*) IT_INTERACTION_RESOURCE_FACT FROM CTI.IT_INTERACTION_RESOURCE_FACT WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') T_2,
    (SELECT COUNT(*) FT_IRF_USER_DATA_KEYS FROM CTI.FT_IRF_USER_DATA_KEYS WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') T_3,
    (SELECT COUNT(*) FT_IRF_USER_DATA_CUST_1 FROM CTI.FT_IRF_USER_DATA_CUST_1 WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') T_4