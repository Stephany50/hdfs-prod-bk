-- ---***********************************************************---
---------Prequery FT OVERDRAFT ---------------------------
-------- ARNOLD CHUENFFO 20-03-2019 
---***********************************************************---



SELECT IF(T_1.FT_OVD_COUNT = 0 and T_2.FT_CSN_COUNT > 1,"OK","NOK") FT_EXIST
FROM
(SELECT COUNT(*) FT_OVD_COUNT FROM MON.FT_OVERDRAFT WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_CSN_COUNT FROM MON.FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE='###SLICE_VALUE###') T_2
;