SELECT IF(T_1.FT_EXIST = 0 and T_2.svi_navigation>0 and T_3.svi_appel>0,"OK","NOK") FT_CALL_TERMINATION
FROM
(SELECT COUNT(*) FT_EXIST FROM CTI.FT_CALL_TERMINATION WHERE DATE_ELEMENT='###SLICE_VALUE###' ) T_1,
(SELECT COUNT(*) SVI_NAVIGATION FROM CTI.SVI_NAVIGATION WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') T_2,
(SELECT COUNT(*)  SVI_APPEL FROM CTI.SVI_APPEL WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') T_3