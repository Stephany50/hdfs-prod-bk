SELECT IF(T_1.FT_EXIST = 0 and T_2.SVI_APPEL>0 and T_3.SVI_NAVIGATION>0  ,"OK","NOK") SVI_APP_TRANSFERE
FROM
(SELECT COUNT(*) FT_EXIST FROM CTI.SVI_APP_TRANSFERE WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) SVI_APPEL FROM CTI.SVI_APPEL WHERE DATE_DEBUT_OMS='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) SVI_NAVIGATION FROM CTI.SVI_NAVIGATION WHERE DATE_ELEMENT ='###SLICE_VALUE###')T_3