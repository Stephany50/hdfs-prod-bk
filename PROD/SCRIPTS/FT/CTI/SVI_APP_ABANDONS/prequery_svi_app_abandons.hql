SELECT IF(T_1.FT_EXIST = 0 and T_2.SVI_APPEL_SELFCARE>0 and T_3.SVI_APPEL>0 and T_4.SVI_APP_TRANSFERE>0   ,"OK","NOK") SVI_APP_ABANDONS
FROM
(SELECT COUNT(*) FT_EXIST FROM CTI.SVI_APP_ABANDONS WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) SVI_APPEL_SELFCARE FROM CTI.SVI_APPEL_SELFCARE WHERE EVENT_DATE='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) SVI_APPEL FROM CTI.SVI_APPEL WHERE DATE_DEBUT_OMS='###SLICE_VALUE###') T_3,
(SELECT COUNT(*) SVI_APP_TRANSFERE FROM CTI.SVI_APP_TRANSFERE WHERE EVENT_DATE='###SLICE_VALUE###') T_4