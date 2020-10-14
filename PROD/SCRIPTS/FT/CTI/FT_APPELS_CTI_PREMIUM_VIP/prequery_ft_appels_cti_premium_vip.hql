select if(it_exist>${hivevar:seuil_nb_ligne} and ft_exist=0 , 'OK', 'NOK') val from
( select count(*) it_exist from CTI.IT_IRF_USER_DATA_CUST where event_date = '###SLICE_VALUE###' ) a,
( select count(*) ft_exist from CTI.FT_APPELS_CTI_PREMIUM_VIP where event_date = '###SLICE_VALUE###' ) b
