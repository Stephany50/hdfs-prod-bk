select a.*, offre, cout, airtime, voix_bonus_on_net, voix_bonus_off_net, voix_bonus_cross_net, voix_bonus_intl_en_s, sms_onnet, sms_off_net, bonus_data_en_ko from mon.spark_ft_marketing_b2b a
INNER join DIM.DT_MARKETING_B2B b
on upper(a.commercial_offer)= upper(b.offre)
where event_date ='###SLICE_VALUE###'
and UPPER(commercial_offer) like '%FLEX PLUS %K%'