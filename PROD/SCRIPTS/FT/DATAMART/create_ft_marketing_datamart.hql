CREATE TABLE MON.FT_MARKETING_DATAMART 
(
msisdn                          varchar(50),
loc_site_name                   varchar(50),
loc_town_name                   varchar(50),
loc_admintrative_region         varchar(50),
loc_commercial_region           varchar(50),
ter_tac_code                    varchar(8) ,
ter_constructor                 varchar(255) ,
ter_model                       varchar(255) ,
ter_2g_3g_4g_compatibility      varchar(50),
ter_2g_compatibility            char(1)    ,
ter_3g_compatibility            char(1)    ,
ter_4g_compatibility            char(1)    ,
contract_type                   varchar(50),
commercial_offer                varchar(50),
activation_date                 timestamp  ,
deactivation_date               timestamp  ,
lang                            varchar(5) ,
osp_status                      varchar(30),
imsi                            varchar(25),
grp_last_og_call                timestamp  ,
grp_last_ic_call                timestamp  ,
grp_remain_credit_main          decimal(20,2),
grp_remain_credit_promo         decimal(20,2),
grp_gp_status                   varchar(5) ,
dir_first_name                  varchar(250) ,
dir_last_name                   varchar(250) ,
dir_birth_date                  timestamp  ,
dir_identification_town         varchar(250) ,
dir_identification_date         timestamp  ,
data_main_rated_amount          decimal(20,2),
data_gos_main_rated_amount      decimal(20,2),
data_promo_rated_amount         decimal(20,2),
data_roam_main_rated_amount     decimal(20,2),
data_roam_promo_rated_amount    decimal(20,2),
data_bytes_received             decimal(20,2),
data_bytes_sent                 decimal(20,2),
data_bytes_used_in_bundle       decimal(20,2),
data_bytes_used_paygo           decimal(20,2),
data_bytes_used_in_bundle_roam  decimal(20,2),
data_byte_used_out_bundle_roam  decimal(20,2),
data_mms_used                   decimal(20,2),
data_mms_used_in_bundle         decimal(20,2),
sos_loan_count                  decimal(20,2),
sos_loan_amount                 decimal(20,2),
sos_reimbursement_count         decimal(20,2),
sos_reimbursement_amount        decimal(20,2),
sos_fees                        decimal(20,2),
total_revenue                   decimal(20,2),
total_voice_revenue             decimal(20,2),
total_sms_revenue               decimal(20,2),
total_subs_revenue              decimal(20,2),
c2s_refill_count                decimal(20,2),
c2s_main_refill_amount          decimal(20,2),
c2s_promo_refill_amount         decimal(20,2),
p2p_refill_count                decimal(20,2),
p2p_refill_amount               decimal(20,2),
scratch_refill_count            decimal(20,2),
scratch_main_refill_amount      decimal(20,2),
scratch_promo_refill_amount     decimal(20,2),
p2p_refill_fees                 decimal(20,2),
total_data_revenue              decimal(20,2),
roam_data_revenue               decimal(20,2),
roam_in_voice_revenue           decimal(20,2),
roam_out_voice_revenue          decimal(20,2),
roam_in_sms_revenue             decimal(20,2),
roam_out_sms_revenue            decimal(20,2),
og_call_total_count             decimal(20,2),
og_call_ocm_count               decimal(20,2),
og_call_mtn_count               decimal(20,2),
og_call_nexttel_count           decimal(20,2),
og_call_camtel_count            decimal(20,2),
og_call_set_count               decimal(20,2),
og_call_roam_in_count           decimal(20,2),
og_call_roam_out_count          decimal(20,2),
og_call_sva_count               decimal(20,2),
og_call_international_count     decimal(20,2),
og_rated_call_duration          decimal(20,2),
og_total_call_duration          decimal(20,2),
rated_tel_ocm_duration          decimal(20,2),
rated_tel_mtn_duration          decimal(20,2),
rated_tel_nexttel_duration      decimal(20,2),
rated_tel_camtel_duration       decimal(20,2),
rated_tel_set_duration          decimal(20,2),
rated_tel_roam_in_duration      decimal(20,2),
rated_tel_roam_out_duration     decimal(20,2),
rated_tel_sva_duration          decimal(20,2),
rated_tel_int_duration          decimal(20,2),
main_rated_tel_amount           decimal(20,2),
promo_rated_tel_amount          decimal(20,2),
og_total_rated_call_amount      decimal(20,2),
main_rated_tel_ocm_amount       decimal(20,2),
main_rated_tel_mtn_amount       decimal(20,2),
main_rated_tel_nexttel_amount   decimal(20,2),
main_rated_tel_camtel_amount    decimal(20,2),
main_rated_tel_set_amount       decimal(20,2),
main_rated_tel_roam_in_amount   decimal(20,2),
main_rated_tel_roam_out_amount  decimal(20,2),
main_rated_tel_sva_amount       decimal(20,2),
main_rated_tel_int_amount       decimal(20,2),
promo_rated_tel_ocm_amount      decimal(20,2),
promo_rated_tel_mtn_amount      decimal(20,2),
promo_rated_tel_nexttel_amount  decimal(20,2),
promo_rated_tel_camtel_amount   decimal(20,2),
promo_rated_tel_set_amount      decimal(20,2),
promo_rated_tel_roamin_amount   decimal(20,2),
promo_rated_tel_roamout_amount  decimal(20,2),
promo_rated_tel_int_amount      decimal(20,2),
og_sms_total_count              decimal(20,2),
og_sms_ocm_count                decimal(20,2),
og_sms_mtn_count                decimal(20,2),
og_sms_nexttel_count            decimal(20,2),
og_sms_camtel_count             decimal(20,2),
og_sms_set_count                decimal(20,2),
og_sms_roam_in_count            decimal(20,2),
og_sms_roam_out_count           decimal(20,2),
og_sms_sva_count                decimal(20,2),
og_sms_international_count      decimal(20,2),
main_rated_sms_amount           decimal(20,2),
main_rated_sms_ocm_amount       decimal(20,2),
main_rated_sms_mtn_amount       decimal(20,2),
main_rated_sms_nexttel_amount   decimal(20,2),
main_rated_sms_camtel_amount    decimal(20,2),
main_rated_sms_set_amount       decimal(20,2),
main_rated_sms_roam_in_amount   decimal(20,2),
main_rated_sms_roam_out_amount  decimal(20,2),
main_rated_sms_sva_amount       decimal(20,2),
main_rated_sms_int_amount       decimal(20,2),
subs_bundle_plenty_count        decimal(20,2),
subs_orange_bonus_count         decimal(20,2),
subs_fnf_modify_count           decimal(20,2),
subs_data_2g_count              decimal(20,2),
subs_data_3g_jour_count         decimal(20,2),
subs_data_3g_semaine_count      decimal(20,2),
subs_data_3g_mois_count         decimal(20,2),
subs_maxi_bonus_count           decimal(20,2),
subs_pro_count                  decimal(20,2),
subs_intern_jour_count          decimal(20,2),
subs_intern_semaine_count       decimal(20,2),
subs_intern_mois_count          decimal(20,2),
subs_pack_sms_jour_count        decimal(20,2),
subs_pack_sms_semaine_count     decimal(20,2),
subs_pack_sms_mois_count        decimal(20,2),
subs_ws_count                   decimal(20,2),
subs_obonus_illimite_count      decimal(20,2),
subs_orange_phenix_count        decimal(20,2),
subs_recharge_plenty_count      decimal(20,2),
subs_autres_count               decimal(20,2),
subs_data_flybox_count          decimal(20,2),
subs_data_autres_count          decimal(20,2),
subs_roaming_count              decimal(20,2),
subs_sms_autres_count           decimal(20,2),
subs_bundle_plenty_amount       decimal(20,2),
subs_orange_bonus_amount        decimal(20,2),
subs_fnf_modify_amount          decimal(20,2),
subs_data_2g_amount             decimal(20,2),
subs_data_3g_jour_amount        decimal(20,2),
subs_data_3g_semaine_amount     decimal(20,2),
subs_data_3g_mois_amount        decimal(20,2),
subs_maxi_bonus_amount          decimal(20,2),
subs_pro_amount                 decimal(20,2),
subs_intern_jour_amount         decimal(20,2),
subs_intern_semaine_amount      decimal(20,2),
subs_intern_mois_amount         decimal(20,2),
subs_pack_sms_jour_amount       decimal(20,2),
subs_pack_sms_semaine_amount    decimal(20,2),
subs_pack_sms_mois_amount       decimal(20,2),
subs_ws_amount                  decimal(20,2),
subs_obonus_illimite_amount     decimal(20,2),
subs_orange_phenix_amount       decimal(20,2),
subs_recharge_plenty_amount     decimal(20,2),
subs_autres_amount              decimal(20,2),
subs_data_flybox_amount         decimal(20,2),
subs_data_autres_amount         decimal(20,2),
subs_roaming_amount             decimal(20,2),
subs_sms_autres_amount          decimal(20,2),
insert_date                     timestamp  ,
refresh_date                    timestamp  ,
subs_voice_count                decimal(20,2),
subs_sms_count                  decimal(20,2),
subs_data_count                 decimal(20,2),
subs_voice_amount               decimal(20,2),
subs_sms_amount                 decimal(20,2),
subs_data_amount                decimal(20,2),
revenu_moyen                    decimal(20,2),
premium                         decimal(20,2),
conso_moy_data                  decimal(20,2),
recharge_moy                    decimal(20,2),
premium_plus                    decimal(20,2),
ter_imei                        varchar(16)
)PARTITIONED BY (EVENT_DATE DATE)
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")  ;