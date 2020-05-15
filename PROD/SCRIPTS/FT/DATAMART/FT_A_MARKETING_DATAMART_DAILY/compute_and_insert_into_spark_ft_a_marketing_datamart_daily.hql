INSERT INTO AGG.SPARK_FT_A_MARKETING_DATAMART_DAILY

SELECT
--     msisdn,
--     loc_site_name,
    count(distinct msisdn) parc,
    sum(case when activation_date = event_date  then 1 else 0 end) gross_adds,
    sum(case when deactivation_date = event_date  then 1 else 0 end) deconnexions,
    loc_town_name,
    loc_admintrative_region,
    loc_commercial_region,
--     ter_tac_code,
--     ter_constructor,
--     ter_model,
--     ter_2g_3g_4g_compatibility,
--     ter_2g_compatibility,
--     ter_3g_compatibility,
--     ter_4g_compatibility,
    contract_type,
    commercial_offer,
    --  activation_date,
 --     deactivation_date,
--     lang,
    osp_status,
--     imsi,
--     grp_last_og_call,
--     grp_last_ic_call,
    sum(grp_remain_credit_main         ),
    sum(grp_remain_credit_promo        ),
    grp_gp_status,
--     dir_first_name,
--     dir_last_name,
--     dir_birth_date,
--     dir_identification_town,
--     dir_identification_date,
    sum(data_main_rated_amount         ),
    sum(data_gos_main_rated_amount     ),
    sum(data_promo_rated_amount        ),
    sum(data_roam_main_rated_amount    ),
    sum(data_roam_promo_rated_amount   ),
    sum(data_bytes_received            ),
    sum(data_bytes_sent                ),
    sum(data_bytes_used_in_bundle      ),
    sum(data_bytes_used_paygo          ),
    sum(data_bytes_used_in_bundle_roam ),
    sum(data_byte_used_out_bundle_roam ),
    sum(data_mms_used                  ),
    sum(data_mms_used_in_bundle        ),
    sum(sos_loan_count                 ),
    sum(sos_loan_amount                ),
    sum(sos_reimbursement_count        ),
    sum(sos_reimbursement_amount       ),
    sum(sos_fees                       ),
    sum(total_revenue                  ),
    sum(total_voice_revenue            ),
    sum(total_sms_revenue              ),
    sum(total_subs_revenue             ),
    sum(c2s_refill_count               ),
    sum(c2s_main_refill_amount         ),
    sum(c2s_promo_refill_amount        ),
    sum(p2p_refill_count               ),
    sum(p2p_refill_amount              ),
    sum(scratch_refill_count           ),
    sum(scratch_main_refill_amount     ),
    sum(scratch_promo_refill_amount    ),
    sum(p2p_refill_fees                ),
    sum(total_data_revenue             ),
    sum(roam_data_revenue              ),
    sum(roam_in_voice_revenue          ),
    sum(roam_out_voice_revenue         ),
    sum(roam_in_sms_revenue            ),
    sum(roam_out_sms_revenue           ),
    sum(og_call_total_count            ),
    sum(og_call_ocm_count              ),
    sum(og_call_mtn_count              ),
    sum(og_call_nexttel_count          ),
    sum(og_call_camtel_count           ),
    sum(og_call_set_count              ),
    sum(og_call_roam_in_count          ),
    sum(og_call_roam_out_count         ),
    sum(og_call_sva_count              ),
    sum(og_call_international_count    ),
    sum(og_rated_call_duration         ),
    sum(og_total_call_duration         ),
    sum(rated_tel_ocm_duration         ),
    sum(rated_tel_mtn_duration         ),
    sum(rated_tel_nexttel_duration     ),
    sum(rated_tel_camtel_duration      ),
    sum(rated_tel_set_duration         ),
    sum(rated_tel_roam_in_duration     ),
    sum(rated_tel_roam_out_duration    ),
    sum(rated_tel_sva_duration         ),
    sum(rated_tel_int_duration         ),
    sum(main_rated_tel_amount          ),
    sum(promo_rated_tel_amount         ),
    sum(og_total_rated_call_amount     ),
    sum(main_rated_tel_ocm_amount      ),
    sum(main_rated_tel_mtn_amount      ),
    sum(main_rated_tel_nexttel_amount  ),
    sum(main_rated_tel_camtel_amount   ),
    sum(main_rated_tel_set_amount      ),
    sum(main_rated_tel_roam_in_amount  ),
    sum(main_rated_tel_roam_out_amount ),
    sum(main_rated_tel_sva_amount      ),
    sum(main_rated_tel_int_amount      ),
    sum(promo_rated_tel_ocm_amount     ),
    sum(promo_rated_tel_mtn_amount     ),
    sum(promo_rated_tel_nexttel_amount ),
    sum(promo_rated_tel_camtel_amount  ),
    sum(promo_rated_tel_set_amount     ),
    sum(promo_rated_tel_roamin_amount  ),
    sum(promo_rated_tel_roamout_amount ),
    sum(promo_rated_tel_int_amount     ),
    sum(og_sms_total_count             ),
    sum(og_sms_ocm_count               ),
    sum(og_sms_mtn_count               ),
    sum(og_sms_nexttel_count           ),
    sum(og_sms_camtel_count            ),
    sum(og_sms_set_count               ),
    sum(og_sms_roam_in_count           ),
    sum(og_sms_roam_out_count          ),
    sum(og_sms_sva_count               ),
    sum(og_sms_international_count     ),
    sum(main_rated_sms_amount          ),
    sum(main_rated_sms_ocm_amount      ),
    sum(main_rated_sms_mtn_amount      ),
    sum(main_rated_sms_nexttel_amount  ),
    sum(main_rated_sms_camtel_amount   ),
    sum(main_rated_sms_set_amount      ),
    sum(main_rated_sms_roam_in_amount  ),
    sum(main_rated_sms_roam_out_amount ),
    sum(main_rated_sms_sva_amount      ),
    sum(main_rated_sms_int_amount      ),
    sum(subs_bundle_plenty_count       ),
    sum(subs_orange_bonus_count        ),
    sum(subs_fnf_modify_count          ),
    sum(subs_data_2g_count             ),
    sum(subs_data_3g_jour_count        ),
    sum(subs_data_3g_semaine_count     ),
    sum(subs_data_3g_mois_count        ),
    sum(subs_maxi_bonus_count          ),
    sum(subs_pro_count                 ),
    sum(subs_intern_jour_count         ),
    sum(subs_intern_semaine_count      ),
    sum(subs_intern_mois_count         ),
    sum(subs_pack_sms_jour_count       ),
    sum(subs_pack_sms_semaine_count    ),
    sum(subs_pack_sms_mois_count       ),
    sum(subs_ws_count                  ),
    sum(subs_obonus_illimite_count     ),
    sum(subs_orange_phenix_count       ),
    sum(subs_recharge_plenty_count     ),
    sum(subs_autres_count              ),
    sum(subs_data_flybox_count         ),
    sum(subs_data_autres_count         ),
    sum(subs_roaming_count             ),
    sum(subs_sms_autres_count          ),
    sum(subs_bundle_plenty_amount      ),
    sum(subs_orange_bonus_amount       ),
    sum(subs_fnf_modify_amount         ),
    sum(subs_data_2g_amount            ),
    sum(subs_data_3g_jour_amount       ),
    sum(subs_data_3g_semaine_amount    ),
    sum(subs_data_3g_mois_amount       ),
    sum(subs_maxi_bonus_amount         ),
    sum(subs_pro_amount                ),
    sum(subs_intern_jour_amount        ),
    sum(subs_intern_semaine_amount     ),
    sum(subs_intern_mois_amount        ),
    sum(subs_pack_sms_jour_amount      ),
    sum(subs_pack_sms_semaine_amount   ),
    sum(subs_pack_sms_mois_amount      ),
    sum(subs_ws_amount                 ),
    sum(subs_obonus_illimite_amount    ),
    sum(subs_orange_phenix_amount      ),
    sum(subs_recharge_plenty_amount    ),
    sum(subs_autres_amount             ),
    sum(subs_data_flybox_amount        ),
    sum(subs_data_autres_amount        ),
    sum(subs_roaming_amount            ),
    sum(subs_sms_autres_amount         ),
--     insert_date,
--     refresh_date,
    sum(subs_voice_count               ),
    sum(subs_sms_count                 ),
    sum(subs_data_count                ),
    sum(subs_voice_amount              ),
    sum(subs_sms_amount                ),
    sum(subs_data_amount               ),
    sum(revenu_moyen                   ),
    sum(premium                        ),
    sum(conso_moy_data                 ),
    sum(recharge_moy                   ),
    sum(premium_plus                   ),
    event_date
--     ter_imei,


FROM MON.SPARK_FT_MARKETING_DATAMART

WHERE event_date ='###SLICE_VALUE###' AND grp_gp_status= 'ACTIF'

GROUP BY
        event_date,
--         msisdn,
--         loc_site_name,
        loc_town_name,
        loc_admintrative_region,
        loc_commercial_region,
--         ter_tac_code,
--         ter_constructor,
--         ter_model,
--         ter_2g_3g_4g_compatibility,
--         ter_2g_compatibility,
--         ter_3g_compatibility,
--         ter_4g_compatibility,
        contract_type,
        commercial_offer,
--         activation_date,
--         deactivation_date,
--         lang,
        osp_status,
--         imsi,
--         grp_last_og_call,
--         grp_last_ic_call,

--         dir_first_name,
--         dir_last_name,
--         dir_birth_date,
--         dir_identification_town


--         dir_identification_date,
--         ter_imei,
        grp_gp_status



