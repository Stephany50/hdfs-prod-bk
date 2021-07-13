
insert into mon.spark_ft_customer_base
select
	T.msisdn msisdn,
	max(parc_group) parc_group,
	max(parc_art) parc_act,
	max(parc_om) parc_om,
	max(parc_actif_om) parc_actif_om,
	max(all30daysbase) all30daysbase,
	max(dailybase) dailybase,
	max(all90dayslost) all90dayslost,
	max(all30dayslost) all30dayslost,
	max(lostat90days) lostat90days,
	max(lostat30days) lostat30days,
	max(all90dayswinback) all90dayswinback,
	max(all30dayswinback) all30dayswinback,
	max(winbackat90days) winbackat90days,
	max(winbackat30days) winbackat30days,
	max(churner) churner,
	max(gross_add) gross_add,
	max(gross_add_om) gross_add_om,
	max(smartphone_user) smartphone_user,
	max(segment_valeur_premium) segment_valeur_premium,
	max(segment_valeur_high_value) segment_valeur_high_value,
	max(segment_valeur_telco) segment_valeur_telco,
	max(type_usage) type_usage,
	max(anciennete) anciennete,
	max(typedezone) statut_urbanite,
	max(COALESCE(upper(site.site_name_b),upper(site.site_name_a),R.site_name)) site_name,
	max(COALESCE(upper(site.townname_b),upper(site.townname_a), TOWNNAME)) ville,
	max(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), REGION)) region_administrative,
	max(COALESCE(upper(site.commercial_region_b),upper(site.commercial_region_a), commercial_region)) REGION_COMMERCIAL,
	current_timestamp insert_date,
	'###SLICE_VALUE###' event_date
from
(
	
	select
		msisdn,
		parc_group,
		parc_art,
		parc_om,
		parc_actif_om,
		all30daysbase,
		dailybase,
		all90dayslost,
		all30dayslost,
		lostat90days,
		lostat30days,
		all90dayswinback,
		all30dayswinback,
		winbackat90days,
		winbackat30days,
		churner,
		gross_add,
		gross_add_om,
		smartphone_user,
		anciennete,
		case
			when revenu_telco > 5000 then 'high_value'
			when revenu_telco > 1000 and revenu_telco <= 5000 then 'middle_value'
			when revenu_telco > 500 and revenu_telco <= 1000 then 'low_value'
			when revenu_telco > 0 and revenu_telco <= 500 then 'very_low_value'
			else 'no_value'
		end segment_valeur_telco,
		case 
			when revenu_telco > 5000 and revenu_telco <= 20000 then 'star'
			when revenu_telco > 20000 then 'premium'
			else NULL
		end segment_valeur_high_value,
		case
			when revenu_telco > 20000 and revenu_telco <= 40000 then 'cristal'
			when revenu_telco > 40000 and revenu_telco <= 80000 then 'gold'
			when revenu_telco > 80000 then 'diamond'
			else NULL
		end segment_valeur_premium,
		case
			when voice_user >= 1 and sms_user >= 1 and data_user >= 1 and vas_user >= 1 and parc_actif_om >= 1 then 'allstream_user' 
			when voice_user >= 1 and (data_user < 1 and vas_user < 1 and sms_user < 1 and parc_actif_om < 1) then 'voice_user_only' 
			when data_user >= 1 and (voice_user < 1 and vas_user < 1 and sms_user < 1 and parc_actif_om < 1) then 'data_user_only'
			when sms_user >= 1 and (voice_user < 1 and vas_user < 1 and data_user < 1 and parc_actif_om < 1) then 'sms_user_only'
			when vas_user >= 1 and (voice_user < 1 and sms_user < 1 and data_user < 1 and parc_actif_om < 1) then 'vas_only_user' 
			when voice_user >= 1 and data_user >= 1 then 'combo_sortant_user'
			when voice_user >= 1 and data_user >= 1 and parc_actif_om >= 1 then 'combo_sortant_om_user' 
			when (voice_user >= 1 or sms_user >= 1) and (vas_user < 1 and data_user < 1 and parc_actif_om < 1) then 'ic_user_only'
			else NULL
		end type_usage
	from
	(
		select
			msisdn,
			parc_group,
			parc_art,
			parc_om,
			parc_actif_om,
			all30daysbase,
			dailybase,
			all90dayslost,
			all30dayslost,
			lostat90days,
			lostat30days,
			all90dayswinback,
			all30dayswinback,
			winbackat90days,
			winbackat30days,
			churner,
			gross_add,
			gross_add_om,
			smartphone_user,
			anciennete,
			data_user,
			voice_user,
			sms_user,
			vas_user,
			(
				revenu_paygo_voice + 
				revenu_paygo_sms + 
				revenu_adjustment + 
				revenu_credit_compte_desactive + 
				revenu_p2p_voix + 
				revenu_p2p_data + 
				revenu_sos_credit_data + 
				revenu_sos_credit_voix + 
				revenu_vas_data + 
				revenu_voice_bundle + 
				revenu_sms_bundle + 
				revenu_data_bundle + 
				revenu_om_data
			) revenu_telco
		from
		(
			select 
				COALESCE
				(
					A.msisdn, B.msisdn, C.msisdn, D.msisdn, E.msisdn, F.msisdn, G.msisdn, H.msisdn, I.msisdn, 
					J.msisdn, K.msisdn, L.msisdn, M.msisdn, N.msisdn, O.msisdn, P.msisdn, Q.msisdn, 'INCONNU'
				) msisdn,
				nvl(parc_group, 0) parc_group,
				nvl(parc_art, 0) parc_art,
				nvl(parc_om, 0) parc_om,
				nvl(parc_actif_om, 0) parc_actif_om,
				nvl(all30daysbase, 0) all30daysbase,
				nvl(dailybase, 0) dailybase,
				nvl(all90dayslost, 0) all90dayslost,
				nvl(all30dayslost, 0) all30dayslost,
				nvl(lostat90days, 0) lostat90days,
				nvl(lostat30days, 0) lostat30days,
				nvl(all90dayswinback, 0) all90dayswinback,
				nvl(all30dayswinback, 0) all30dayswinback,
				nvl(winbackat90days, 0) winbackat90days,
				nvl(winbackat30days, 0) winbackat30days,
				nvl(churner, 0) churner,
				
				nvl(gross_add, 0) gross_add,
				nvl(gross_add_om, 0) gross_add_om,
				nvl(smartphone_user, 0) smartphone_user,

				anciennete,
				
				nvl(data_user, 0) data_user,
				nvl(voice_user, 0) voice_user,
				nvl(sms_user, 0) sms_user,
				nvl(vas_user, 0) vas_user,
				
				nvl(revenu_paygo_voice, 0) revenu_paygo_voice,
				nvl(revenu_paygo_sms, 0) revenu_paygo_sms,
				nvl(revenu_adjustment, 0) revenu_adjustment,
				nvl(revenu_credit_compte_desactive, 0) revenu_credit_compte_desactive,
				nvl(revenu_p2p_voix, 0) revenu_p2p_voix,
				nvl(revenu_p2p_data, 0) revenu_p2p_data,
				nvl(revenu_sos_credit_data, 0) revenu_sos_credit_data,
				nvl(revenu_sos_credit_voix, 0) revenu_sos_credit_voix,
				nvl(revenu_vas_data, 0) revenu_vas_data,
				nvl(revenu_voice_bundle, 0) revenu_voice_bundle,
				nvl(revenu_sms_bundle, 0) revenu_sms_bundle,
				nvl(revenu_data_bundle, 0) revenu_data_bundle,
				nvl(revenu_om_data, 0) revenu_om_data
			from
			(
				select
					msisdn,
					sum(
						case 
							when OG_CALL >= DATE_SUB(event_date,31) or least(IC_CALL_4,IC_CALL_3,IC_CALL_2,IC_CALL_1) >= DATE_SUB(event_date,31) then 1 
							else 0 
						end
					) all30daysbase,
					sum(
						case 
							when OG_CALL >= DATE_SUB(event_date,1) or least(IC_CALL_4,IC_CALL_3,IC_CALL_2,IC_CALL_1) >= DATE_SUB(event_date,1) then 1 
							else 0 
						end
					) dailybase,
					sum(
						case 
							when GP_STATUS_DATE >= DATE_SUB(event_date,31) and GP_STATUS = 'INACT' then 1 
							else 0 
						end
					) all30dayslost,
					sum(
						case 
							when GP_STATUS_DATE = DATE_SUB(event_date,31) and GP_STATUS = 'INACT' then 1 
							else 0 
						end
					) lostat30days,
					sum(
						case 
							when GP_STATUS_DATE >= DATE_SUB(event_date,91) and GP_STATUS = 'INACT' then 1 
							else 0 
						end
					) all90dayslost,
					sum(
						case 
							when GP_STATUS_DATE = DATE_SUB(event_date,91) and GP_STATUS = 'INACT' then 1 
							else 0 
						end
					) lostat90days,
					sum(
						case 
							when GP_STATUS_DATE >= DATE_SUB(event_date,31) and GP_STATUS = 'ACTIF' then 1 
							else 0 
						end
					) all30dayswinback,
					sum(
						case 
							when GP_STATUS_DATE = DATE_SUB(event_date,31) and GP_STATUS = 'ACTIF' then 1 
							else 0 
						end
					) winbackat30days,
					sum(
						case 
							when GP_STATUS_DATE >= DATE_SUB(event_date,91) and GP_STATUS = 'ACTIF' then 1 
							else 0 
						end
					) all90dayswinback,
					sum(
						case 
							when GP_STATUS_DATE = DATE_SUB(event_date,91) and GP_STATUS = 'ACTIF' then 1 
							else 0 
						end
					) winbackat90days,
					sum(
						case 
							when GP_STATUS = 'INACT' and GP_STATUS_DATE = DATE_SUB(event_date,1) then 1 
							else 0 
					end) churner
				from MON.SPARK_FT_ACCOUNT_ACTIVITY a
				where event_date = date_add('###SLICE_VALUE###', 1) 
				group by msisdn
			) A
			full outer join
			(
				select
					msisdn,
					(
						CASE
							WHEN 
								DATEDIFF(IC_CALL_4, '###SLICE_VALUE###')>=-90 
								or
								DATEDIFF(OG_CALL, '###SLICE_VALUE###')>=-90 
								THEN 1
							ELSE 0
						END
					) parc_art,
					(
						CASE
							WHEN 
								(
									DATEDIFF(IC_CALL_1, '###SLICE_VALUE###')>=-90 and
									DATEDIFF(IC_CALL_2, '###SLICE_VALUE###')>=-90 and
									DATEDIFF(IC_CALL_3, '###SLICE_VALUE###')>=-90 and
									DATEDIFF(IC_CALL_4, '###SLICE_VALUE###')>=-90
								) OR
								DATEDIFF(OG_CALL, '###SLICE_VALUE###')>=-90 
								THEN 1
							ELSE 0
						END
					) parc_group
				from	
				(
					SELECT
						MSISDN
						, MAX(OG_CALL) OG_CALL
						, MAX(IC_CALL_1) IC_CALL_1
						, MAX(IC_CALL_2) IC_CALL_2
						, MAX(IC_CALL_3) IC_CALL_3
						, MAX(IC_CALL_4) IC_CALL_4
					FROM MON.SPARK_FT_OG_IC_CALL_SNAPSHOT
					WHERE EVENT_DATE=DATE_ADD('###SLICE_VALUE###', 1) 
					GROUP BY MSISDN
				) T
			) B
			on A.msisdn = B.msisdn
			full outer join
			(
				SELECT
					MSISDN,
					1 parc_om,
					case
						when 
							account_status='Y' and
							REGISTERED_ON <= event_date then 1
						else 0
					end gross_add_om				
				FROM MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT
				WHERE EVENT_DATE = '###SLICE_VALUE###' AND UPPER(USER_TYPE) = 'SUBSCRIBER' 
			) C
			on A.msisdn = C.msisdn
			full outer join
			(
				select
				msisdn,
				1 parc_actif_om
				from
				(
					SELECT
						DISTINCT SENDER_MSISDN MSISDN
					FROM cdr.spark_IT_OMNY_TRANSACTIONS
					WHERE TRANSFER_DATETIME = "###SLICE_VALUE###"
						AND TRANSFER_STATUS='TS' AND SERVICE_TYPE IN ('CASHIN', 'CASHOUT', 'MERCHPAY', 'BILLPAY', 'P2P', 'P2PNONREG','ENT2REG','RC') and SENDER_CATEGORY_CODE='SUBS'
					UNION
					SELECT
						DISTINCT RECEIVER_MSISDN MSISDN
					FROM cdr.spark_IT_OMNY_TRANSACTIONS
					WHERE TRANSFER_DATETIME = "###SLICE_VALUE###"
						AND TRANSFER_STATUS='TS' AND SERVICE_TYPE IN ('CASHIN', 'CASHOUT', 'MERCHPAY', 'BILLPAY', 'P2P', 'P2PNONREG','ENT2REG','RC') AND RECEIVER_CATEGORY_CODE=='SUBS'
				) T
			) D
			on A.msisdn = D.msisdn
			full outer join
			(
				SELECT
					MSISDN,
					max
					(
						case
							when device_type like '%Smartphone%' then 1
							else 0
						end 
					) smartphone_user
				FROM
				(
					SELECT
						MSISDN
						, SUBSTR(IMEI, 1, 14) IMEI
					FROM MON.SPARK_FT_IMEI_ONLINE
					WHERE SDATE='###SLICE_VALUE###'
				) C0
				LEFT JOIN DIM.DT_NEW_HANDSET_REF C
				ON lpad(TRIM(SUBSTR(C0.IMEI, 1, 8)), 8, 0) = TRIM(C.TAC)
				group by msisdn
			) E
			on A.msisdn = E.msisdn
			full outer join
			(
				SELECT
					ACCESS_KEY MSISDN,
					case
						when ACTIVATION_DATE >= DATE_SUB(event_date, 29) then 'inf_30' 
						when ACTIVATION_DATE < DATE_SUB(event_date, 29) and ACTIVATION_DATE >= DATE_SUB(event_date, 89) then 'act_inf_90_sup_30'
						when ACTIVATION_DATE < DATE_SUB(event_date, 89) and ACTIVATION_DATE >= DATE_SUB(event_date, 179) then 'act_inf_180_sup_90'
						when ACTIVATION_DATE < DATE_SUB(event_date, 179) and ACTIVATION_DATE >= DATE_SUB(event_date, 364) then 'act_inf_365_sup_180' 
						else 'act_sup_365' 
					end anciennete,
					CASE
						WHEN 
							ACTIVATION_DATE <= date_sub(event_date, 1) AND
						(
								CASE
									WHEN NVL(OSP_STATUS,CURRENT_STATUS)='ACTIVE' THEN 'ACTIF'
									WHEN NVL(OSP_STATUS,CURRENT_STATUS)='a' THEN 'ACTIF'
									WHEN NVL(OSP_STATUS,CURRENT_STATUS)='d' THEN 'DEACT'
									WHEN NVL(OSP_STATUS,CURRENT_STATUS)='s' THEN 'INACT'
									WHEN NVL(OSP_STATUS,CURRENT_STATUS)='s' THEN 'INACT'
									WHEN NVL(OSP_STATUS,CURRENT_STATUS)='DEACTIVATED' THEN 'DEACT'
									WHEN NVL(OSP_STATUS,CURRENT_STATUS)='INACTIVE' THEN 'INACT'
									WHEN NVL(OSP_STATUS,CURRENT_STATUS)='VALID' THEN 'VALIDE'
									ELSE NVL(OSP_STATUS,CURRENT_STATUS)
								END
							)='ACTIF' THEN 1
						ELSE 0
					END gross_add
				FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
				WHERE EVENT_DATE = DATE_ADD('###SLICE_VALUE###',1) 
			) F
			on A.msisdn = F.msisdn
			full outer join
			(
				select
					charged_party_msisdn msisdn,
					case 
						when bytes/1024/2024 >= 1 then 1
						else 0
					end data_user
				from
				(
					select
						charged_party_msisdn,
						sum(nvl(bytes_sent, 0) + nvl(bytes_received, 0)) bytes
					from mon.spark_ft_cra_gprs
					where session_date = '###SLICE_VALUE###' and nvl(main_cost, 0)>=0
					group by charged_party_msisdn
				) T
			) G
			on A.msisdn = G.msisdn
			full outer join
			(
				select
					msisdn,
					case 
						when traffic_voice > 0 then 1
						else 0
					end voice_user,
					case
						when nb_sms > 0 then 1
						else 0
					end sms_user,
					nvl(revenu_VOICE, 0) revenu_paygo_voice,
					nvl(revenu_sms, 0) revenu_paygo_sms
				from
				(
					SELECT 
						CHARGED_PARTY MSISDN,
						SUM
						(
							CASE 
								WHEN
									UPPER(SERVICE_CODE) IN ('TEL','OC','OCFWD','OCRMG','TCRMG') 
									OR UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%'
									OR UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN CALL_PROCESS_TOTAL_DURATION
								ELSE 0 
							END
						) TRAFFIC_VOICE,
						SUM
						(
							CASE 
								WHEN
									UPPER(SERVICE_CODE) IN ('TEL','OC','OCFWD','OCRMG','TCRMG') 
									OR UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%'
									OR UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN MAIN_RATED_AMOUNT
								ELSE 0 
							END
						) revenu_voice,
						SUM
						(
							CASE 
								WHEN
									UPPER(SERVICE_CODE) IN ('SMS' ,'SMSMO','SMSRMG') THEN 1
								ELSE 0 
							END
						) NB_SMS,
						SUM
						(
							CASE 
								WHEN
									UPPER(SERVICE_CODE) IN ('SMS' ,'SMSMO','SMSRMG') THEN MAIN_RATED_AMOUNT
								ELSE 0 
							END
						) revenu_sms
					FROM MON.SPARK_FT_BILLED_TRANSACTION_PREPAID
					WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
						AND MAIN_RATED_AMOUNT >= 0
						AND PROMO_RATED_AMOUNT >= 0
					GROUP BY 
						CHARGED_PARTY
				) T
			) H
			on A.msisdn = H.msisdn
			full outer join
			(
				select
					msisdn,
					case
						when nvl(revenu, 0) > 0 then 1
						else 0
					end vas_user
				from
				(
					select
						msisdn,
						sum(nvl(billing,0)) revenu
					from
					(
						select
							msisdn,
							service,
							code,
							partner,
							billing,
							source
						from
						(
							select
								served_party_msisdn msisdn,
								sdp_gos_serv_name service,
								null code,
								null partner,
								TOTAL_COST billing,
								null bundle,
								'CRA_GPRS' source
							from
							(
								select
									served_party_msisdn,
									sdp_gos_serv_name,
									sum(TOTAL_COST) TOTAL_COST
								from
								(
									select
										served_party_msisdn,
										TOTAL_COST,
										sdp_gos_serv_name,
										SERVED_PARTY_OFFER
									from mon.spark_ft_cra_gprs
									where session_date = '###SLICE_VALUE###'
								) a00 left join DIM.DT_OFFER_PROFILES a01
								on a00.SERVED_PARTY_OFFER = a01.PROFILE_CODE
								where a01.OPERATOR_CODE In ('OCM') and sdp_gos_serv_name is not null
								group by served_party_msisdn, sdp_gos_serv_name
							) a0
							UNION all
							SELECT
								msisdn,
								service,
								A1.code,
								partner,
								sum(billing) billing,
								frequency bundle,
								'VOICE_SMS' source
							FROM
							(
								SELECT
									served_party MSISDN,
									other_party CODE,
									NVL(MAIN_RATED_AMOUNT, 0) + NVL(PROMO_RATED_AMOUNT, 0) BILLING
								FROM MON.SPARK_FT_VAS_REVENUE_DETAIL
								WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
							) A1
							LEFT JOIN tmp.dim_vas_services B1
							ON trim(A1.CODE) = trim(B1.code) and billing = price
							group by msisdn, A1.CODE, service, partner, frequency
							UNION all
							select
								substr(acc_nbr, -9, 9) msisdn,
								nvl(B3.usage_description, B2.usage_description) service,
								null code,
								null partner,
								sum(charge)/100 billing,
								null bundle,
								'IT_ZTE_ADJUSTMENT' source
							from
							(
								select *
								from CDR.SPARK_IT_ZTE_ADJUSTMENT
								where create_date = '###SLICE_VALUE###'
									and channel_id in (13, 15, 28, 37)
							) A2
							left join dim.DT_ZTE_USAGE_TYPE B2 on A2.CHANNEL_ID = B2.USAGE_CODE
							left join DIM.DT_USAGES B3 on B2.GLOBAL_USAGE_CODE = B3.USAGE_CODE
							group by substr(acc_nbr, -9, 9), nvl(B3.usage_description, B2.usage_description)
							union all
							select
								SERVED_PARTY_MSISDN msisdn,
								'SOS Credit Fees' service,
								null code,
								null partner,
								FEE billing,
								null bundle,
								'FT_OVERDRAFT' source
							FROM MON.SPARK_FT_OVERDRAFT
							WHERE TRANSACTION_DATE = '###SLICE_VALUE###' AND NVL(FEE_FLAG,'ND') ='YES'
						) a
					) T
					group by msisdn
				) R
			) I
			on A.msisdn = I.msisdn
			full outer join
			(
				select
					GET_NNP_MSISDN_9DIGITS(acc_nbr) msisdn,
					SUM(nvl(CHARGE/100, 0)) revenu_adjustment
				from CDR.SPARK_IT_ZTE_ADJUSTMENT A
				LEFT JOIN (SELECT USAGE_CODE, FLUX_SOURCE FROM DIM.DT_ZTE_USAGE_TYPE ) B ON B.USAGE_CODE = A.CHANNEL_ID
				WHERE CREATE_DATE = '###SLICE_VALUE###'  AND B.FLUX_SOURCE='ADJUSTMENT' AND CHANNEL_ID IN ('13','9','14','15','26','29','28','37') 
				AND CHARGE > 0 AND ACCT_RES_CODE='1'
				group by GET_NNP_MSISDN_9DIGITS(acc_nbr)
			) J
			on A.msisdn = J.msisdn
			full outer join
			(
				select
					access_key msisdn,
					sum(nvl(MAIN_CREDIT, 0)) revenu_credit_compte_desactive
				from MON.SPARK_FT_CONTRACT_SNAPSHOT
				WHERE EVENT_DATE = '###SLICE_VALUE###' AND DEACTIVATION_DATE = '###SLICE_VALUE###' AND MAIN_CREDIT > 0
				group by access_key
			) K
			on A.msisdn = K.msisdn
			full outer join
			(
				select
					GET_NNP_MSISDN_9DIGITS(sender_msisdn) msisdn,
					SUM(nvl(TRANSFER_FEES, 0)) revenu_p2p_voix
				from MON.SPARK_FT_CREDIT_TRANSFER
				WHERE REFILL_DATE = '###SLICE_VALUE###' AND TERMINATION_IND = '000'
				group by GET_NNP_MSISDN_9DIGITS(sender_msisdn)
			) L
			on A.msisdn = L.msisdn
			full outer join
			(
				select
					GET_NNP_MSISDN_9DIGITS(SENDER_MSISDN) msisdn,
					sum(nvl(amount_charged, 0)) revenu_p2p_data
				from MON.SPARK_FT_DATA_TRANSFER A
				where TRANSACTION_DATE = '###SLICE_VALUE###' AND amount_charged > 0
				group by GET_NNP_MSISDN_9DIGITS(SENDER_MSISDN)
			) M
			on A.msisdn = M.msisdn
			full outer join
			(
				select
					GET_NNP_MSISDN_9DIGITS(msisdn) msisdn,
					sum(nvl(AMOUNT, 0)) revenu_sos_credit_data
				from MON.SPARK_FT_EMERGENCY_DATA A
				where TRANSACTION_DATE = '###SLICE_VALUE###' AND NVL(TRANSACTION_TYPE,'ND') ='LOAN'
				group by GET_NNP_MSISDN_9DIGITS(msisdn)
			) N
			on A.msisdn = N.msisdn
			full outer join
			(
				select
					GET_NNP_MSISDN_9DIGITS(served_party_msisdn) msisdn,
					sum(nvl(FEE, 0)) revenu_sos_credit_voix
				from MON.SPARK_FT_OVERDRAFT 
				where TRANSACTION_DATE = '###SLICE_VALUE###' AND NVL(FEE_FLAG,'ND') ='YES'
				group by GET_NNP_MSISDN_9DIGITS(served_party_msisdn)
			) O
			on A.msisdn = O.msisdn
			full outer join
			(
				select
					GET_NNP_MSISDN_9DIGITS(served_party_msisdn) msisdn,
					sum(nvl(MAIN_AMOUNT, 0)) revenu_vas_data
				from MON.SPARK_FT_SUBS_RETAIL_ZEBRA
				where TRANSACTION_DATE = '###SLICE_VALUE###' AND MAIN_AMOUNT > 0
				group by GET_NNP_MSISDN_9DIGITS(served_party_msisdn)
			) P
			on A.msisdn = P.msisdn
			full outer join
			(
				select
					GET_NNP_MSISDN_9DIGITS(served_party_msisdn) msisdn,
					sum(nvl(RATED_AMOUNT*coef_voix, 0)) revenu_voice_bundle,
					sum(nvl(RATED_AMOUNT*coef_sms, 0)) revenu_sms_bundle,
					sum(nvl(RATED_AMOUNT*data, 0)) revenu_data_bundle,
					SUM(
						case 
							when 
								upper(trim(SUBSCRIPTION_SERVICE_DETAILS))!='RP DATA SHAPE_5120K' and 
								SUBSCRIPTION_SERVICE_DETAILS is not null and 
								SUBSCRIPTION_CHANNEL ='32' then nvl(RATED_AMOUNT*data, 0)
							else 0
						end
					) revenu_om_data
				FROM mon.SPARK_FT_SUBSCRIPTION  ud
				left join  (
					select 
						event,
						(nvl(VOIX_ONNET,0) + nvl(VOIX_OFFNET,0) + nvl(VOIX_INTER,0)+ nvl(VOIX_ROAMING,0)) coef_voix,
						(nvl(SMS_ONNET,0) +nvl(SMS_OFFNET,0)+nvl(SMS_INTER,0)+nvl(SMS_ROAMING,0)) coef_sms,
						(case when data_bundle != 1 then nvl(DATA_BUNDLE,0) else 0 end) data_combo,
						(case when  data_bundle = 1 then nvl(DATA_BUNDLE,0) else 0 end) data_pur,
						nvl(DATA_BUNDLE,0) data
					from dim.dt_services
				) events on upper(trim(ud.SUBSCRIPTION_SERVICE_DETAILS)) = upper(trim(events.EVENT))
				WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
				group by GET_NNP_MSISDN_9DIGITS(served_party_msisdn)
			) Q
			on A.msisdn = Q.msisdn
		) A
	) U
) T
left join 
(
	select
		a.msisdn,
		max(a.site_name) site_name_a,
		max(a.townname) townname_a,
		max(a.commercial_region) commercial_region_a,
		max(a.administrative_region) administrative_region_a,
		max(b.site_name) site_name_b,
		max(b.townname) townname_b,
		max(b.commercial_region) commercial_region_b,
		max(b.administrative_region) administrative_region_b
	from mon.spark_ft_client_last_site_day a
	left join (
		select * from mon.spark_ft_client_site_traffic_day where event_date='###SLICE_VALUE###'
	) b on a.msisdn = b.msisdn
	where a.event_date='###SLICE_VALUE###'
	group by a.msisdn
) site 
on GET_NNP_MSISDN_9DIGITS(T.msisdn) = GET_NNP_MSISDN_9DIGITS(site.msisdn)
left join
(
	SELECT 
		SITE_NAME, 
		TOWNNAME,
		region,
		commercial_region, 
		max(typedezone) typedezone 
	FROM DIM.SPARK_DT_GSM_CELL_CODE 
	group by site_name, townname, commercial_region, region
) R
on 
	TRIM(COALESCE(upper(site.site_name_b),upper(site.site_name_a), 'INCONNU')) = TRIM(upper(r.site_name), 'INCONNU') and
	TRIM(COALESCE(upper(site.townname_b),upper(site.townname_a), 'INCONNU')) = TRIM(upper(r.townname), 'INCONNU') and
	TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = TRIM(upper(r.region), 'INCONNU') and
	TRIM(COALESCE(upper(site.commercial_region_b),upper(site.commercial_region_a), 'INCONNU')) = TRIM(upper(r.commercial_region), 'INCONNU') 
group by T.msisdn
