insert into mon.spark_ft_acquisitions_report_month
select distinct
	iccid,
    A.numero msisdn_identified,
    numeroduvendeur msisdn_identificator,
    numeropiece cni,
    nomprenomclient noms_prenoms,
    typedecontrat contract_type,
    DATE_IDENTIFICATION identification_date,
    EST_SNAPPE est_snappe,
    Statut_back_office statut_back_office,
    Statut_back_office_date statut_back_office_date,
    activation_date,
    LAST_REFRESH_DATE,
    est_fake,
	first_day_refill FIRST_DATE_REFILL,
	FIRST_DATE_REFILL_AMOUNT,
	pps_first_dial_date,
	FIRST_MONTH_REFILL,
	SECOND_MONTH_REFILL,
	THIRD_MONTH_REFILL,
	FIRST_MONTH_DEPOT,
	SECOND_MONTH_DEPOT,
	THIRD_MONTH_DEPOT,
	FIRST_MONTH_CONSO,
	SECOND_MONTH_CONSO,
	THIRD_MONTH_CONSO,
	FIRST_MONTH_SOUSCRIPTIONDATA,
	SECOND_MONTH_SOUSCRIPTIONDATA,
	THIRD_MONTH_SOUSCRIPTIONDATA,
	FIRST_MONTH_TRANSACTION,
	SECOND_MONTH_TRANSACTION,
	THIRD_MONTH_TRANSACTION,
	INSCRIPTEUR_OM,
	DATE_INSCRIPTION_OM,
	case when M.msisdn is not null then 'OUI' else 'NON' end est_bypass,
	categorie_site,
	site_name,
	current_timestamp() insert_date,
	'###SLICE_VALUE###' event_month
from
(
	select distinct
        telephone numero,
        numeroduvendeur,
        numeropiece,
        concat(nomduclient, ' ', prenomduclient) nomprenomclient,
        typedecontrat,
        substr(EMISLE,1,10) DATE_IDENTIFICATION,
        (
            CASE 
                WHEN upper(ETAT)='VALID' and upper(ETATDEXPORTGLOBAL)='SUCCESS' then 'OUI'
                WHEN upper(ETAT)='INVALID' then 'NON'  else 'UNKNOWN' 
            END
        ) EST_SNAPPE,
        substr(MAJLE,1,10) Statut_back_office_date,
        etat Statut_back_office,
        last_update_date LAST_REFRESH_DATE
    from CDR.SPARK_IT_NOMAD_CLIENT_DIRECTORY_30J 
    where original_file_date = last_day(concat('###SLICE_VALUE###', '-01'))
) A
left join
(
	SELECT distinct
        accnbr numero, 
        iccid
    FROM CDR.SPARK_IT_CONT WHERE original_file_date between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))
) B
on GET_NNP_MSISDN_9DIGITS(A.numero)=GET_NNP_MSISDN_9DIGITS(B.numero)
left join
(
	select
		msisdn,
		inscripteur INSCRIPTEUR_OM,
		date_inscript DATE_INSCRIPTION_OM
	from
	(
		select 
			msisdn,
			inscripteur,
			date_inscript,
			row_number() over(partition by msisdn order by date_inscript asc) rang
		from MON.SPARK_FT_OMNY_SDT 
		where date_inscript between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) 
	) T
	where rang = 1
) D
on GET_NNP_MSISDN_9DIGITS(A.numero)=GET_NNP_MSISDN_9DIGITS(D.msisdn)
left join
(
	select
		served_party_msisdn msisdn,
		transaction_date pps_first_dial_date
	from
	(
		select
			served_party_msisdn,
			transaction_date,
			row_number() over(partition by served_party_msisdn order by transaction_date asc) rang
		from mon.spark_ft_subscription 
		where transaction_date between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))
	) T
	where rang = 1
) F
on GET_NNP_MSISDN_9DIGITS(A.numero)=GET_NNP_MSISDN_9DIGITS(F.msisdn)
left join
(
    select
        msisdn,
        est_fake_activation est_fake
    from
    (
        select
            msisdn,
            est_fake_activation,
            row_number() over(partition by msisdn order by event_date desc) rang
        from MON.SPARK_FT_FAKE_ACTIVATION_STATUS 
        where event_date between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) and 
			est_fake_activation is not null
    ) T
    where rang = 1
) G
on GET_NNP_MSISDN_9DIGITS(A.numero)=GET_NNP_MSISDN_9DIGITS(G.msisdn)
left join
(
	select distinct
		msisdn
	from MON.SPARK_FT_CONSO_SIMBOX 
	where detect_month='###SLICE_VALUE###'
) M
on GET_NNP_MSISDN_9DIGITS(A.numero) = GET_NNP_MSISDN_9DIGITS(M.msisdn)
LEFT JOIN 
(
	select
		msisdn,
		R.site_name site_name,
		C.categorie_site categorie_site
	from
	(
		select
			msisdn,
			nvl(site_name_B, site_name_A) site_name
		from
		(
			SELECT
				A.MSISDN msisdn,
				MAX(A.site_name) site_name_A,
				MAX(B.site_name) site_name_B
			FROM 
			(
				select * from MON.SPARK_FT_CLIENT_LAST_SITE_DAY where event_date = last_day(concat('###SLICE_VALUE###', '-01'))
			) A
			LEFT JOIN (
				SELECT * FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE=last_day(concat('###SLICE_VALUE###', '-01'))
			) B 
			ON GET_NNP_MSISDN_9DIGITS(A.MSISDN) = GET_NNP_MSISDN_9DIGITS(B.MSISDN)
			GROUP BY A.MSISDN
		) P
	) R
	left join
	(
		select * from dim.spark_dt_gsm_cell_code
	) C
	on upper(trim(R.site_name)) = upper(trim(C.site_name))
) SITE 
ON GET_NNP_MSISDN_9DIGITS(A.numero) = GET_NNP_MSISDN_9DIGITS(SITE.MSISDN)
left join 
(
	select *
	from MON.SPARK_FT_ACQ_SNAP_MONTH
	where event_month = '###SLICE_VALUE###'
) Z 
on GET_NNP_MSISDN_9DIGITS(A.numero) = GET_NNP_MSISDN_9DIGITS(Z.msisdn)