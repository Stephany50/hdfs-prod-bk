insert into mon.spark_ft_crm_reporting
SELECT
	msisdn,
	case
		when upper(trim(fileAttente)) like '%DD-%' or upper(trim(fileAttente)) like '%AGENCE%' then 'AGENCES'
		when upper(trim(fileAttente)) like '%FRANCHISE%' or upper(trim(fileAttente)) like '%CONTAINER%' or upper(trim(fileAttente)) like '%PDV%' then 'FRANCHISES & CONTAINERS'
		when upper(trim(fileAttente)) like '%WEB%' then 'DIGITAL ASSISTE'
		when upper(trim(fileAttente)) like '%INTELCIA%' or upper(trim(fileAttente)) like '%LMT%' then 'CALL CENTER'
		else NULL
	end queue_type,
	fileAttente queue_name,
	categorie,
	TypeArticle,
	article,
	motif,
	Agent,
	cuid_agent,
	count(*) total,
	concat(year(date_interaction), '.S', case when length(WEEKOFYEAR(date_interaction)) = 1 then concat('0', WEEKOFYEAR(date_interaction)) else WEEKOFYEAR(date_interaction) end) semaine,
	replace(substr(date_interaction, 1, 7), '-', '.') mois,     
	case 
		when DATE_FORMAT(date_interaction, 'mm') < 60 then DATE_FORMAT(date_interaction, 'HH')||':00'
		else DATE_FORMAT(date_interaction, 'HH')||':01' 
	end heure,  
	segment,
	sous_segment,
	region,
	ville,
	site_name,
	cellname,
	CURRENT_TIMESTAMP() INSERT_DATE,
    '###SLICE_VALUE###' EVENT_DATE
from
(
	select
		A.msisdn msisdn,
		FileAttente,
		categorie,   
		TypeArticle,         
		article,          
		motif,   
		Agent, 
		cuid_agent,  
		date_interaction,
		segment,
		sous_segment,   
		nvl(ADMINISTRATIVE_REGION_B, ADMINISTRATIVE_REGION_A) region,   
		nvl(townname_B, townname_A) ville,
		nvl(site_b, site_a) site_name,
		cellname
	FROM 
	(
		SELECT 
			trim(onc_numeroappelant) msisdn,
			FileAttente,
			categorie,        
			typarticle TypeArticle,         
			article,          
			motif,   
			Agent,      
			cuid_agent,    
			date_interaction  
		from cdr.spark_it_crm_reporting 
		where original_file_date='###SLICE_VALUE###' 
			and onc_numeroappelant is not null and 
			length(GET_NNP_MSISDN_9DIGITS(trim(onc_numeroappelant)))=9
	) A
	left join
	(
		select
			T.msisdn msisdn,
			segment,
			case
				when segment = 'B2B' then sous_segment_b2b
				when segment = 'B2C' then sous_segment_b2c
				else null
			end sous_segment
		from
		(
			select
				MSISDN,
				upper(C.SEGMENTATION) segment
			from
			(
				SELECT
					ACCESS_KEY MSISDN,
					COMMERCIAL_OFFER
				FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
				WHERE EVENT_DATE = DATE_ADD('###SLICE_VALUE###',1) AND OSP_STATUS IN ('ACTIVE', 'INACTIVE')
			) B1
			LEFT JOIN DIM.DT_OFFER_PROFILES C ON UPPER(B1.COMMERCIAL_OFFER) = UPPER(C.PROFILE_CODE)
			WHERE UPPER(C.SEGMENTATION) IN ('B2B', 'B2C') 
		) T
		left join
		(
			select
				msisdn,
				case 
					when arpu <= 0 then 'zero'
					when arpu > 0 and arpu <= 1000 then 'low'
					when arpu >= 1000 and arpu < 5000 then 'medium'
					else 'high'
				end sous_segment_b2c,
				case 
					when arpu < 100000 then 'tpe'
					when arpu >= 100000 and arpu < 1000000 then 'pme'
					else 'gc'
				end sous_segment_b2b
			from
			(
				select 
					msisdn,
					sum(nvl(arpu, 0)) arpu
				from MON.SPARK_FT_CBM_ARPU_MOU
				where EVENT_DATE between concat(substr(add_months('###SLICE_VALUE###', -1), 1, 7), '-01') and last_day(concat(substr(add_months('###SLICE_VALUE###', -1), 1, 7), '-01'))
				group by msisdn
			) K
		) U 
		on T.msisdn = U.msisdn
	) B
	on A.msisdn=B.MSISDN
	LEFT JOIN 
	(
		select
			MSISDN,
			ADMINISTRATIVE_REGION_A,
			ADMINISTRATIVE_REGION_B,
			townname_A,
			townname_B,
			site_a,
			site_b,
			cellname
		from
		(
			SELECT
				A.MSISDN,
				MAX(A.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_A,
				MAX(B.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_B,
				MAX(A.townname) townname_A,
				MAX(B.townname) townname_B,
				max(a.site_name) site_a,
				max(b.site_name) site_b
			FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY A
			LEFT JOIN (
				SELECT * FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE='###SLICE_VALUE###'
			) B 
			ON A.MSISDN = B.MSISDN
			WHERE A.EVENT_DATE='###SLICE_VALUE###'
			GROUP BY A.MSISDN
		) T
		left join
		(
			SELECT
				trim(upper(site_name)) SITE_NAME,
				max(cellname) cellname
			FROM DIM.SPARK_DT_GSM_CELL_CODE 
			group by trim(upper(site_name))
		) CEL
		on upper(trim(nvl(site_b, site_a))) = upper(trim(SITE_NAME))
	) SITE 
	ON SITE.MSISDN = A.msisdn
) T
where upper(trim(categorie)) != 'NA' and 
	upper(trim(TypeArticle)) != 'NA' and 
	upper(trim(article)) != 'NA' and 
	upper(trim(motif)) != 'NA' and 
	segment is not null
group by
	msisdn,
	FileAttente,
	categorie,
	TypeArticle,
	article,
	motif,
	Agent,
	cuid_agent,
	date_interaction,
	segment,
	sous_segment,
	region,
	ville,
	site_name,
	cellname