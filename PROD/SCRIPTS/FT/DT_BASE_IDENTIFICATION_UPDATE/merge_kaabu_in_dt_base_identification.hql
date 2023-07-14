

INSERT INTO DIM.SPARK_DT_BASE_IDENTIFICATION_KAABU

SELECT
    a.MSISDN MSISDN,
    a.NOM NOM,
    a.PRENOM PRENOM,
    a.NEE_LE NEE_LE,
    a.NEE_A NEE_A,
    a.PROFESSION PROFESSION,
    a.QUARTIER_RESIDENCE QUARTIER_RESIDENCE,
    a.VILLE_VILLAGE VILLE_VILLAGE,
    a.CNI CNI,
    a.DATE_IDENTIFICATION DATE_IDENTIFICATION,
    a.TYPE_DOCUMENT TYPE_DOCUMENT,
    a.FICHIER_CHARGEMENT FICHIER_CHARGEMENT,
    a.DATE_INSERTION DATE_INSERTION,
    a.EST_SNAPPE EST_SNAPPE,
    a.IDENTIFICATEUR IDENTIFICATEUR,
    a.DATE_MISE_A_JOUR DATE_MISE_A_JOUR,
    a.DATE_TABLE_MIS_A_JOUR DATE_TABLE_MIS_A_JOUR,
    a.GENRE GENRE,
    a.CIVILITE CIVILITE,
    a.TYPE_PIECE_IDENTIFICATION TYPE_PIECE_IDENTIFICATION,
    a.PROFESSION_IDENTIFICATEUR PROFESSION_IDENTIFICATEUR,
    a.MOTIF_REJET MOTIF_REJET,
	a.TYPEDECONTRAT TYPEDECONTRAT

FROM
(
	select * from
	(
	select *, ROW_NUMBER() OVER (PARTITION BY MSISDN ORDER BY DATE_IDENTIFICATION, DATE_TABLE_MIS_A_JOUR  DESC) AS RG
	FROM
	(
		select MSISDN, NOM, PRENOM, NEE_LE, NEE_A, PROFESSION, QUARTIER_RESIDENCE, VILLE_VILLAGE, CNI, DATE_IDENTIFICATION, TYPE_DOCUMENT, FICHIER_CHARGEMENT, DATE_INSERTION, EST_SNAPPE, IDENTIFICATEUR, DATE_MISE_A_JOUR, DATE_TABLE_MIS_A_JOUR, GENRE, CIVILITE, TYPE_PIECE_IDENTIFICATION, PROFESSION_IDENTIFICATEUR, MOTIF_REJET, TYPEDECONTRAT  from TT.SPARK_DT_BASE_IDENTIFICATION_2
		UNION ALL
		select
			TELEPHONE MSISDN,
			NOMDUCLIENT NOM,
			PRENOMDUCLIENT PRENOM,
			to_date(DATEDENAISSANCE) NEE_LE,
			LIEUDENAISSANCE NEE_A,
			null PROFESSION,
			QUARTIER QUARTIER_RESIDENCE,
			VILLE VILLE_VILLAGE,
			NUMEROPIECE CNI,
			substr(EMISLE,1,10)  DATE_IDENTIFICATION,
			null TYPE_DOCUMENT,
			'NOMAD' fichier_chargement,
			substr(MAJLE,1,10) DATE_INSERTION,
			(CASE WHEN upper(ETAT)='VALID' and upper(ETATDEXPORTGLOBAL)='SUCCESS' then 'OUI'
			  WHEN upper(ETAT)='INVALID' then 'NON'  else 'UNKNOWN' END )EST_SNAPPE,
			LOGINVENDEUR IDENTIFICATEUR,
			substr(MAJLE,1,10)  date_mise_a_jour,
			substr(CURRENT_TIMESTAMP,1,19)  date_table_mis_a_jour,
			(Case when TITRE ='Madame(Mme)' then 'F' else 'M' END) genre,
			TITRE  civilite,
			piece TYPE_PIECE_IDENTIFICATION,
			null PROFESSION_IDENTIFICATEUR,
			null MOTIF_REJET,
			TYPEDECONTRAT -- add contract type
		from cdr.spark_it_kaabu_client_directory
		where
			original_file_date = DATE_ADD('###SLICE_VALUE###', 1)
			and upper(typeoperation) like '%TELCO%' -- in ('Nouvel Abonnement', 'Flex Sim')
			and LOGINVENDEUR != ''
			and (LOGINVENDEUR != null or length(LOGINVENDEUR)>2 )
	)T
	)D where RG =1
) A