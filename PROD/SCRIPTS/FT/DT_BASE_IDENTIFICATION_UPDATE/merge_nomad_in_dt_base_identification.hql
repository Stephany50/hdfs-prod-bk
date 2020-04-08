

INSERT INTO DIM.SPARK_DT_BASE_IDENTIFICATION

SELECT
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.MSISDN, b.TELEPHONE), b.TELEPHONE) MSISDN,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.NOM, b.NOMDUCLIENT), b.NOMDUCLIENT) NOM,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.PRENOM, b.PRENOMDUCLIENT), b.PRENOMDUCLIENT) PRENOM,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.NEE_LE, b.DATEDENAISSANCE), b.DATEDENAISSANCE) NEE_LE,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.NEE_A, b.LIEUDENAISSANCE), b.LIEUDENAISSANCE) NEE_A,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.PROFESSION, b.PROFESSION), b.PROFESSION) PROFESSION,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.QUARTIER_RESIDENCE, b.QUARTIER), b.QUARTIER) QUARTIER_RESIDENCE,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.VILLE_VILLAGE, b.VILLE), b.VILLE) VILLE_VILLAGE,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.CNI, b.NUMEROPIECE), b.NUMEROPIECE) CNI,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.DATE_IDENTIFICATION, b.DATE_IDENTIFICATION), b.DATE_IDENTIFICATION) DATE_IDENTIFICATION,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.TYPE_DOCUMENT, b.TYPE_DOCUMENT), b.TYPE_DOCUMENT) TYPE_DOCUMENT,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.FICHIER_CHARGEMENT, b.fichier_chargement), b.fichier_chargement) FICHIER_CHARGEMENT,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.DATE_INSERTION, b.DATE_INSERTION), b.DATE_INSERTION) DATE_INSERTION,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.EST_SNAPPE, b.EST_SNAPPE), b.EST_SNAPPE) EST_SNAPPE,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.IDENTIFICATEUR, b.IDENTIFICATEUR), b.IDENTIFICATEUR) IDENTIFICATEUR,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.DATE_MISE_A_JOUR, b.date_mise_a_jour), b.date_mise_a_jour) DATE_MISE_A_JOUR,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.DATE_TABLE_MIS_A_JOUR, b.date_table_mis_a_jour), b.date_table_mis_a_jour) DATE_TABLE_MIS_A_JOUR,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.GENRE, b.genre), b.genre) GENRE,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.CIVILITE, b.CIVILITE), b.CIVILITE) CIVILITE,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.TYPE_PIECE_IDENTIFICATION, b.TYPE_PIECE_IDENTIFICATION), b.TYPE_PIECE_IDENTIFICATION) TYPE_PIECE_IDENTIFICATION,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.PROFESSION_IDENTIFICATEUR, b.PROFESSION_IDENTIFICATEUR), b.PROFESSION_IDENTIFICATEUR) PROFESSION_IDENTIFICATEUR,
    IF(a.msisdn IS NULL OR b.telephone IS NULL, nvl(a.MOTIF_REJET, b.MOTIF_REJET), b.MOTIF_REJET) MOTIF_REJET

FROM
(
	select * from
	(
	select *, ROW_NUMBER() OVER (PARTITION BY MSISDN ORDER BY DATE_IDENTIFICATION, DATE_TABLE_MIS_A_JOUR  DESC) AS RG
	FROM
	(
		select MSISDN, NOM, PRENOM, NEE_LE, NEE_A, PROFESSION, QUARTIER_RESIDENCE, VILLE_VILLAGE, CNI, DATE_IDENTIFICATION, TYPE_DOCUMENT, FICHIER_CHARGEMENT, DATE_INSERTION, EST_SNAPPE, IDENTIFICATEUR, DATE_MISE_A_JOUR, DATE_TABLE_MIS_A_JOUR, GENRE, CIVILITE, TYPE_PIECE_IDENTIFICATION, PROFESSION_IDENTIFICATEUR, MOTIF_REJET  from TT.SPARK_DT_BASE_IDENTIFICATION_2
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
			null MOTIF_REJET
		from CDR.SPARK_IT_NOMAD_CLIENT_DIRECTORY_UPDATE
		where
			original_file_date = DATE_SUB('###SLICE_VALUE###',7)
			and TYPEDECONTRAT='Nouvel Abonnement'
			and  ETATDEXPORTGLOBAL ='SUCCESS'
			and LOGINVENDEUR not in ('testfo','NKOLBONG','testve')
			and LOGINVENDEUR != ''
			and (LOGINVENDEUR != null or length(LOGINVENDEUR)>2 )
	)T
	)D where RG =1
) A
FULL OUTER JOIN
(
	select
		TELEPHONE,NOMDUCLIENT,PRENOMDUCLIENT,DATEDENAISSANCE,LIEUDENAISSANCE,PROFESSION,QUARTIER,VILLE,NUMEROPIECE,
		DATE_IDENTIFICATION,TYPE_DOCUMENT,fichier_chargement,DATE_INSERTION,EST_SNAPPE,ETAT, ETATDEXPORTGLOBAL,IDENTIFICATEUR,
		date_mise_a_jour,date_table_mis_a_jour,genre,civilite,TYPE_PIECE_IDENTIFICATION,PROFESSION_IDENTIFICATEUR,MOTIF_REJET
	from
	(
	select
		TELEPHONE ,
		NOMDUCLIENT,
		PRENOMDUCLIENT,
		DATEDENAISSANCE,
		LIEUDENAISSANCE,
		null PROFESSION,
		QUARTIER,
		VILLE,
		NUMEROPIECE,
		substr(EMISLE,1,10)  DATE_IDENTIFICATION,
		null TYPE_DOCUMENT,
		'NOMAD' fichier_chargement,
		substr(MAJLE,1,10) DATE_INSERTION,
		(CASE WHEN upper(ETAT)='VALID' and upper(ETATDEXPORTGLOBAL)='SUCCESS' then 'OUI'
		  WHEN upper(ETAT)='INVALID' then 'NON'  else 'UNKNOWN' END )EST_SNAPPE,
		ETAT ,
		ETATDEXPORTGLOBAL,
		LOGINVENDEUR IDENTIFICATEUR,
		substr(MAJLE,1,10)  date_mise_a_jour,
		CURRENT_TIMESTAMP  date_table_mis_a_jour,
		(Case when TITRE ='Madame(Mme)' then 'F' else 'M' END) genre,
		TITRE  civilite,
		piece TYPE_PIECE_IDENTIFICATION,
		null PROFESSION_IDENTIFICATEUR,
		null MOTIF_REJET ,
		ROW_NUMBER() OVER (PARTITION BY TELEPHONE ORDER BY substr(MAJLE,1,19) DESC) AS RG
	  from CDR.SPARK_IT_NOMAD_CLIENT_DIRECTORY
	  where
		original_file_date = DATE_ADD('###SLICE_VALUE###',1)
		and TYPEDECONTRAT='Nouvel Abonnement'
		and  ETATDEXPORTGLOBAL ='SUCCESS'
		and LOGINVENDEUR not in ('testfo','NKOLBONG','testve')
		and LOGINVENDEUR != ''
		and (LOGINVENDEUR != null or length(LOGINVENDEUR)>2 )
	)T   where RG =1
) B
ON a.MSISDN = b.TELEPHONE