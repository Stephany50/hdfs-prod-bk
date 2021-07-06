
CREATE EXTERNAL TABLE CDR.tt_dim_ref_sites (
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    codesite varchar(200),
    codesite_new varchar(200),
    nomdusite varchar(200),
    nomdusite_new varchar(200),
    longitude varchar(200),
    latitude varchar(200),
    categorie_de_site varchar(200),
    cellule varchar(200),
    lac varchar(200),
    ci varchar(200),
    id_bts varchar(200),
    id_bts_new varchar(200),
    date_met_cell varchar(200),
    date_arret_cell varchar(200),
    techno_cell varchar(200),
    frequence_cell varchar(200),
    programme varchar(200),
    type_de_site varchar(200),
    tower_height varchar(200),
    date_mad_site varchar(200),
    date_met_site varchar(200),
    date_arret_site varchar(200),
    techno_site varchar(200),
    frequences_site varchar(200),
    nom_bailleur varchar(200),
    expiration__bail varchar(200),
    typologie_site varchar(200),
    typologie_site_2 varchar(200),
    code_partenaire varchar(200),
    nbre_tenants varchar(200),
    priorite varchar(200),
    config varchar(200),
    topology varchar(200),
    typologie_trans varchar(200),
    aggregation varchar(200),
    canal_de_transmission varchar(200),
    localite varchar(200),
    quartier varchar(200),
    region_terr varchar(200),
    region_bus varchar(200),
    departement varchar(200),
    arrondissement varchar(200),
    type_de_zone varchar(200),
    region_comerciale varchar(200),
    zone_pmo varchar(200),
    secteur_om varchar(200),
    erp_geo_code varchar(200)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/REF_SITES'
TBLPROPERTIES ('serialization.null.format'='')
;