insert into  TMP.tt_flotte4_ADRES_STRUCT
select msisdn, ADRESSE_STRUCTURE, compte_client_structure, DATE_SOUSCRIPTION
from (
select a.msisdn, b.ADRESSE_STRUCTURE, a.compte_client_structure, a.DATE_SOUSCRIPTION,
row_number() over(partition by a.msisdn order by a.DATE_SOUSCRIPTION) as rang
from
(select x1.*
from (select *
from TMP.tt_flotte3
where upper(trim(compte_client_structure)) not like '1.%'
and (trim(ADRESSE_STRUCTURE) = '' or ADRESSE_STRUCTURE is NULL or
(trim(translate(trim(ADRESSE_STRUCTURE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(ADRESSE_STRUCTURE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(ADRESSE_STRUCTURE)) < 2)) x1
left join (select distinct msisdn as msisdn_s from TMP.tt_flotte02_generique1) x2
on trim(x1.msisdn) = trim(x2.msisdn_s)
where x2.msisdn_s is null) a
join (select x1.* from
(select *
from TMP.tt_flotte3
where upper(trim(compte_client_structure)) not like '1.%'
and not(trim(ADRESSE_STRUCTURE) = '' or ADRESSE_STRUCTURE is NULL or
(trim(translate(trim(ADRESSE_STRUCTURE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(ADRESSE_STRUCTURE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(ADRESSE_STRUCTURE)) < 2)) x1
left join (select distinct msisdn as msisdn_s from TMP.tt_flotte02_generique1) x2
on trim(x1.msisdn) = trim(x2.msisdn_s)
where x2.msisdn_s is null) b
on substr(trim(a.compte_client_structure),1,6) = substr(trim(b.compte_client_structure),1,6)
) c where rang = 1
union all
select msisdn, ADRESSE_STRUCTURE, compte_client_structure, DATE_SOUSCRIPTION
from (
select a.msisdn, b.ADRESSE_STRUCTURE, a.compte_client_structure, a.DATE_SOUSCRIPTION,
row_number() over(partition by a.msisdn order by a.DATE_SOUSCRIPTION) as rang
from
(select *
from TMP.tt_flotte3
where upper(trim(compte_client_structure))  like '1.%'
and (trim(ADRESSE_STRUCTURE) = '' or ADRESSE_STRUCTURE is NULL or
(trim(translate(trim(ADRESSE_STRUCTURE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(ADRESSE_STRUCTURE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(ADRESSE_STRUCTURE)) < 2)) a
join (select *
from TMP.tt_flotte3
where upper(trim(compte_client_structure))  like '1.%'
and not(trim(ADRESSE_STRUCTURE) = '' or ADRESSE_STRUCTURE is NULL or
(trim(translate(trim(ADRESSE_STRUCTURE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(ADRESSE_STRUCTURE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(ADRESSE_STRUCTURE)) < 2)) b
on trim(a.compte_client_structure) = trim(b.compte_client_structure)
) c where rang = 1
