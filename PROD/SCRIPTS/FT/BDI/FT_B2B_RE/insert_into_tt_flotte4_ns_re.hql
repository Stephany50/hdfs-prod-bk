insert into TMP.tt_flotte4_ns_RE
select msisdn, nom_structure, compte_client_structure, DATE_SOUSCRIPTION
from (
select a.msisdn, b.nom_structure, a.compte_client_structure, a.DATE_SOUSCRIPTION,
row_number() over(partition by a.msisdn order by a.DATE_SOUSCRIPTION) as rang
from
(select x1.*
from (select *
from TMP.tt_flotte3_RE
where upper(trim(compte_client_structure)) not like '1.%'
and (trim(nom_structure) = '' or nom_structure is null or
trim(translate(trim(nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
length(trim(nom_structure)) < 2)) x1
left join (select distinct msisdn as msisdn_s from TMP.tt_flotte02_generique) x2
on trim(x1.msisdn) = trim(x2.msisdn_s)
where x2.msisdn_s is null) a
join (select x1.* from
(select *
from TMP.tt_flotte3_RE
where upper(trim(compte_client_structure)) not like '1.%'
and not(trim(nom_structure) = '' or nom_structure is null or
trim(translate(trim(nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
length(trim(nom_structure)) < 2)) x1
left join (select distinct msisdn as msisdn_s from TMP.tt_flotte02_generique) x2
on trim(x1.msisdn) = trim(x2.msisdn_s)
where x2.msisdn_s is null) b
on substr(trim(a.compte_client_structure),1,6) = substr(trim(b.compte_client_structure),1,6)
) c where rang = 1
union all
select msisdn, nom_structure, compte_client_structure, DATE_SOUSCRIPTION
from (
select a.msisdn, b.nom_structure, a.compte_client_structure, a.DATE_SOUSCRIPTION,
row_number() over(partition by a.msisdn order by a.DATE_SOUSCRIPTION) as rang
from
(select *
from TMP.tt_flotte3_RE
where upper(trim(compte_client_structure))  like '1.%'
and (trim(nom_structure) = '' or nom_structure is null or
trim(translate(trim(nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
length(trim(nom_structure)) < 2)
) a
join (select *
from TMP.tt_flotte3_RE
where upper(trim(compte_client_structure))  like '1.%'
and not(trim(nom_structure) = '' or nom_structure is null or
trim(translate(trim(nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
length(trim(nom_structure)) < 2)
) b
on trim(a.compte_client_structure) = trim(b.compte_client_structure)
) c where rang = 1