insert into TMP.tt_flotte4_ns
select msisdn, nom_structure, compte_client_structure, DATE_SOUSCRIPTION
from (
select a.msisdn, b.nom_structure, a.compte_client_structure, a.DATE_SOUSCRIPTION,
row_number() over(partition by a.msisdn order by a.DATE_SOUSCRIPTION) as rang
from
(select x1.*
from (select *
from TMP.tt_flotte3
where upper(trim(compte_client_structure)) not like '1.%'
and (trim(nom_structure) = '' or nom_structure is null or
(trim(translate(trim(nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(nom_structure)) < 2)) x1) a
join (select x1.* from
(select *
from TMP.tt_flotte3
where upper(trim(compte_client_structure)) not like '1.%'
and not(trim(nom_structure) = '' or nom_structure is null or
(trim(translate(trim(nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(nom_structure)) < 2)) x1) b
on substr(trim(a.compte_client_structure),1,6) = substr(trim(b.compte_client_structure),1,6)
) c where rang = 1
union all
select msisdn, nom_structure, compte_client_structure, DATE_SOUSCRIPTION
from (
select a.msisdn, b.nom_structure, a.compte_client_structure, a.DATE_SOUSCRIPTION,
row_number() over(partition by a.msisdn order by a.DATE_SOUSCRIPTION) as rang
from
(select *
from TMP.tt_flotte3
where upper(trim(compte_client_structure))  like '1.%'
and (trim(nom_structure) = '' or nom_structure is null or
(trim(translate(trim(nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(nom_structure)) < 2)
) a
join (select *
from TMP.tt_flotte3
where upper(trim(compte_client_structure))  like '1.%'
and not(trim(nom_structure) = '' or nom_structure is null or
(trim(translate(trim(nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(nom_structure),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(nom_structure)) < 2)
) b
on trim(a.compte_client_structure) = trim(b.compte_client_structure)
) c where rang = 1