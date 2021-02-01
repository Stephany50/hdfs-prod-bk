insert into TMP.tt_flotte02_generique
select *
from TMP.tt_flotte02_RE
where (upper(nom_structure) like '%FORIS%TELEC%' or upper(nom_structure) like '%SAVANA%ISLAM%')
and trim(type_personne) = 'M2M'