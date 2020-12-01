SELECT
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(nom,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS nom,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(prenom,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS prenom,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(date_naissance,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS date_naissance,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(numero_cni,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS numero_cni,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(msisdn,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS msisdn,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(est_actif_om_90j,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS est_actif_om_90j,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(est_actif_om_30j,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS est_actif_om_30j,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(activation_date_tel,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS activation_date_tel,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(created_date_om,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS created_date_om,
cast(CURRENT_TIMESTAMP() as string) as INSERT_DATE,
'###SLICE_VALUE###' EVENT_DATE
FROM
