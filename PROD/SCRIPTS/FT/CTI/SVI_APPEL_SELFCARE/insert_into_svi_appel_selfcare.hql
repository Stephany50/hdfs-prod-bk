insert into CTI.SVI_APPEL_SELFCARE
SELECT DISTINCT 
    DATE_DEBUT_OMS as JOUR,
    DATE_FORMAT(date_debut_oms_nq, 'HH:mm:ss') HEURE,
    case when DATE_FORMAT(date_debut_oms_nq, 'mm')<60
        then DATE_FORMAT(date_debut_oms_nq, 'HH')||':00'
        else DATE_FORMAT(date_debut_oms_nq, 'HH')||':30' end TRANCHE_HEURE,
    s.id_appel,
    s.MSISDN,
    s.service AS numero_appele,
    s.SEGMENT_CLIENT AS SEGMENT_CLIENT,
    s.TYPE_HANGUP AS TYPE_HANGUP,
    s.DATE_DEBUT_OMS AS DATE_DEBUT_OMS,
	CURRENT_TIMESTAMP() INSERT_DATE,
    DATE_DEBUT_OMS as EVENT_DATE
FROM 
(
    select * 
    from CTI.SVI_APPEL 
    WHERE DATE_DEBUT_OMS = '###SLICE_VALUE###'
) s
left join
(
    select 
        ID_APPEL
    from 
    (
        select 
            distinct ID_APPEL 
        from CTI.SVI_NAVIGATION A
        left join 
        (
            select distinct menu from CTI.MENU_SELFCARE
        ) B 
        on upper(trim(A.element)) = upper(trim(B.menu))
        where B.menu is not null

        minus 

        select 
            distinct id_appel
        from CTI.SVI_APP_TRANSFERE
        where JOUR = '###SLICE_VALUE###'
    ) T
) U
on s.id_appel = U.ID_APPEL
where U.ID_APPEL is not null