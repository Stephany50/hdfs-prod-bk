INSERT INTO mon.ft_client_site_traffic_day
            (
             msisdn,
             site_name,
             duree_sortant,
             nbre_tel_sortant,
             duree_entrant,
             nbre_tel_entrant,
             nbre_sms_sortant,
             nbre_sms_entrant,
             refresh_date,
             served_party_location,
             townname,
             administrative_region,
             commercial_region,
             operator_code,
             event_date)
SELECT
       a.msisdn,
       b.site_name                 SITE_NAME,
       Sum (duree_sortant)         DUREE_SORTANT,
       Sum (nbre_tel_sortant)      NBRE_TEL_SORTANT,
       Sum (duree_entrant)         DUREE_ENTRANT,
       Sum (nbre_tel_entrant)      NBRE_TEL_ENTRANT,
       Sum (nbre_sms_sortant)      NBRE_SMS_SORTANT,
       Sum (nbre_sms_entrant)      NBRE_SMS_ENTRANT,
       current_timestamp           REFRESH_DATE,
       Max (served_party_location) SERVED_PARTY_LOCATION,
       b.townname,
       b.administrative_region,
       b.commercial_region,
       operator_code,
       event_date
FROM   mon.tt_client_cell_trafic_day a
       LEFT JOIN (SELECT a.msisdn,
                         b.*
                  FROM   (SELECT DISTINCT b.msisdn                  msisdn,
                                          First_value(b.location_ci)
                                            OVER (
                                              partition BY b.msisdn
                                              ORDER BY b.nbre DESC) LOCATION_CI
                          FROM   (SELECT b.msisdn,
                                         b.location_ci,
                                         Sum (Nvl (duree_sortant, 0) + Nvl (
                                              duree_entrant, 0)
                                              + Nvl (nbre_sms_sortant, 0)
                                              + Nvl (nbre_sms_entrant, 0)) nbre
                                  FROM   mon.tt_client_cell_trafic_day b
                                  GROUP  BY b.msisdn,
                                            b.location_ci) b) a
                         RIGHT JOIN(SELECT ci,
                                           site_name,
                                           townname,
                                           administrative_region,
                                           commercial_region
                                    FROM   vw_sdt_ci_info_new) b
                                 ON a.location_ci = b.ci) b
              ON a.msisdn = b.msisdn
GROUP  BY event_date,
          a.msisdn,
          b.site_name,
          b.townname,
          b.administrative_region,
          b.commercial_region,
          operator_code;