SELECT
'' Numero_transaction,
Ticket_Reference Transaction_Code,
Ticket_number Numero_Ticket_Vente,
'' Numero_Code_barre,
Article_reference Numero_Article,
TRANSACTION_DATE `Date`,
Quantity_Sold Quantite,
'' Prix,
Amount_of_the_sale Montant_Net,
'' Montant_Remise,
'' Montant_TVA,
Amount_of_the_sale Cout_total,
Organization_Code Numero_TPV_Caisse,
Staff_CUID ID_Employe,
'' Equipe_de_vente,
'' Etage,
'' Heure,
Description_of_the_article Remise_infocode,
'' Remise_Totale,
'' Remise_ligne,
'' Remises_Periodiques,
'' Remise_client,
'' Intro_Article_par_Clavier,
'' Prix_en_Code_Barre,
'' Remise_sur_Ligne,
'' Numero_Article_Scanne,
'' Article_a_peser
FROM CDR.SPARK_IT_SELL_CUST_DAILY
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
AND Description_of_the_article like '%ORANGE MONEY CASH IN%'

