INSERT INTO cti.svi_navigation
SELECT
    id_appel,
    element,
    date_element date_element_nq,
    type_element,
    complement,
    date_debut_oms,
    original_file_name,
    original_file_size,
    original_file_line_count,
    To_date(From_unixtime(Unix_timestamp(Substring (original_file_name, -23, 8),'yyyyMMdd'))) original_file_date,
    CURRENT_TIMESTAMP() insert_date,
    to_date(date_element)   date_element
FROM cti.tt_svi_navigation c
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM   cti.svi_navigation) T ON T.file_name = C.original_file_name
WHERE T.file_name IS NULL