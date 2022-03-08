CREATE OR REPLACE FUNCTION get_labels(_address STRING)

RETURNS STRING

RETURN
    SELECT
    concat_ws(', ',array_agg(distinct blockchain),array_agg(DISTINCT name)) as label_name
    FROM default.all_labels_table labels
    WHERE (labels.address = get_labels._address);