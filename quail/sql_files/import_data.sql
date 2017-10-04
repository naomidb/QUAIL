/*
name get_forms

gets form names
*/
SELECT DISTINCT instrument_name FROM meta.instruments;

/*
name get_table_info

writes the data to a csv
*/
PRAGMA table_info({{form}});

/*
name get_import_data

gets the data to write to csv
*/
SELECT {{ cols| join(', ')}} from {{form}};
