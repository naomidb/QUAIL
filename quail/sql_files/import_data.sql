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
name get_descriptive_fields

gets the descriptive fields for a given form. These cannot be uploaded
so we have to remove them before writing them
*/
select field_name from meta.fields where field_type="descriptive" and form_name="{{form}}";

/*
name get_import_data

gets the data to write to csv
*/
SELECT "{{ cols| join('", "')}}" from "{{form}}";
