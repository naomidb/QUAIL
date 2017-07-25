/*
name create_subjects

Creates a table called subjects with two columns
Takes:
  - unique_field_name <= the key of the unique field
  - secondary_unique_field_name <= optional key of the secondary unique field

Both columns are TEXT type and they have names equal to what the key is in redcap
*/
CREATE TABLE IF NOT EXISTS subjects(
{{unique_field_name}} TEXT PRIMARY KEY
{%- if secondary_unique_field_name -%}
,
{{- secondary_unique_field_name}} TEXT
{% endif -%}
);

/*
name create_batches

Creates a table called batches that tracks the downloaded data
Takes:
  - year <= four numbers for the year the batch was downloaded
  - month <= two numbers for the month the batch was downloaded
  - day <= two numbers for the day the batch was downloaded
  - batch_path <= string that indicates the batches path
  - batch_name <= the folder that contains the batch data, last directory on path
  - redcap_host <= host that the data came from
  - token <= token that was used to pull the data
*/
CREATE TABLE IF NOT EXISTS batches(
batch_name TEXT PRIMARY KEY,
date_pulled TEXT,
redcap_host TEXT,
token TEXT,
batch_path TEXT
);


/*
name create_instrument

Creates a table for an instrument
Takes the following dictionary
primary_key and primary_keys are mutually exclusive

primary_key
primary_key_type
primary_keys: [
  - field:
    type:
]
fields: [
  - (field, type)
]
foreign_keys: [
  - field:
    other_table:
    other_key:
    fk_sub_clause:
]

*/
CREATE TABLE IF NOT EXISTS {{name}}(
-- single primary key
{% if primary_key -%}
{{primary_key}} {{primary_key_type}} PRIMARY KEY
{%- if fields -%},{%- endif %}
{%- endif -%}

-- unique together column definitions
{% if primary_keys -%}
{%- for def in primary_keys -%}
{{def.field}} {{def.type}} NOT NULL{% if not loop.last %},{% endif %}
{%- endfor -%}
{%- if fields -%},{%- endif -%}
{%- endif %}
-- fields
{% for field_name, field_type in fields -%}
  {{-field_name}} {{field_type-}}
{%- if not loop.last %},{% endif -%}
{%- endfor -%}
{%- if primary_keys %},

-- multiple primary keys, for when the unique field form has many events
PRIMARY KEY (
{%- for def in primary_keys -%}
{{def.field}}{% if not loop.last %},{% endif %}
{%- endfor -%}
){%- endif -%}

{%- if foreign_keys -%},
{%- for fk in foreign_keys %}
FOREIGN KEY ({{fk.field}})
REFERENCES {{fk.other_table}}({{fk.other_key}}) {{fk.fk_sub_clause}}{% if not loop.last %},{% endif %}
{%- endfor -%}
{%- endif -%}
);


/*
name create_checkbox

Used in creating and populating checkbox lookup tables
takes a dictionary with
{
name
form_name
options [(
        export_name,
        value,
        display
  )]
}

where export_name is the name of the field value when exporting a form from
redcap. Note that the threee fields are in a tuple
*/
CREATE TABLE IF NOT EXISTS checkbox_{{name}}(
export_name TEXT PRIMARY KEY,
val TEXT,
display TEXT
);

INSERT INTO checkbox_{{name}}(export_name, val, display)
VALUES {% for export, val, display in options -%}
('{{export}}', '{{val}}', '{{display}}')
{%- if not loop.last %},{% else %};{% endif -%}
{%- endfor %}

/*
name create_lookup

Used in creating and populating dropdown or radio lookup tables
{
name
form_name
type : 'radio' or 'dropdown'
options [{
        val: ''
        display: ''
  }]
}
*/
CREATE TABLE IF NOT EXISTS {{type}}_{{name}}(
val TEXT PRIMARY KEY,
display TEXT,
FOREIGN KEY(val) REFERENCES {{form_name}}({{name}})
);

INSERT INTO {{type}}_{{name}}(val, display)
VALUES {% for val, display in options -%}
('{{val}}', '{{display}}')
{%- if not loop.last %},{% else %};{% endif -%}
{%- endfor %}

