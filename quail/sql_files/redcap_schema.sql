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
"{{primary_key}}" {{primary_key_type}} PRIMARY KEY
{%- if fields -%},{%- endif %}
{%- endif -%}
-- unique together column definitions
{% if primary_keys -%}
{%- for def in primary_keys -%}
"{{def.field}}" {{def.type}} NOT NULL{% if not loop.last %},{% endif %}
{%- endfor -%}
{%- if fields -%},{%- endif -%}
{%- endif %}
-- fields
{% for field_name, field_type in fields -%}
  "{{-field_name}}" {{field_type-}}
{%- if not loop.last %},{% endif -%}
{%- endfor -%}
{%- if primary_keys %},
-- multiple primary keys, for when the unique field form has many events
PRIMARY KEY (
{%- for def in primary_keys -%}
  "{{def.field}}"{% if not loop.last %},{% endif %}
{%- endfor -%}
){%- endif -%}
{%- if foreign_keys -%},
{%- for fk in foreign_keys %}
FOREIGN KEY ("{{fk.field}}")
REFERENCES {{fk.other_table}}("{{fk.other_key}}") {{fk.fk_sub_clause}}{% if not loop.last %},{% endif %}
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
FOREIGN KEY(val) REFERENCES {{form_name}}("{{name}}")
);

INSERT INTO {{type}}_{{name}}(val, display)
VALUES {% for val, display in options -%}
('{{val}}', '{{display}}')
{%- if not loop.last %},{% else %};{% endif -%}
{%- endfor %}

