
/*
name create_table

Takes a tables argument that contains objects of the form below
NOTE make sure that the tables are in the right order for foreign
key relations
tables:
    - tablename: 'my_table'
      primary_key: 'sql_id'
      primary_key_tyep: 'TEXT'
      coldefs:
          - field: 'my_field1'
            type: 'INTEGER'
          - field: 'my_field2'
            type: 'TEXT'
      foreign_keys:
          - field: 'my_field1'
            other_table: 'my_other_table'
            other_key: 'other_table_primary_key'
            fk_subclause: 'ON DELETE SET NULL'
*/
{% for table in tables -%}
CREATE TABLE IF NOT EXISTS {{table.tablename}}(
{{table.primary_key}} {{table.primary_key_type}} PRIMARY KEY
{%- if table.coldefs -%},{%- endif -%}
{%- for def in table.coldefs -%}
{{def.field}} {{def.type}}{% if not loop.last %},{% endif %}
{%- endfor -%}
{%- if table.foreign_keys -%},{%- endif -%}
{%- for fk in table.foreign_keys -%}
FOREIGN KEY ({{fk.field}})
REFERENCES {{fk.other_table}}({{fk.other_key}}) {{fk.fk_sub_clause}}{% if not loop.last %},{% endif %}
{%- endfor -%}
);
{% endfor %}

/*
name insert

Takes a inserts dictionary that has a tablename and two lists
one is of the column names into which we will be inserting and the other
is the values that we will be using

inserts:
  tablename: 'my_table'
  cols: ['id', 'favorite_color', 'favorite_movie']
  vals: [
  [1, 'green', 'James and the Giant Peach'],
  [2, 'red', 'Spiderman'],
]
*/
INSERT INTO {{ tablename }}(
{%- for col in cols -%}
{{- col -}}{% if not loop.last %},{% endif %}
{%- endfor -%})
VALUES
{% for val in vals -%}
({%- for item in val -%}
'{{- item -}}'{% if not loop.last %},{% endif %}
{%- endfor -%}){% if not loop.last %},{% endif %}
{% endfor -%};


/*
name table_info

Gets information regarding the table passed in.

Returns
cid|name|type|notnull|dflt_value|pk
*/
PRAGMA table_info({{table}});
