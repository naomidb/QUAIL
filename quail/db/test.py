from db.pyyesql import Database

with open('test.db', 'w'):
    pass

d = Database(**{
    'database_path': 'test.db',
    'query_path': 'sql_files/redcap_schema.sql'
})

d.create_subjects(unique_field_name='dm_subjid', secondary_unique_field_name='dm_usubjid').execute()
d.create_batches().execute()
d.create_instrument(**{'instrument': {
    'name': 'test',
    'fields': [
        ('name', 'TEXT'),
        ('num', 'INTEGER'),
    ]
}}).execute()
