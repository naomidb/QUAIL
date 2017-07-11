from pyyesql import Database

with open('test.db', 'w'):
    pass

d = Database(**{
    'database_path': 'test.db',
    'query_path': '../sql_files/hcv/build_full_hcv_schema.sql'
})

d.build_schema().executescript()
