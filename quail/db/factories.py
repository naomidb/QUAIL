from quail.utils.file_manipulation_mixin import FileManipulationMixin as file_util
from quail.db.pyyesql import Database

def dynamic_schema(path):
    db_root = file_util.path_split(__file__)[0]
    project_root = file_util.path_split(db_root)[0]
    return Database(**{
        'database_path': path,
        'query_path': file_util.join([project_root, 'sql_files', 'dynamic_schema.sql'])
    })

def redcap_schema(path):
    db_root = file_util.path_split(__file__)[0]
    project_root = file_util.path_split(db_root)[0]
    return Database(**{
        'database_path': path,
        'query_path': file_util.join([project_root, 'sql_files', 'redcap_schema.sql'])
    })
