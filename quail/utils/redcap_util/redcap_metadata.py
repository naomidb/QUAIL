from quail.utils.file_manipulation_mixin import FileManipulationMixin

class TableMaker(FileManipulationMixin):
    def __init__(self, primary_key, batch_path, metadata_filename, tablename, serialization_format='json'):
        path = self.join([batch_path, 'redcap_metadata', metadata_filename])
        self.data = self.read(path, serialization_format)

        self.table_data = self.make_schema(tablename, primary_key)
        self.insert_data = self.make_inserts(tablename, self.data)

    def make_schema(self, tablename, primary_key):
        table = {}
        table['tablename'] = tablename
        table['primary_key'] = primary_key
        table['primary_key_type'] = 'TEXT'
        if not type(self.data) == type([]):
            self.data = [self.data]
        for key, val in self.data[0].items():
            coldefs = table.setdefault('coldefs', [])
            if not key == primary_key:
                coldefs.append({
                    'field': key,
                    'type': 'TEXT'
                })
        return table

    def make_inserts(self, tablename, redcap_data):
        inserts = {
            'tablename': tablename
        }
        if not type(self.data) == type([]):
            self.data = [redcap_data]
        cols = inserts.setdefault('cols', [])
        for key in redcap_data[0].keys():
            cols.append(key)
        vals = inserts.setdefault('vals', [])
        for item in redcap_data:
            escaped = [self.escape(str(item[col])) for col in cols]
            vals.append(escaped)
        return inserts

    def escape(self, string):
        string = string.replace("\'", "\'\'")
        if string == 'None':
            return None
        else:
            return string


class Arm(TableMaker):
    def __init__(self, batch_root):
        super().__init__('arm_num', batch_root, 'arms.json', 'arm')

class Event(TableMaker):
    def __init__(self, batch_root):
        super().__init__('unique_event_name', batch_root, 'events.json', 'event')

class Instrument(TableMaker):
    def __init__(self, batch_root):
        super().__init__('instrument_name', batch_root, 'instruments.json', 'instrument')

class Field(TableMaker):
    def __init__(self, batch_root):
        super().__init__('field_name', batch_root, 'metadata.json', 'field')

class Project(TableMaker):
    def __init__(self, batch_root):
        super().__init__('project_title', batch_root, 'project_info.json', 'project')

class JunctionTables():
        tables = [
            {
                'tablename': 'project_arm',
                'primary_key': 'id',
                'primary_key_type': 'INTEGER',
                'coldefs': [
                    {
                        'field': 'project_title',
                        'type': 'TEXT'
                    },
                    {
                        'field': 'arm_num',
                        'type': 'TEXT'
                    }
                ],
                'foreign_keys': [
                    {
                        'field': 'project_title',
                        'other_table': 'project',
                        'other_key': 'project_title',
                        'fk_subclause': '',
                    },
                    {
                        'field': 'arm_num',
                        'other_table': 'arm',
                        'other_key': 'arm_num',
                        'fk_subclause': '',
                    },
                ]
            },
            {
                'tablename': 'arm_event',
                'primary_key': 'id',
                'primary_key_type': 'INTEGER',
                'coldefs': [
                    {
                        'field': 'arm_num',
                        'type': 'TEXT'
                    },
                    {
                        'field': 'unique_event_name',
                        'type': 'TEXT'
                    }
                ],
                'foreign_keys': [
                    {
                        'field': 'arm_num',
                        'other_table': 'arm',
                        'other_key': 'arm_num',
                        'fk_subclause': '',
                    },
                    {
                        'field': 'unique_event_name',
                        'other_table': 'event',
                        'other_key': 'unique_event_name',
                        'fk_subclause': '',
                    },
                ]
            },
            {
                'tablename': 'event_instrument',
                'primary_key': 'id',
                'primary_key_type': 'INTEGER',
                'coldefs': [
                    {
                        'field': 'unique_event_name',
                        'type': 'TEXT'
                    },
                    {
                        'field': 'instrument_name',
                        'type': 'TEXT'
                    }
                ],
                'foreign_keys': [
                    {
                        'field': 'unique_event_name',
                        'other_table': 'event',
                        'other_key': 'unique_event_name',
                        'fk_subclause': '',
                    },
                    {
                        'field': 'instrument_name',
                        'other_table': 'instrument',
                        'other_key': 'instrument_name',
                        'fk_subclause': '',
                    },
                ]
            },
            {
                'tablename': 'instrument_field',
                'primary_key': 'id',
                'primary_key_type': 'INTEGER',
                'coldefs': [
                    {
                        'field': 'instrument_name',
                        'type': 'TEXT'
                    },
                    {
                        'field': 'field_name',
                        'type': 'TEXT'
                    }
                ],
                'foreign_keys': [
                    {
                        'field': 'instrument_name',
                        'other_table': 'instrument',
                        'other_key': 'instrument_name',
                        'fk_subclause': '',
                    },
                    {
                        'field': 'field_name',
                        'other_table': 'field',
                        'other_key': 'field_name',
                        'fk_subclause': '',
                    },
                ]
            },
        ]
