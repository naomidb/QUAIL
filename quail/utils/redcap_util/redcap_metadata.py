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
        table['primary_key'] = primary_key or 'sqlid'
        table['primary_key_type'] = 'TEXT' if primary_key else 'INTEGER'
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

class InstrumentEvent(TableMaker):
    def __init__(self, batch_root):
        super().__init__(None, batch_root, 'instrument_event.json', 'instrument_event')

class Field(TableMaker):
    def __init__(self, batch_root):
        super().__init__('field_name', batch_root, 'metadata.json', 'field')

class Project(TableMaker):
    def __init__(self, batch_root):
        super().__init__('project_title', batch_root, 'project_info.json', 'project')
