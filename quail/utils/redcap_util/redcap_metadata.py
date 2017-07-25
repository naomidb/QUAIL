from copy import copy
from quail.utils.file_manipulation_mixin import FileManipulationMixin

class TableMaker(FileManipulationMixin):
    def __init__(self,
                 primary_key,
                 batch_path,
                 metadata_filename,
                 tablename,
                 insert_batch_size=-1,
                 serialization_format='json'):
        path = self.join([batch_path, 'redcap_metadata', metadata_filename])
        self.data = self.read(path, serialization_format)

        self.table_data = self.make_schema(tablename, primary_key)
        self.insert_data = self.make_inserts(tablename, self.data, insert_batch_size)

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

    def make_inserts(self, tablename, redcap_data, insert_batch_size):
        """
        If batch size is -1 there will only ever be one batch. This is
        useful for when you dont have or want a limit on how many items
        you can insert at one time
        """
        batches = []
        cols = [key for key in redcap_data[0].keys()]
        batch = {
            'tablename': tablename,
            'cols': cols,
            'vals': []
        }
        for item in redcap_data:
            if (not len(batches)) or (len(batches[-1]['vals']) == insert_batch_size):
                batches.append({
                    'tablename': tablename,
                    'cols': cols,
                    'vals': []
                })
            escaped = [self.escape(str(item[col])) for col in cols]
            batches[-1]['vals'].append(escaped)
        return batches



    def escape(self, string):
        string = string.replace("\'", "\'\'")
        if string == 'None':
            return None
        else:
            return string


class Arm(TableMaker):
    def __init__(self, batch_root, insert_batch_size):
        super().__init__('arm_num', batch_root, 'arms.json', 'arms', insert_batch_size)

class Event(TableMaker):
    def __init__(self, batch_root, insert_batch_size):
        super().__init__('unique_event_name', batch_root, 'events.json', 'events', insert_batch_size)

class Instrument(TableMaker):
    def __init__(self, batch_root, insert_batch_size):
        super().__init__('instrument_name', batch_root, 'instruments.json', 'instruments', insert_batch_size)

class InstrumentEvent(TableMaker):
    def __init__(self, batch_root, insert_batch_size):
        super().__init__(None, batch_root, 'instrument_event.json', 'instrument_event', insert_batch_size)

class InstrumentEvent(TableMaker):
    def __init__(self, batch_root, insert_batch_size):
        super().__init__(None, batch_root, 'instrument_event.json', 'instrument_event', insert_batch_size)

class Field(TableMaker):
    def __init__(self, batch_root, insert_batch_size):
        super().__init__('field_name', batch_root, 'metadata.json', 'fields', insert_batch_size)

class Project(TableMaker):
    def __init__(self, batch_root, insert_batch_size):
        super().__init__('project_title', batch_root, 'project_info.json', 'project', insert_batch_size)
