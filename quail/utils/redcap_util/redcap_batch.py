import json
import datetime

from quail.utils.file_manipulation_mixin import FileManipulationMixin as file_util
from cappy import API

class Batcher(file_util):
    """
    This class is responsible for pulling metadata and data from redcap as well
    as writing the files to the batch root under the quail install directory
    """

    def __init__(self, batch_root, name, token, url):
        self.batch_root = batch_root
        self.api = API(token, url, 'v6.16.0.json')
        self.event_key = 'redcap_event_name'

    def pull_metadata(self, metadata_type=None):
        calls = [
            ( 'project_info', self.api.export_project_info ),
            ( 'arms', self.api.export_arms ),
            ( 'events', self.api.export_events ),
            ( 'instruments', self.api.export_instruments ),
            ( 'instrument_event', self.api.export_instrument_event_mapping ),
            ( 'metadata', self.api.export_metadata ),
            ( 'records', self.api.export_records )
        ]
        if metadata_type:
            calls = [( m_type, call ) for m_type, call in calls if m_type == metadata_type]
        for m_type, call in calls:
            data = json.loads(str(call().content, 'utf-8'))
            today = str(datetime.date.today())
            self.date = today
            file_path = self.join([self.batch_root, today, 'redcap_metadata', m_type + '.json'])
            self.write(file_path, data, 'json')

    def pull_data(self):
        """
        The only sneaking thing in here is that the field that determines the primary
        key of a subject in redcap is given by the very first metadata item
        """
        newest_metadata = self.get_most_recent_date_path(self.batch_root)
        metadata_path = self.join([newest_metadata, 'redcap_metadata'])
        metadata = self.read(self.join([metadata_path, 'metadata.json']), 'json')
        instrument_event = self.read(self.join([metadata_path, 'instrument_event.json']), 'json')
        today = str(datetime.date.today())
        self.metadata_date = self.path_split(newest_metadata)[1]
        self.date = today

        self.unique_field = metadata[0]['field_name']
        self.instruments = list(set([item['form_name'] for item in metadata]))
        self.event_instrument_mapping = {}
        for item in instrument_event:
            form = self.event_instrument_mapping.setdefault(item['form'], set())
            form.add(item['unique_event_name'])

        for instrument in self.instruments:
            print('Downloading Instrument {}'.format(instrument))
            event_list = self.event_instrument_mapping.get(instrument)
            if not event_list:
                continue
            res = self.api.export_records(fields=[self.unique_field, self.event_key],
                                          events=list(event_list),
                                          forms=[instrument])
            data = json.loads(str(res.content, 'utf-8'))
            data_path = self.join([self.batch_root, today, 'redcap_data_files', instrument + '.json'])
            self.write(data_path, data, 'json')

            print('Wrote Instrument {} to path {}'.format(instrument, data_path))
