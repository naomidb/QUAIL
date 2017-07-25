from itertools import chain, repeat

from quail.utils.file_manipulation_mixin import FileManipulationMixin as file_util

class Instrumentor(file_util):
    def __init__(self, metadata_root):
        self.metadata = self.load_metadata(metadata_root)
        self.instruments = list(set([item['form_name'] for item in self.metadata]))
        self.unique_field = self.metadata[0]

        self.checkboxes = [field for field in self.metadata if field['field_type'] == 'checkbox']
        self.dropdowns = [field for field in self.metadata if field['field_type'] == 'dropdown']
        self.radios = [field for field in self.metadata if field['field_type'] == 'radio']

    def load_metadata(self, metadata_root):
        for root, dirs, files in self.walk(metadata_root):
            for filename in files:
                if filename == 'metadata.json':
                    return self.read(self.join([root, filename]), 'json')

    def fields_for_instrument(self, instrument_name):
        fields = [field for field in self.metadata
                  if field['form_name'] == instrument_name
                  and field['field_name'] != self.unique_field['field_name']]
        normal = [field for field in fields if field not in self.checkboxes]

        checkboxes = [field for field in self.checkboxes if field['form_name'] == instrument_name]
        parsed_select_choices = chain(*[self.parse_select_choices(field) for field in checkboxes])

        field_names = list(chain(
            [field['field_name'] for field in normal],
            [export_name for export_name, value, display in parsed_select_choices]
        ))
        texttype = repeat('TEXT')
        field_names.insert(1, '{}_complete'.format(instrument_name))
        # the instrument which contains the unique field will already define these
        # and use them as primary keys
        if instrument_name != self.unique_field['form_name']:
            field_names.insert(0, self.unique_field['field_name'])
            field_names.insert(1, 'redcap_event_name')

        return zip(field_names, texttype)

    def parse_select_choices(self, field):
        choices = field['select_choices_or_calculations'].split('|')
        field_name = field['field_name']
        parsed = []
        for choice in choices:
            value = choice.split(',')[0].strip().replace("\'","\'\'")
            display = ','.join(choice.split(',')[1:]).strip().replace("\'","\'\'")
            export_name = field_name.strip() + '___' + value.strip().replace("\'","\'\'")
            parsed.append(( export_name, value, display ))
        return parsed

    def get_subject_fk(self):
        return {
            'field': self.unique_field['field_name'],
            'other_table': self.unique_field['form_name'],
            'other_key': self.unique_field['field_name'],
            'fk_sub_clause': ''
        }

    def get_instrument_table(self, instrument_name):
        primary_key = None
        primary_key_type = None
        primary_keys = []
        if instrument_name == self.unique_field['form_name']:
            primary_keys = [
                {'field': self.unique_field['field_name'], 'type': 'TEXT'},
                {'field': 'redcap_event_name', 'type': 'TEXT'}
            ]
        else:
            primary_key = 'sql_id'
            primary_key_type = 'INTEGER'

        return {
            'name': instrument_name,
            'primary_key': primary_key,
            'primary_key_type': primary_key_type,
            'primary_keys': primary_keys,
            'fields': self.fields_for_instrument(instrument_name),
            'foreign_keys': [
                # fix this for tuesday
                # (self.get_subject_fk() if instrument_name != self.unique_field['form_name'])
            ]
        }

    def get_all_instruments(self):
        return [self.get_instrument_table(name) for name in self.instruments]

    def get_all_checkboxes(self):
        return [
            {
                'name': field['field_name'],
                'form_name': field['form_name'],
                'options': self.parse_select_choices(field)
            }
            for field in self.checkboxes
        ]

    def get_all_dropdowns(self):
        return [
            {
                'name': field['field_name'],
                'form_name': field['form_name'],
                'type': 'dropdown',
                'options': [(val, disp) for ex, val, disp in self.parse_select_choices(field)]
            }
            for field in self.dropdowns
        ]

    def get_all_radios(self):
        return [
            {
                'name': field['field_name'],
                'form_name': field['form_name'],
                'type': 'radio',
                'options': [(val, disp) for ex, val, disp in self.parse_select_choices(field)]
            }
            for field in self.radios
        ]
