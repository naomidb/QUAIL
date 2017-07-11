import datetime
from copy import copy

from quail.utils.redcap_util import redcap_batch
from quail.utils.redcap_util import redcap_metadata
from quail.utils.redcap_util import redcap_sqlize
from quail.db.factories import dynamic_schema, redcap_schema
from quail.utils.file_manipulation_mixin import FileManipulationMixin as file_util

def generate(quail_conf_path, name, token, url, init=False):
    """
    Generates a redcap project from which to pull data
    """
    print('Generating new redcap project')

    quail_data = file_util.read(quail_conf_path, 'yaml')
    file_util.write(quail_conf_path + '.bak', quail_data, 'yaml')
    quail_root = quail_data.get('quail_root')

    project_path = file_util.join([quail_root, 'sources', name])
    project_conf_path = file_util.join([project_path, 'redcap.conf.yaml'])
    project_batch_path = file_util.join([quail_root, 'batches', name])

    project_data = {
        'name': name,
        'token': token,
        'url': url,
        'batch_root': project_batch_path,
        'notes': {
            'source_type': 'Redcap',
            'free_text': ''
        }
    }
    quail_data['sources'][name] = project_data

    file_util.mkdir(project_path)
    file_util.mkdir(project_batch_path)
    file_util.write(project_conf_path, project_data, 'yaml')
    file_util.write(quail_conf_path, quail_data, 'yaml')
    print('Generated new redcap project {}'.format(name))
    if init:
        get_data(quail_conf_path, name, True)

def get_meta(quail_conf, project_name):
    """
    Gets the metadata for a particular redcap
    """
    print('Pulling metadata for {}'.format(project_name))
    quail_data = file_util.read(quail_conf, 'yaml')
    file_util.write(quail_conf + '.bak', quail_data, 'yaml')
    proj_data = copy(quail_data['sources'][project_name])
    del proj_data['notes']
    project = redcap_batch.Batcher(**proj_data)
    project.pull_metadata()
    print('Done pulling metadata for {}'.format(project_name))

def get_data(quail_conf, project_name, pull_metadata=True):
    """
    By default gets the metadata and data from an existing generated
    redcap project in the quail sources

    This function also adds the unique_field metadata to the quail.conf.yaml
    under the sources key.
    """
    if pull_metadata:
        get_meta(quail_conf, project_name)
    print('Pulling data for {}'.format(project_name))
    start = datetime.datetime.now()
    quail_data = file_util.read(quail_conf, 'yaml')
    proj_data = copy(quail_data['sources'][project_name])
    del proj_data['notes']
    project = redcap_batch.Batcher(**proj_data)
    project.pull_data()
    quail_data['sources'][project_name]['notes']['unique_field'] = project.unique_field
    end = datetime.datetime.now()
    batch = {
        'project_name': project_name,
        'date': project.date,
        'metadata_date': project.metadata_date,
        'path': file_util.join([project.batch_root, project.date]),
        'start': str(start),
        'end': str(end),
    }
    project_batches = quail_data['batches'].setdefault(project_name, {})
    project_batches.setdefault(project.date, batch)
    file_util.write(file_util.join([batch['path'], 'batch_info.json']), batch, 'json')
    file_util.write(quail_conf, quail_data, 'yaml')
    print('Done pulling data for {}'.format(project_name))

def gen_meta(quail_conf, project_name):
    """
    Generates the metadata.db in the most recent batch folder of the project.
    This database contains information about how the redcap is set up
    """
    quail_data = file_util.read(quail_conf, 'yaml')
    project_batches = list(quail_data['batches'][project_name].items())
    most_recent_batch_path = sorted(project_batches, key=lambda i: i[0])[-1][1]['path']
    database_path = file_util.join([most_recent_batch_path, 'metadata.db'])
    file_util.write(database_path)
    db = dynamic_schema(database_path)

    arm = redcap_metadata.Arm(most_recent_batch_path)
    event = redcap_metadata.Event(most_recent_batch_path)
    instrument = redcap_metadata.Instrument(most_recent_batch_path)
    field = redcap_metadata.Field(most_recent_batch_path)
    project = redcap_metadata.Project(most_recent_batch_path)
    junctions = redcap_metadata.JunctionTables()

    db.create_table(**{
        'tables': [
            arm.table_data,
            event.table_data,
            instrument.table_data,
            field.table_data,
            project.table_data
        ]
    }).executescript()

    db.create_table(**{'tables': junctions.tables }).executescript()

    res = db.insert(**arm.insert_data).execute()

    db.insert(**event.insert_data).execute()
    db.commit()
    db.insert(**instrument.insert_data).execute()
    db.commit()
    db.insert(**field.insert_data).execute()
    db.commit()
    db.insert(**project.insert_data).execute()
    db.commit()

def gen_data(quail_conf, project_name):
    """
    Generates the data.db in the most recent batch folder of the project.
    This database contains all the data from the redcap data pull.

    Here the subjects are in a table along with tables of the forms.
    The database also has lookup tables for the dropdowns, checkboxes and
    radio buttons

    See docs/quail.org for a description of its schema
    """
    quail_data = file_util.read(quail_conf, 'yaml')
    project_batches = list(quail_data['batches'][project_name].items())
    most_recent_batch_path = sorted(project_batches, key=lambda i: i[0])[-1][1]['path']
    redcap_data_path = file_util.join([most_recent_batch_path, 'redcap_data_files'])
    database_path = file_util.join([most_recent_batch_path, 'data.db'])
    file_util.write(database_path)
    db = redcap_schema(database_path)

    print('Loading metadata...')
    instrumentor = redcap_sqlize.Instrumentor(most_recent_batch_path)
    subject_form = instrumentor.unique_field['form_name']

    print('Writing instruments tables...')
    for instrument in instrumentor.get_all_instruments():
        db.create_instrument(**instrument).execute()

    print('Writing checkboxes tables...')
    for checkbox in instrumentor.get_all_checkboxes():
        db.create_checkbox(**checkbox).executescript()

    print('Writing dropdowns tables...')
    for dropdown in instrumentor.get_all_dropdowns():
        db.create_lookup(**dropdown).executescript()

    print('Writing radios tables...')
    for radio in instrumentor.get_all_radios():
        db.create_lookup(**radio).executescript()

    db.close()
    print('Done with the schema starting with inserts...')

    db = dynamic_schema(database_path)

    for root, dirs, files in file_util.walk(redcap_data_path):
        for path in files:
            filepath = file_util.join([root, path])
            data = file_util.read(filepath, 'json')
            tablename = path.split('.')[0]

            table_info = db.table_info(table=tablename).execute().fetchall()
            name_index = 1
            is_pk_index = 5
            if tablename != subject_form:
                cols = [row[name_index] for row in table_info if not row[is_pk_index]]
            else:
                cols = [row[name_index] for row in table_info]
            cols.append('redcap_event_name')
            vals = []
            if type(data) != type([]):
                data = [data]
            for item in data:
                val = [str(item.setdefault(col, None)) for col in cols]
                val = [s.replace("\'","\'\'") if s != 'None' else '' for s in val]
                nonempty = [s for s in val if s != '']
                required_fields = 2 if tablename == subject_form else 3
                if len(nonempty) >= required_fields:
                    vals.append(val)
                else:
                    pass

            print('Writing {} many rows to the {} table'.format(len(vals), tablename))
            db.insert(tablename=tablename, cols=cols, vals=vals).execute()
            db.commit()

    print('Done with inserting data')

