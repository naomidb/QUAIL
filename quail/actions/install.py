from quail.utils.file_manipulation_mixin import FileManipulationMixin as file_util

def run(root):
    """
    Initializes the file system where QUAIL will store its files

    The state of the file system after this should be

    root/
         batches/
         sources/
         subject_index.db
         quail.conf.yaml
    """
    root = file_util.full_path(root)
    print('Installing QUAIL at {}'.format(root))
    sources_path = file_util.join([root, 'sources'])
    batch_path = file_util.join([root, 'batches'])
    subject_index_path = file_util.join([root, 'subject_index.db'])
    conf_path = file_util.join([root, 'quail.conf.yaml'])

    conf_data = {
        'quail_root': root,
        'sources': {},
        'batches': {},
    }

    file_util.mkdir(sources_path)
    file_util.mkdir(batch_path)
    file_util.write(subject_index_path)
    file_util.write(conf_path, conf_data, serialization_format='yaml')

    print('QUAIL installed at {}'.format(root))




