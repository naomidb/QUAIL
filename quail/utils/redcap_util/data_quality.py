import json
from copy import copy

def record_has_data(record, unique_field_name=None, form_record_name=None):
    """
    When asking for a whole form in redcap from a longitudinal project you will get back
    extra records that wont have data. Since we dont want to write a whole bunch of empty
    records to whatever store we are using we want to know when they contain no information

    Therefore, we check to make sure that they have more than just the subject identifier,
    event identifier, and the always defaulted to zero form_complete field.

    The one caveat here is that the unique field IS a new piece of information on the form
    which contains it. That is why we default it to falsy here so we dont always need to
    pass it if we want the unique field to quailfy as new data
    """
    copied = copy(record)

    if unique_field_name:
        del copied[unique_field_name]
    del copied['redcap_event_name']

    if form_record_name:
        form_key = form_record_name + '_complete'
        completed = record.get(form_key)
        if str(completed) == '0':
            del copied[form_key]

    to_delete = []
    for key, val in copied.items():
        test_val = str(val)
        if not test_val:
            to_delete.append(key)

    for key in to_delete:
        del copied[key]

    return bool(len(copied.keys()))

