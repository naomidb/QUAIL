import json
import datetime

import requests as req
from requests.auth import HTTPBasicAuth

from quail.utils.quail_conf_util import QuailConfig

def generate(quail_conf, dropper_name, url, user, password):
    config = QuailConfig(quail_conf)
    dropper_data = {
        'name': dropper_name,
        'url': url,
        'user': user,
        'password': password
    }
    config.add_source(dropper_name, dropper_data)
    config.save()

def get_meta(quail_conf, dropper_name):
    config = QuailConfig(quail_conf)

    # encode user pass for basic auth

    # call api
    # res = req.get(auth=HTTPBasicAuth(user, pass))

    batch = {
        'dropper_name': dropper_name,
        # 'date': project.date,
        # 'metadata_date': project.metadata_date,
        # 'path': file_util.join([project.batch_root, project.date]),
        # 'start': str(start),
        # 'end': str(end),
    }

    # make new batch in config
    batch_name = str(datetime.date.today())
    config.add_batch(dropper_name, batch_name, batch)

    # get path for batch of data

    # save metadata to file
    # data = json.loads(str(res.content, 'utf-8'))
    # file_util.write(metadata_path, data, 'json')

def get_data(quail_conf, dropper_name):
    # do some claw sftp stuff
    pass
