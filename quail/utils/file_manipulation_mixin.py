import os
import json
import shutil
import csv

import yaml

class FileManipulationMixin(object):
    """
    This class exists as a way to inherit and wrap os file manipulation
    functions.

    If more functionality is needed to do file manipulation please add those as
    class methods on this object.

    Any function that simply wraps an os api is done so that we keep all the functionality
    related to file manipulation codified in this one file.
    """

    @staticmethod
    def full_path(path):
        return os.path.abspath(path)

    @staticmethod
    def copyfile(src, dest):
        return shutil.copyfile(src, dest)

    @staticmethod
    def cwd():
        return os.getcwd()

    @staticmethod
    def path_split(path):
        """
        Splits the file into a head and tail
        /var/www/test => /var/www , test
        """
        return os.path.split(path)

    @staticmethod
    def walk(path):
        return os.walk(path)

    @staticmethod
    def get_most_recent_date_path(path):
        for root, dirs, files in FileManipulationMixin.walk(path):
            return FileManipulationMixin.join([root, sorted(dirs)[-1]])

    @staticmethod
    def join(dirs=[]):
        """
        Here we wrap the os.path.join so that it functions just like string.join
        """
        return os.path.join(*dirs)

    @staticmethod
    def mkdir(path):
        """
        There is almost never a time you want to make one directory. This function
        wraps the os.makedirs so that it functions just like unix 'mkdir -p'
        """
        if path:
            if not os.path.isdir(path):
                os.makedirs(path)

    @staticmethod
    def read(path, serialization_format=None, unsafe=False, **kwargs):
        """
        Loads data from a file. If a serialization format is passed
        the file will be parsed by that serializer. Only json and
        yaml are currently supported

        CSV is now also supported but will return an instance of a csv DictReader
        and so will require iteration to get all the items

        The unsafe parameter is there to not check that the file exists
        before opening it.
        """
        if unsafe or os.path.isfile(path):
            with open(path, 'r') as infile:
                if serialization_format == 'csv':
                    return csv.DictReader(infile, **kwargs)
                else:
                    text = infile.read()
                    if serialization_format == 'json':
                        data = json.loads(text)
                    elif serialization_format == 'yaml':
                        data = yaml.load(text)
                    else:
                        data = text
            return data
        else:
            return None

    @staticmethod
    def write(path, data='', serialization_format=None):
        """
        This is the writing twin to this classes read function.

        When passing data it will serialize in the chosen format,
        otherwise it will just write text into the file.
        """
        prefix, filename = os.path.split(path)
        FileManipulationMixin.mkdir(prefix)
        with open(path, 'w') as outfile:
            if serialization_format == 'json':
                outfile.write(json.dumps(data, indent=4, sort_keys=True))
            elif serialization_format == 'yaml':
                outfile.write(yaml.dump(data,
                                        indent=2,
                                        width=80,
                                        allow_unicode=True,
                                        default_flow_style=False))
            else:
                outfile.write(data)

    @staticmethod
    def write_csv(path, header, data, **kwargs):
        """
        Given a list of tuples and a header, writes them to a file as a csv
        """
        prefix, filename = os.path.split(path)
        FileManipulationMixin.mkdir(prefix)
        with open(path, 'w') as outfile:
            writer = csv.writer(outfile, **kwargs)
            writer.writerow(header)
            writer.writerows(data)
