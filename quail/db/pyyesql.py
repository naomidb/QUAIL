import os
import sqlite3
from types import SimpleNamespace

from jinja2 import Environment, PackageLoader, select_autoescape

class Query(object):
    def __init__(self, name, docstring, template, cursor):
        self.template = template
        self.__doc__ = 'docstring'
        self.name = name
        self.cursor = cursor

    def __call__(self, **context):
        self.query = self.template.render(**context)
        return self

    def log(self):
        print(self.query)
        return self

    def execute(self):
        try:
            return self.cursor.execute(self.query)
        except Exception as err:
            print(self.query)
            raise err

    def executescript(self):
        """
        In the future we will want the function definition in the sql
        to define which of the execute functions we want
        """
        try:
            return self.cursor.executescript(self.query)
        except Exception as err:
            print(self.query)
            raise err

class Database(object):

    def __init__(self, **options):
        """
        Right now only supports sqlite

        databases: name, paths dict of databases
        query_path: absolute path of directory that contains formatted queryies
        """
        if options.get('database_path'):
            self._multiple = False
            self._connection = sqlite3.connect(options.get('database_path'))
        elif options.get('databases'):
            self._multiple = True
            self._connection = sqlite3.connect(':memory:')
        else:
            raise Exception('You must initialize the DB with a path or a name: path dict')

        self.cursor = self._connection.cursor()
        self.commit = lambda : self._connection.commit()
        self.close = lambda : self._connection.close()

        for key, item in options.items():
            setattr(self, '_' + key, item)

        if self._multiple == True:
            for name, path in self._databases.items():
                self._attach_database(name, path)

        self.__add_functions(self._query_path)


    def _attach_database(self, name, path):
        self.cursor.execute("""
        ATTACH DATABASE ? as ?
        """, (path, name))
        self.commit()

    @staticmethod
    def __parse_query_at_path(path):
        qfile = open(path, 'r')
        funcs = {}
        opened = False
        next_lines = 'open'
        func_name = None
        for index, line in enumerate( qfile ):
            if '/*' in line:
                if func_name:
                    func_name = None
                opened = True
                next_lines = 'name'
            elif '*/' in line:
                opened = False
                next_lines = 'sql'
            elif opened and next_lines == 'name':
                func_name = line.split()[-1]
                funcs[func_name] = {}
                funcs[func_name]['name'] = func_name
                funcs[func_name]['docstring'] = ''
                funcs[func_name]['sql'] = ''
                next_lines = 'docstring'
            elif opened and next_lines == 'docstring' or next_lines == 'sql':
                funcs[func_name][next_lines] += line
            elif line == '\n' and not opened:
                continue
            else:
                msg = 'There was a bad config at path {}, line {}'.format(path, index)
                raise Exception(msg)
        return list(funcs.values())

    def __instantiate_and_attach(self, environment, namespace, funcs):
        for func in funcs:
            template = environment.from_string(func['sql'])
            query = Query(name=func['name'], docstring=func['docstring'], template=template, cursor=self.cursor)
            if namespace:
                setattr(getattr(self, namespace), func['name'], query)
            else:
                setattr(self, func['name'], query)


    def __add_functions(self, query_path):
        """
        Adds functions to the DataBase object based on the queries
        the the query dir.
        """
        if os.path.isfile(query_path) and '.sql' in query_path:
            root, path = os.path.split(query_path)
            funcs = self.__parse_query_at_path(os.path.join(root, path))
            self.__instantiate_and_attach(Environment(), None, funcs)
        else:
            for root, dirs, files in os.walk(query_path):
                for path in files:
                    if '.sql' in path:
                        namespace = path.split('.')[0]
                        setattr(self, namespace, SimpleNamespace())
                        funcs = self.__parse_query_at_path(os.path.join(root, path))
                        self.__instantiate_and_attach(Environment(), namespace, funcs)
