
try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

with open('quail/version.py') as ver:
    exec(ver.read())

config = {
    'description': 'QUAIL',
    'author': 'Patrick White',
    'url': 'https://github.com/ctsit/QUAIL',
    'author_email': 'pfwhite9@gmail.com',
    'version': __version__,
    'install_requires': [
        'docopt==0.6.2',
        'nose',
        'cappy==1.1.1',
        'pyyaml==3.12',
        'Jinja2==2.9.6',
        'python-dateutil==2.6.0',
    ],
    'dependency_links': ["git+https://github.com/ctsit/cappy@1.1.1#egg=cappy-1.1.1"],
    'include_package_data': True,
    'packages': find_packages(),
    'scripts': [],
    'entry_points': {
          'console_scripts': [
              'quail = quail.__main__:cli_run',
          ],
      },
    'name': 'quail'
}

setup(**config)

