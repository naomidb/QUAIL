
try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

config = {
    'description': 'QUAIL',
    'author': 'Patrick White',
    'url': 'URL to get it at.',
    'download_url': 'Where to download it.',
    'author_email': 'pfwhite9@gmail.com',
    'version': '0.1.0',
    'install_requires': ['nose', 'docopt', 'cappy', 'pyyaml'],
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

