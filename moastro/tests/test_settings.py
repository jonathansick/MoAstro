import os

from  ..utils.pkgdata import data_path


def setup_test_config():
    """Make moastro/data/example_moastro.json the settings file."""
    json_path = data_path('example_moastro.json')
    print json_path
    assert os.path.exists(json_path) == True
    os.putenv('MOASTROCONFIG', json_path)


def test_read_settings():
    from ..settings import read_settings
    # setup_test_config()  # FIXME can't read package data from tests?
    conf = read_settings()
    assert 'servers' in conf
    assert 'local' in conf['servers']
    assert 'localhost' == conf['servers']['local']['url']
    assert 27017 == conf['servers']['local']['port']


def test_locate_server():
    from ..settings import locate_server
    # setup_test_config()  # FIXME can't read package data from tests?
    url, port = locate_server('local')
    assert url == 'localhost'
    assert port == 27017
