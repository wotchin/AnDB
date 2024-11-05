from andb.configrations.mgr import ConfigurationMgr
from andb.runtime import global_vars


def should_raise_exception_while_setting(config, option, value):
    try:
        config.set_value(option, value)
    except ValueError as e:
        assert 'Incorrect configuration value.' == str(e)
    else:
        raise AssertionError()


def test_configuration_mgr():
    config = ConfigurationMgr()
    assert (config.get_value('buffer_pool_size')) == 1024
    assert global_vars.buffer_pool_size == 1024
    try:
        config.getint('buffer_pool_size')
    except NotImplementedError:
        pass
    else:
        raise AssertionError()

    config.set_value('buffer_pool_size', 2048)
    assert config.get_value('buffer_pool_size') == 2048
    config.perform_side_effects()
    assert global_vars.buffer_pool_size == 2048
    should_raise_exception_while_setting(config, 'buffer_pool_size', 0.1)
    should_raise_exception_while_setting(config, 'abc', 1)
    should_raise_exception_while_setting(config, 'max_dirty_page_pct', 110)

    filename = 'abc'
    with open(filename, 'w+') as fp:
        config.write(fp)

    with open(filename, 'r+') as fp:
        config2 = ConfigurationMgr()
        config2.read_file(fp)
        assert list(config.items()) == list(config2.items())
        assert (config2.get_value('buffer_pool_size')) == 2048

    with ConfigurationMgr().bind_file(filename) as config3:
        assert list(config.items()) == list(config3.items())
        assert (config3.get_value('buffer_pool_size')) == 2048
        config3.set_value('datadir', '/path/to/data')
        assert global_vars.database_directory == 'data'

    with ConfigurationMgr().bind_file(filename) as config3:
        assert config3.get_value('datadir') == '/path/to/data'
        assert global_vars.database_directory == '/path/to/data'
        config3.set_value('datadir', 'tmp')
        assert global_vars.database_directory == '/path/to/data'
        config3.perform_side_effects()
        assert global_vars.database_directory == 'tmp'

    import os
    os.unlink(filename)
