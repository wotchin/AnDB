from configparser import ConfigParser

from andb.runtime import global_vars
from andb.runtime import session_vars


class ConfigOption:
    CONTEXT_SESSION = 'session'
    CONTEXT_RELOAD = 'reload'
    CONTEXT_REBOOT = 'reboot'
    CONTEXT_CONSTANT = 'constant'  # cannot set

    OPTTYPE_INT = int
    OPTTYPE_FLOAT = float
    OPTTYPE_STRING = str

    def __init__(self, name, value, opttype=int, min_val=0, max_val=65535, enumvals=None, context='reboot'):
        self.name = name
        self.value = value
        assert opttype in (ConfigOption.OPTTYPE_INT, ConfigOption.OPTTYPE_FLOAT, ConfigOption.OPTTYPE_STRING)
        self.opttype = opttype
        self.min_val = min_val
        self.max_val = max_val
        self.enumvals = enumvals
        assert context in (ConfigOption.CONTEXT_SESSION, ConfigOption.CONTEXT_RELOAD, ConfigOption.CONTEXT_REBOOT,
                           ConfigOption.CONTEXT_CONSTANT)
        self.context = context

        # do nothing in defaults
        self._side_effect = ConfigOption._dummy

        if self.opttype is ConfigOption.OPTTYPE_INT:
            self._check = self.int_check_func
        elif self.opttype is ConfigOption.OPTTYPE_FLOAT:
            self._check = self.float_check_func
        elif self.opttype is ConfigOption.OPTTYPE_STRING:
            self._check = self.str_check_func
        else:
            self._check = ConfigOption._dummy

    @staticmethod
    def _dummy(*args, **kwargs):
        pass

    def numeric_check_func(self, value):
        if self.context == ConfigOption.CONTEXT_CONSTANT:
            return False
        if self.enumvals is not None:
            return value in self.enumvals
        return self.min_val <= value <= self.max_val

    def int_check_func(self, value):
        return isinstance(value, int) and self.numeric_check_func(value)

    def float_check_func(self, value):
        return isinstance(value, float) and self.numeric_check_func(value)

    def str_check_func(self, value):
        if self.context == ConfigOption.CONTEXT_CONSTANT:
            return False
        if self.enumvals is not None:
            return value in self.enumvals
        return self.min_val <= len(value) <= self.max_val

    def set_check_function(self, func):
        self._check = func
        return self

    def set_side_effect_function(self, func):
        self._side_effect = func
        return self

    def check(self, value):
        return self._check(value)

    def perform_side_effect(self, value):
        return self._side_effect(value)


def get_side_effect_function(context, variable_name):
    def effect_in_global(v):
        #TODO: multiprocess should do more?
        assert hasattr(global_vars, variable_name)
        setattr(global_vars, variable_name, v)
        return getattr(global_vars, variable_name)

    def effect_in_session(v):
        assert hasattr(session_vars, variable_name)
        setattr(session_vars, variable_name, v)
        return getattr(session_vars, variable_name)

    if context == 'global':
        return effect_in_global
    elif context == 'session':
        return effect_in_session
    else:
        raise ValueError(context)


class ConfigurationMgr(ConfigParser):
    DEFAULT_SECTION = 'AnDB'

    """This is a tool class and cannot be resident in memory."""

    def __init__(self):
        super().__init__()
        super().add_section(ConfigurationMgr.DEFAULT_SECTION)
        # load defaults
        self._default_mapper = {}
        self._set_defaults()
        self._filepath = None
        self._fp = None
        self.perform_side_effects()

    def get_value(self, option):
        if option not in self._default_mapper:
            return None
        config_opt = self._default_mapper[option]
        rv = self.get(section=self.DEFAULT_SECTION, option=option,
                      fallback=config_opt.value)
        return config_opt.opttype(rv)

    def getint(self, option, **kwargs):
        raise NotImplementedError('Use get_value() instead.')

    def getfloat(self, option, **kwargs):
        raise NotImplementedError('Use get_value() instead.')

    def getboolean(self, option, **kwargs):
        raise NotImplementedError('Use get_value() instead.')

    def add_section(self, section):
        raise NotImplementedError('Forbid to add section.')

    def _set_defaults(self):
        defaults = [
            ConfigOption(name='datadir', value='data', opttype=str, min_val=0, max_val=65535, enumvals=None,
                         context='reboot').set_side_effect_function(
                get_side_effect_function('global', 'database_directory')),
            ConfigOption(name='buffer_pool_size', value=1024, opttype=int, min_val=0, max_val=65535, enumvals=None,
                         context='reboot').set_side_effect_function(
                get_side_effect_function('global', 'buffer_pool_size')),
            ConfigOption(name='port', value=5678, opttype=int, min_val=1024, max_val=65535, enumvals=None,
                         context='reboot'),
            ConfigOption(name='max_connections', value=1024, opttype=int, min_val=0, max_val=65535, enumvals=None,
                         context='reload'),
            ConfigOption(name='process_pool_size', value=0, opttype=int, min_val=0, max_val=2048, enumvals=None,
                         context='reboot'),
            ConfigOption(name='work_mem', value=1024, opttype=int, min_val=0, max_val=65535, enumvals=None,
                         context='reload'),
            ConfigOption(name='max_dirty_page_pct', value=90, opttype=int, min_val=0, max_val=100, enumvals=None,
                         context='reboot'),
        ]
        for config_opt in defaults:
            key = config_opt.name
            self._default_mapper[key] = config_opt
            # set default values from builtin settings
            self.set_value(key, config_opt.value)

    def options(self, *kwargs):
        return super().options(ConfigurationMgr.DEFAULT_SECTION)

    def items(self, *kwargs):
        return super().items(section=ConfigurationMgr.DEFAULT_SECTION)

    def perform_side_effects(self):
        """Set all values to runtime variables using side effect function."""
        for option in self.options():
            config_opt = self._default_mapper[option]
            value = self.get_value(option)
            config_opt.perform_side_effect(value)

    def get_default(self, option):
        config_opt = self._default_mapper.get(option)
        if config_opt:
            return config_opt.value
        return None

    def set_value(self, option, value=None):
        config_opt = self._default_mapper.get(option)
        if not config_opt or (not config_opt.check(value)):
            raise ValueError('Incorrect configuration value.')
        # option values must be strings
        return self.set(section=ConfigurationMgr.DEFAULT_SECTION, option=option,
                        value=str(value))

    def bind_file(self, filepath):
        self._filepath = filepath
        return self

    def __enter__(self):
        assert self._filepath
        self._fp = open(file=self._filepath, mode='r+', errors='ignore')
        self.read_file(self._fp)
        self.perform_side_effects()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._fp.truncate(0)
        self._fp.seek(0)
        self.write(self._fp)
        self._fp.flush()
        self._fp.close()
