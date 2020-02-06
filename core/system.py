# -*- coding: utf-8 -*-
"""
    th-e-core.system
    ~~~~~~~~~~~~~~~~
    
    
"""
import logging
logger = logging.getLogger(__name__)

import os
import re
import copy
from collections.abc import MutableSequence, MutableMapping
from configparser import ConfigParser

INVALID_CHARS = "'!@#$%^&?*;:,./\|`´+~=- "


class Systems(MutableSequence):

    def __init__(self, *systems):
        self._systems = list()
        self._systems.extend(systems)

    def __len__(self):
        return len(self._systems)

    def __getitem__(self, index):
        return self._systems[index]

    def __delitem__(self, index):
        del self._systems[index]

    def __setitem__(self, index, value):
        if not isinstance(value, System):
            raise TypeError('Invalid system type: {}'.format(type(value)))
        
        self._systems[index] = value

    def insert(self, index, value):
        if not isinstance(value, System):
            raise TypeError('Invalid system type: {}'.format(type(value)))
        
        self._systems.insert(index, value)

    def read(self, 
             root_dir='.', 
             lib_dir='lib', 
             data_dir='data', 
             config_dir='conf', 
             config_scan=False,
             **kwargs):
        
        if not os.path.isabs(config_dir):
            config_dir = os.path.join(root_dir, config_dir)
        if not os.path.isdir(config_dir):
            raise ValueError('Invalid configuration directory: {}'.format(config_dir))
        
        config_file = os.path.join(config_dir, 'system.cfg')
        if not os.path.isfile(config_file):
            raise ValueError('Unable to open system configurations: {}'.format(config_file))
        
        configs = ConfigParser()
        configs.read(config_file)
        
        if 'General' not in configs.sections():
            configs.add_section('General')
        
        configs.set('General', 'root_dir', root_dir)
        configs.set('General', 'lib_dir', lib_dir if os.path.isabs(lib_dir) else os.path.join(root_dir, lib_dir))
        configs.set('General', 'data_dir', data_dir if os.path.isabs(data_dir) else os.path.join(root_dir, data_dir))
        configs.set('General', 'config_dir', config_dir)
        configs.set('General', 'config_file', config_file)
        configs.set('General', 'config_scan', str(config_scan))
        
        if configs.get('General', 'config_scan').lower() == 'true':
            for d in os.scandir(configs.get('General', 'data_dir')):
                f = os.path.join(d.path, 'system.cfg')
                if d.is_dir() and os.path.isfile(f):
                    c = copy.deepcopy(configs)
                    c.set('General', 'data_dir', d.path)
                    c.set('General', 'config_file', f)
                    c.read(f)
                    
                    self.append(self._read_system(c, **kwargs))
        else:
            if 'name' not in configs['General']:
                configs.set('General', 'name', 'system')
            
            self.append(self._read_system(copy.deepcopy(configs), **kwargs))

    def _read_system(self, configs, **kwargs):
        system = self._init_system(configs, **kwargs)
        
        if configs.get('General', 'config_scan').lower() != 'true':
            config_dir = configs.get('General', 'config_dir')
        else:
            config_dir = configs.get('General', 'data_dir')
        
        for entry in os.scandir(config_dir):
            if entry.is_file() and entry.path.endswith('.cfg') and entry.name.startswith(tuple(system._component_types)):
                component = system._read_component(entry, **kwargs)
                system[component.id] = component
        
        from core import Model
        system._model = Model.build(system, **kwargs)
        
        return system

    def _init_system(self, configs, **kwargs):
        try:
            if 'type' not in configs['General']:
                configs.set('General', 'type', 'System')
            if 'module' not in configs['General']:
                configs.set('General', 'module', 'system')
            if 'package' not in configs['General']:
                package = kwargs.get('package') if 'package' in kwargs else 'core'
                configs.set('General', 'package', package)
            
            system = self._import_system(configs, **kwargs)
            
            if not isinstance(system, System):
                raise TypeError('Invalid system type: {}'.format(type(system)))
        
        except (ImportError, TypeError) as e:
            logger.warning('Error instancing {}.{}.{} and core class will be used: {}'.format(configs['General']['package'], 
                                                                                              configs['General']['module'], 
                                                                                              configs['General']['type']), e)
            configs.set('General', 'type', 'System')
            configs.set('General', 'module', 'system')
            configs.set('General', 'package', 'core')
            system = self._import_system(configs, **kwargs)
            
        return system

    def _import_system(self, configs, **kwargs):
        module = __import__(configs['General']['package']+'.'+configs['General']['module'], 
                            fromlist=[configs['General']['type']])
        return getattr(module, configs['General']['type'])(configs, **kwargs)

    def run(self, **kwargs):
        for system in self:
            system.run(**kwargs)


class System(MutableMapping):

    def __init__(self, configs, **kwargs):
        #super().__init__(**kwargs)
        
        if not isinstance(configs, ConfigParser):
            raise ValueError('Invalid system configuration type: {}'.format(type(configs)))
        
        if not configs.has_option('General', 'name'):
            raise ValueError('Invalid system configurations without specified name')
        
        if 'id' not in configs['General']:
            configs.set('General', 'id', configs['General']['name'].lower())
        
        self.id = configs['General']['id']
        self.name = configs['General']['name']
        
        self._components = dict()
        self._configs = configs
        self._configure(configs, **kwargs)
        
        if 'Database' in configs:
            from core import Database
            
            if 'dir' in configs['Database']:
                database_dir = configs['Database']['dir']
                if not os.path.isabs(database_dir):
                    configs['Database']['dir'] = os.path.join(configs['General']['data_dir'], database_dir)
            
            self._database = Database.open(configs)
        else:
            self._database = None

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, s):
        self._id = re.sub('[^A-Za-z0-9]+', '', s.translate ({ord(c): "_" for c in INVALID_CHARS}))

    def __setitem__(self, key, value):
        if not isinstance(value, Component):
            raise TypeError('Invalid component type: {}'.format(type(value)))
        
        self._components[self.__keytransform__(key)] = value

    def __getitem__(self, key):
        return self._components[self.__keytransform__(key)]

    def __delitem__(self, key):
        del self._components[self.__keytransform__(key)]

    def __iter__(self):
        return iter(self._components)

    def __len__(self):
        return len(self._components)

    def __keytransform__(self, key):
        return key

    @property
    def _component_types(self):
        return ['component']

    def _read_component(self, config_file, **kwargs):
        configs = ConfigParser()
        configs.read(config_file.path)
        configs.set('General', 'id', os.path.splitext(config_file.name)[0])
        
        return self._init_component(configs, **kwargs)

    def _init_component(self, configs, **kwargs):
        return Component(self, configs, **kwargs)

    def _configure(self, configs, **_):
        if logger.isEnabledFor(logging.DEBUG):
            text = ''
            for section in configs.sections():
                text += '\n    [{}]'.format(section) + '\n'
                for (k, v) in configs.items(section):
                    text += '        {} = {}'.format(k, v) + '\n'
            
            print('[{}]{}'.format(configs.get('General', 'name'), text))

    def run(self, **kwargs):
        data = self._model.run(weather=self.weather.get(**kwargs), **kwargs)
        
        if self._database is not None:
            self._database.persist(data, **kwargs)
        
        return data


class Component:

    def __init__(self, parent, configs, **kwargs):
        super().__init__(**kwargs)
        
        if not isinstance(parent, System):
            raise TypeError('Invalid component parent type: {}'.format(type(parent)))
        if not isinstance(configs, ConfigParser):
            raise ValueError('Invalid component configuration type: {}'.format(type(configs)))
        
        if 'id' not in configs['General']:
            raise ValueError('Invalid component configurations without specified id')
        
        self.id = configs['General']['id']
        self.system = parent
        self._configs = configs
        self._configure(configs, **kwargs)

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, s):
        self._id = re.sub('[^A-Za-z0-9]+', '', s.translate ({ord(c): "_" for c in INVALID_CHARS}))

    @property
    def type(self):
        return 'cmpt'

    def _configure(self, configs, **_):
        if logger.isEnabledFor(logging.DEBUG):
            text = ''
            for section in configs.sections():
                text += '\n        [{}]'.format(section) + '\n'
                for (k, v) in configs.items(section):
                    text += '            {} = {}'.format(k, v) + '\n'
            
            print('    [Component {}]{}'.format(configs.get('General', 'id'), text))


class ConfigComponent(Component, MutableMapping):

    def __setitem__(self, key, value):
        self._configs[self.__keytransform__(key)] = value

    def __getitem__(self, key):
        return self._configs[self.__keytransform__(key)]

    def __delitem__(self, key):
        del self._configs[self.__keytransform__(key)]

    def __iter__(self):
        return iter(self._configs)

    def __len__(self):
        return len(self._configs)

    def __keytransform__(self, key):
        return key

    @property
    def type(self):
        return 'cfg'

