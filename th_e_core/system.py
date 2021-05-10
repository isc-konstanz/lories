# -*- coding: utf-8 -*-
"""
    th-e-core.system
    ~~~~~~~~~~~~~~~~
    
    
"""
import logging
logger = logging.getLogger(__name__)

import os
import re
from collections.abc import MutableSequence, MutableMapping

from configparser import ConfigParser
from th_e_core import Configurable, Database

INVALID_CHARS = "'!@#$%^&?*;:,./\|`´+~=- "


class Systems(MutableSequence):

    def __init__(self, *systems):
        self._systems = list()
        self._systems.extend(systems)

    def __iter__(self):
        return iter(self._systems)

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

    def run(self, *args, **kwargs):
        for system in self:
            system.run(*args, **kwargs)


class System(Configurable, MutableMapping):

    def __init__(self, configs, **kwargs):
        if not isinstance(configs, ConfigParser):
            raise ValueError('Invalid configuration type: {}'.format(type(configs)))
        
        if not configs.has_option('General', 'name'):
            raise ValueError('Invalid configuration, missing specified system name')
        
        if configs.has_option('General', 'id'):
            self.id = configs['General']['id']
        else:
            self.id = configs['General']['name']
            configs['General']['id'] = self.id
        
        self.name = configs['General']['name']
        
        self._configs = configs
        self._configure(configs, **kwargs)
        
        self._components = self._components_read(**kwargs)
        self._activate(self._components, configs, **kwargs)

    def _configure(self, configs, **kwargs):
        super()._configure(configs, **kwargs)
        
        if configs.has_section('Location'):
            from pvlib.location import Location
            self.location = Location(configs.getfloat('Location', 'latitude'), 
                                     configs.getfloat('Location', 'longitude'), 
                                     tz=configs.get('Location', 'timezone', fallback='UTC'), 
                                     altitude=configs.getfloat('Location', 'altitude', fallback=0), 
                                     name=self.name, 
                                     **kwargs)

    def _activate(self, components, configs, **kwargs): #@UnusedVariable
        if configs.has_section('Database') and \
                configs.get('Database', 'enabled', fallback='True').lower() == 'true' and \
                configs.get('Database', 'enable', fallback='True').lower() == 'true':

            if 'dir' in configs['Database']:
                database_dir = configs['Database']['dir']
                if not os.path.isabs(database_dir):
                    configs['Database']['dir'] = os.path.join(configs['General']['data_dir'], database_dir)
            else:
                configs['Database']['dir'] = configs['General']['data_dir']
            
            self._database = Database.open(configs, **kwargs)
        else:
            self._database = None

    def _components_read(self, **kwargs):
        cmpt_dir = self._configs.get('General', 'cmpt_dir', fallback='cmpt')
        if not os.path.isabs(cmpt_dir):
            cmpt_dir = os.path.join(self._configs['General']['config_dir'], cmpt_dir)
        
        if not os.path.isdir(cmpt_dir):
            cmpt_dir = self._configs['General']['config_dir']
        
        components = dict()
        for entry in os.scandir(cmpt_dir):
            if entry.is_file() and entry.path.endswith('.cfg') and not entry.path.endswith('default.cfg') \
                    and entry.name.startswith(tuple(self._component_types)):
                
                component = self._component_read(entry, **kwargs)
                components[component.id] = component
        
        return components

    def _component_read(self, config_file, **kwargs):
        component_configs = Component._read_configs(self, config_file)
        component_type = [t for t in self._component_types if config_file.name.startswith(t)][0]
        component = self._component(component_configs, component_type, **kwargs)
        
        if not isinstance(component, Component):
            raise TypeError('Invalid component type: {}'.format(type(component)))
        
        return component

    def _component(self, configs, type, **kwargs): #@ReservedAssignment
        if type == 'pv':
            from th_e_core.pvsystem import PVSystem
            return PVSystem(configs, self, **kwargs)
        
        return Component._import(configs, self._configs.get('Import', 'package'), 'system', ConfigComponent.__name__,
                                 self, **kwargs)

    @property
    def _component_types(self):
        return ['component', 'cmpt', 'pv']

    def contains_type(self, key):
        return len(self.get_type(key)) > 0

    def get_type(self, key):
        return [component for component in self._components.values() if component.type in key]

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, s):
        for c in INVALID_CHARS:
            s = s.replace(c, '_')
        
        self._id = re.sub('[^A-Za-z0-9_]+', '', s).lower()

    def __getattr__(self, attr):
        if attr in self._component_types:
            return self._components[self.__keytransform__(attr)]
        try:
            return super().__getattr__(attr)
        
        except AttributeError:
            raise AttributeError("'{0}' object has no attribute '{1}'".format(type(self).__name__, attr))

    def __getitem__(self, key):
        return self._components[self.__keytransform__(key)]

    def __setitem__(self, key, value):
        if not isinstance(value, Component):
            raise TypeError('Invalid component type: {}'.format(type(value)))
        
        self._components[self.__keytransform__(key)] = value

    def __delitem__(self, key):
        del self._components[self.__keytransform__(key)]

    def __iter__(self):
        return iter(self._components)

    def __len__(self):
        return len(self._components)

    def __keytransform__(self, key):
        return key

    @classmethod
    def read(cls, data_dir='data', config_scan=False, **kwargs):
        systems = Systems()
        
        if not isinstance(config_scan, bool):
            config_scan = str(config_scan).lower() == 'true'
        kwargs['config_scan'] = config_scan
        
        if config_scan:
            for system_dir in os.scandir(data_dir):
                if os.path.isdir(system_dir.path):
                    systems.append(cls._read(data_dir=system_dir.path, **kwargs))
        else:
            systems.append(cls._read(data_dir=data_dir, **kwargs))
        
        return systems

    def run(self, *args, **kwargs):
        raise NotImplementedError


class Component(Configurable):

    def __init__(self, configs, context, **kwargs):
        super().__init__(configs, **kwargs);
        
        if not configs.has_option('General', 'id'):
            raise ValueError('Invalid configuration, missing specified component id')
        
        self.id = configs.get('General', 'id')
        
        self._context = context
        self._activate(context, **kwargs)

    def _activate(self, system, **kwargs):
        pass

    @staticmethod
    def _read_configs(context, config_file):
        configs = Configurable._read_configs(context._configs.get('General', 'root_dir'), 
                                             context._configs.get('General', 'lib_dir'), 
                                             context._configs.get('General', 'tmp_dir'), 
                                             context._configs.get('General', 'data_dir'),  
                                             os.path.dirname(config_file.path), 
                                             config_file.name)
        
        if not configs.has_option('General', 'id'):
            configs.set('General', 'id', os.path.splitext(config_file.name)[0])
        
        return configs

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, s):
        self._id = re.sub('[^A-Za-z0-9]+', '', s.translate ({ord(c): "_" for c in INVALID_CHARS}))

    @property
    def type(self):
        return 'cmpt'


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

