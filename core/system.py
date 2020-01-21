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

from .database import Database

INVALID_CHARS = "'!@#$%^&?*;:,./\|`´+~=- "

SECTION_GENERAL = 'General'
SECTION_DATABASE = 'Database'
SECTION_IMPORT = 'Import'

SETTINGS_DEFAULT = os.path.join('conf', 'settings.cfg')


class Systems(MutableSequence):

    def __init__(self, *systems):
        self._systems = list()
        self._systems.extend(systems)

    @staticmethod
    def read(configdir=SETTINGS_DEFAULT, **kwargs):
        if not os.path.isdir(configdir):
            raise ValueError('Invalid configuration directory: {}'.format(configdir))
        
        settingsfile = os.path.join(configdir, 'settings.cfg')
        if not os.path.isfile(settingsfile):
            raise ValueError('Unable to open settings: {}'.format(settingsfile))
        
        systems = Systems()
        settings = ConfigParser()
        settings.read(settingsfile)
        
        if SECTION_GENERAL not in settings.sections():
            settings.add_section(SECTION_GENERAL)
        
        settings.set(SECTION_GENERAL, 'path', settingsfile)
        settings.set(SECTION_GENERAL, 'configs', configdir)
        
        if 'libs' not in settings[SECTION_GENERAL]:
            settings.set(SECTION_GENERAL, 'libs', 'lib')
        
        if 'dir' not in settings[SECTION_GENERAL]:
            settings.set(SECTION_GENERAL, 'dir', 'data')
        
        if 'recursive' not in settings[SECTION_GENERAL]:
            settings.set(SECTION_GENERAL, 'recursive', 'false')
        if settings.get(SECTION_GENERAL, 'recursive').lower() == 'true':
            for d in os.scandir(settings.get(SECTION_GENERAL, 'dir')):
                if d.is_dir() and os.path.isfile(os.path.join(d.path, 'settings.cfg')):
                    systems.append(Systems._read_settings(copy.deepcopy(settings), d.path, **kwargs))
        else:
            if 'name' not in settings[SECTION_GENERAL]:
                settings.set(SECTION_GENERAL, 'name', 'default')
            
            systems.append(Systems._read_settings(copy.deepcopy(settings), **kwargs))
        
        return systems

    @staticmethod
    def _read_settings(settings, **kwargs):
        settingsfile = os.path.join(settings.get(SECTION_GENERAL, 'dir'), 'settings.cfg')
        if os.path.isfile(settingsfile):
            settings.read(settingsfile)
        
        if settings.has_section(SECTION_DATABASE):
            database = settings[SECTION_DATABASE]['dir']
            if not os.path.isabs(database):
                settings[SECTION_DATABASE]['dir'] = os.path.join(settings.get(SECTION_GENERAL, 'dir'), database)
        
        if SECTION_IMPORT not in settings.sections():
            settings.add_section(SECTION_IMPORT)
        
        if 'package' not in settings[SECTION_IMPORT]:
            package = kwargs.get('package') if 'package' in kwargs else 'core'
            settings.set(SECTION_IMPORT, 'package', package)
        if 'module' not in settings[SECTION_IMPORT]:
            settings.set(SECTION_IMPORT, 'module', 'system')
        if 'type' not in settings[SECTION_IMPORT]:
            settings.set(SECTION_IMPORT, 'type', 'System')
        
        module = __import__(settings[SECTION_IMPORT]['package']+'.'+settings[SECTION_IMPORT]['module'], 
                            fromlist=[settings[SECTION_IMPORT]['type']])
        system = getattr(module, settings[SECTION_IMPORT]['type'])(settings, **kwargs)
        
        if not isinstance(system, System):
            raise ValueError('Invalid system type: {}'.format(type(system)))
        
        for entry in os.scandir(settings.get(SECTION_GENERAL, 'dir')):
            if entry.is_file() and not entry.name.startswith(('settings', 'model')) and entry.path.endswith('.cfg'):
                configs = copy.deepcopy(settings)
                configs.read(entry.path)
                
                configs.set(SECTION_GENERAL, 'path', entry.path)
                
                name = os.path.splitext(entry.name)[0]
                if 'name' not in configs[SECTION_GENERAL]:
                    configs.set(SECTION_GENERAL, 'name', name) 
                
                if 'id' not in configs[SECTION_GENERAL]:
                    configs.set(SECTION_GENERAL, 'id', 
                                re.sub('[^A-Za-z0-9]+', '', name.lower() ({ord(c): "_" for c in INVALID_CHARS})))
                
                system.add(configs, **kwargs)
        
        return system

    def __len__(self):
        return len(self._systems)

    def __getitem__(self, index):
        return self._systems[index]

    def __delitem__(self, index):
        del self._systems[index]

    def __setitem__(self, index, value):
        if not isinstance(value, System):
            raise ValueError('Invalid system type: {}'.format(type(value)))
        
        self._systems[index] = value

    def insert(self, index, value):
        if not isinstance(value, System):
            raise ValueError('Invalid system type: {}'.format(type(value)))
        
        self._systems.insert(index, value)


class System(MutableMapping):

    def __init__(self, settings, **kwargs):
        if not isinstance(settings, ConfigParser):
            raise ValueError('Invalid settings type: {}'.format(type(settings)))
        
        if not settings.has_option('General', 'name'):
            raise ValueError('Invalid settings without specified name')
        
        if settings.has_option('General', 'id'):
            self.id = settings['General']['id']
        else:
            self.id = settings['General']['name'].lower()
        
        self.id = re.sub('[^A-Za-z0-9]+', '', self.id.translate ({ord(c): "_" for c in INVALID_CHARS}))
        self.name = settings['General']['name']
        
        self._components = dict()
        self._configure(settings, **kwargs)
        #self._model = Model.read(settings, **kwargs)

    def __setitem__(self, key, value):
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

    def _configure(self, configs, **kwargs):
        pass

    def add(self, configs):
        self[configs.id] = configs

