# -*- coding: utf-8 -*-
"""
    th-e-core.model
    ~~~~~~~~~~~~~~~
    
    
"""
import logging
logger = logging.getLogger(__name__)

from abc import ABC, abstractmethod

from th_e_core.configs import Configurable
from th_e_core.system import System


class Model(ABC, Configurable):

    @classmethod
    def read(cls, context, **kwargs):
        if not isinstance(context, Configurable):
            raise TypeError('Invalid context type: {}'.format(type(context)))
        
        configs = cls.read_configs(context, **kwargs)
        return cls.from_configs(context, configs, **kwargs)

    @staticmethod
    def read_configs(context, config_name='model.cfg', **kwargs):
        if not isinstance(context, Configurable):
            raise TypeError('Invalid context type: {}'.format(type(context)))
        
        return Configurable._read_configs(context._configs.get('General', 'root_dir'), 
                                          context._configs.get('General', 'lib_dir'), 
                                          context._configs.get('General', 'tmp_dir'), 
                                          context._configs.get('General', 'data_dir'), 
                                          context._configs.get('General', 'config_dir'), 
                                          config_name, **kwargs)

    @staticmethod
    def from_configs(context, configs, **kwargs):
        package = context._configs.get('Import', 'package', fallback='.'.join(context.__module__.split('.')[:-1]))
        model = Model._from_configs(configs, package, 'model', 'Model', 
                                    context, **kwargs)
        
        if not isinstance(model, Model):
            raise TypeError('Invalid model type: {}'.format(type(model)))
        
        return model

    def __init__(self, configs, context, **kwargs):
        super().__init__(configs, **kwargs)
        
        self._context = context
        self._build(context, configs, **kwargs)

    def _build(self, context, configs, **kwargs):
        pass

    @property
    def _system(self):
        if not isinstance(self._context, System):
            raise TypeError('Context is not of type System: {}'.format(type(self._context)))
        
        return self._context

    @abstractmethod
    def run(self, *args, **kwargs):
        pass

