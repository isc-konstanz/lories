# -*- coding: utf-8 -*-
"""
    th-e-core.io.json
    ~~~~~~~~~~~~~~~~~
    
    
"""
import os
import json
import shutil
import time

from . import Database, DatabaseException
from ..configs import Configurations
from collections import OrderedDict


class JsonDatabase:  # (Database):

    def __init__(self, configs: Configurations):
        # if configs.has_section('Database'):
        #     dbargs = dict(configs.items('Database'))
        # else:
        #     dbargs = {}
        # super().__init__(**dbargs)

        self._root_dir = configs.get('General', 'root_dir')
        self._lib_dir = configs.get('General', 'lib_dir')

    def exists(self, key: str, sub_dir: str = '', **kwargs):
        file_path = os.path.join(self._lib_dir, sub_dir, key + '.json')

        if not os.path.isfile(file_path):
            file_path = os.path.join(self._root_dir, 'lib', sub_dir, key + '.json')

        return os.path.isfile(file_path)

    def read(self, key: str, sub_dir: str = '', **kwargs):
        file = key + '.json'
        file_dir = os.path.join(self._lib_dir, sub_dir)
        file_path = os.path.join(file_dir, file)

        if not os.path.isfile(file_path):
            file_dir = os.path.join(self._root_dir, 'lib', sub_dir)

        return self._read(file_dir, file)

    @staticmethod
    def _read(path, file):
        file_path = os.path.join(path, file)
        if not os.path.isfile(file_path):
            raise DatabaseException("Unable to locate module file %s".format(file_path))

        with open(file_path, encoding='utf-8') as file:
            return json.load(file)

    def write(self, key, data, lib_dir=None, sub_dir='', **kwargs):
        if lib_dir is None:
            lib_dir = self._lib_dir

        file = key + '.json'
        file_dir = os.path.join(lib_dir, sub_dir)
        if not os.path.isdir(file_dir):
            os.makedirs(file_dir)

        self._write(file_dir, file, data)

    @staticmethod
    def _write(path, file, data):
        file_path = os.path.join(path, file)
        if os.path.isfile(file_path):
            return
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(json.dumps(data, separators=(',', ':'), indent=4))

    def clean(self, sub_dir='', lib_dir=None):
        if lib_dir is None:
            lib_dir = self._lib_dir

        data_dir = os.path.join(lib_dir, sub_dir)

        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)

            while os.path.exists(data_dir):
                time.sleep(.1)

        os.makedirs(data_dir)
