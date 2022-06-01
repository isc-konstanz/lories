# -*- coding: utf-8 -*-
"""
    th-e-core.pv.module
    ~~~~~~~~~~~~~~~~~~~~~~~~
    
    
"""
import os
import logging
import pandas as pd
import numpy as np

from pvlib import pvsystem
from collections import OrderedDict
from th_e_core.io.json import JsonDatabase

logger = logging.getLogger(__name__)

SAM_CEC_MODULES_CSV = 'sam-library-cec-modules-2019-03-05.csv'
SAM_CEC_INVERTERS_CSV = 'sam-library-cec-inverters-2019-03-05.csv'


class ModuleDatabase(JsonDatabase):

    def read(self, key: str, sub_dir='', **kwargs):
        return super().read(key, sub_dir=os.path.join('modules', sub_dir))

    def write(self, key, data, lib_dir=None, sub_dir='', **kwargs):
        return super().write(key, data, lib_dir=lib_dir, sub_dir=os.path.join('modules', sub_dir))

    def build(self):
        data = _load_cec(SAM_CEC_MODULES_CSV)
        meta = OrderedDict()

        def find_manufacturer(i):
            arr = data.index[i].split(' ')
            arr_prior = []
            if i > 0:
                arr_prior = np.intersect1d(arr, data.index[i-1].split(' '))
            len_prior = len(arr_prior)

            arr_post = []
            if i < len(data.index)-1:
                arr_post = np.intersect1d(arr, data.index[i+1].split(' '))
            len_post = len(arr_post)

            if len_prior > 0 and len_prior > len_post:
                return ' '.join(arr[:len_prior]).strip()
            elif len_post > 0 and len_post > len_prior:
                return ' '.join(arr[:len_post]).strip()
            else:
                logger.warning("Unable to find manufacturer for {}".format(index))
                return False

        manufacturer = False
        manufacturers = []
        manufacturers_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_cec.txt')
        if os.path.isfile(manufacturers_file):
            with open(manufacturers_file, 'r', encoding="utf-8") as f:
                cont = f.read()
                manufacturers = cont.split('\n')
                manufacturers = list(filter(None, manufacturers))

        for index, module in data.iterrows():
            if index.startswith(tuple(manufacturers)):
                for manufacturer in manufacturers:
                    if index.startswith(manufacturer):
                        break
            else:
                manufacturer = find_manufacturer(data.index.get_loc(index))
            if not manufacturer:
                continue

            if manufacturer not in manufacturers:
                manufacturers.append(manufacturer)
                sorted(manufacturers)
                with open(manufacturers_file, 'w', encoding="utf-8") as f:
                    f.write('\n'.join(manufacturers))
                logger.info("Adding manufacturer: {}".format(manufacturer))

            module_meta, module_data = self._decode_singlediode(manufacturer, index, module)
            module_model = self._encode_str(index[len(manufacturer)+1:].strip())
            manufacturer = self._encode_str(manufacturer)
            if manufacturer not in meta:
                meta[manufacturer] = OrderedDict()
            meta[manufacturer][module_model] = module_meta

            self.write(module_model, module_data, sub_dir='modules')

            logger.debug("Successfully built Module: %s %s",
                         module_meta['Manufacturer'],
                         module_meta['Model Number'])

        super().write('modules', meta)

        logger.info("Complete module library built for %i entries", len(meta))

    @staticmethod
    def _encode_str(string: str) -> str:
        return string.lower().replace(' ', '_').replace('/', '-').replace('&', 'n') \
            .replace(',', '').replace('.', '').replace('!', '').replace('?', '') \
            .replace('(', '').replace(')', '').replace('[', '').replace(']', '')

    @staticmethod
    def _decode_singlediode(manufacturer: str, model: str, module):
        data = OrderedDict()
        data['Date'] = module['Date']
        data['Version'] = module['Version']
        data['Technology'] = module['Technology']
        data['Bifacial'] = module['Bifacial']
        data['BIPV'] = module['BIPV']
        data['STC'] = float(module['STC'])
        data['PTC'] = float(module['PTC'])
        data['A_c'] = float(module['A_c'])
        data['Length'] = float(module['Length'])
        data['Width'] = float(module['Width'])
        data['N_s'] = float(module['N_s'])
        data['I_sc_ref'] = float(module['I_sc_ref'])
        data['V_oc_ref'] = float(module['V_oc_ref'])
        data['I_mp_ref'] = float(module['I_mp_ref'])
        data['V_mp_ref'] = float(module['V_mp_ref'])
        data['alpha_sc'] = float(module['alpha_sc'])
        data['beta_oc'] = float(module['beta_oc'])
        data['T_NOCT'] = float(module['T_NOCT'])
        data['a_ref'] = float(module['a_ref'])
        data['I_L_ref'] = float(module['I_L_ref'])
        data['I_o_ref'] = float(module['I_o_ref'])
        data['R_s'] = float(module['R_s'])
        data['R_sh_ref'] = float(module['R_sh_ref'])
        data['gamma_r'] = float(module['gamma_r'])
        data['Adjust'] = float(module['Adjust'])

        technology = data['Technology']
        if technology == 'Mono-c-Si':
            technology = 'Monocrystalline'
        elif technology == 'Multi-c-Si':
            technology = 'Polycrystalline'

        description = '{}W {} Module'.format(round(data['STC']), technology)

        meta = OrderedDict()
        meta['Model Number'] = model
        meta['Manufacturer'] = manufacturer
        meta['Description'] = description
        meta['Bifacial'] = module['Bifacial'] > 0
        meta['BIPV'] = module['BIPV'] == 'Y'

        return meta, data


class InverterDatabase(JsonDatabase):

    def read(self, key: str, sub_dir='', **kwargs):
        return super().read(key, sub_dir=os.path.join('inverters', sub_dir))

    def write(self, key, data, lib_dir=None, sub_dir='', **kwargs):
        return super().write(key, data, lib_dir=lib_dir, sub_dir=os.path.join('inverters', sub_dir))

    def build(self):
        data = _load_cec(SAM_CEC_INVERTERS_CSV)
        meta = OrderedDict()

        for index, inverter in data.iterrows():
            manufacturer = index.split(':')[0].strip()

            inverter_meta, inverter_data = self._decode_snl(manufacturer, index, inverter)
            inverter_model = self._encode_str(':'.join(index.split(':')[1:]).strip())
            manufacturer = self._encode_str(manufacturer)
            if manufacturer not in meta:
                meta[manufacturer] = OrderedDict()
            meta[manufacturer][inverter_model] = inverter_meta

            self.write(inverter_model, inverter_data, sub_dir=manufacturer)

            logger.debug("Successfully built Inverter: %s %s",
                         inverter_meta['Manufacturer'],
                         inverter_meta['Model Number'])

        super().write('inverters', meta)

        logger.info("Complete inverter library built for %i entries", len(meta))

    @staticmethod
    def _encode_str(string: str) -> str:
        return string.lower().replace(' ', '_').replace('/', '-').replace('&', 'n') \
            .replace(',', '').replace('.', '').replace('!', '').replace('?', '') \
            .replace('(', '').replace(')', '').replace('[', '').replace(']', '')

    @staticmethod
    def _decode_snl(manufacturer: str, model: str, inverter):
        data = OrderedDict()
        data['CEC_Date'] = inverter['CEC_Date']
        data['CEC_Type'] = inverter['CEC_Type']
        data['Vac'] = int(inverter['Vac']) if inverter['Vac'].isnumeric() else inverter['Vac']
        data['Paco'] = float(inverter['Paco'])
        data['Pdco'] = float(inverter['Pdco'])
        data['Vdco'] = float(inverter['Vdco'])
        data['Pso'] = float(inverter['Pso'])
        data['C0'] = float(inverter['C0'])
        data['C1'] = float(inverter['C1'])
        data['C2'] = float(inverter['C2'])
        data['C3'] = float(inverter['C3'])
        data['Pnt'] = float(inverter['Pnt'])
        data['Vdcmax'] = float(inverter['Vdcmax'])
        data['Idcmax'] = float(inverter['Idcmax'])
        data['Mppt_low'] = float(inverter['Mppt_low'])
        data['Mppt_high'] = float(inverter['Mppt_high'])

        inverter_type = data['CEC_Type']
        if inverter_type == 'Utility Interactive':
            inverter_type = 'Interactive Utility'

        voltage = data['Vac']
        power = round(data['Paco'])
        unit = 'W'
        if power > 1000:
            power = ('%.1f' % (power/1000)).replace('.0', '')
            unit = 'kW'

        description = '{}{} {} Inverter'.format(power, unit, inverter_type)
        if isinstance(voltage, str) or voltage > 0:
            description += ' ({}V)'.format(voltage)

        meta = OrderedDict()
        meta['Model Number'] = model
        meta['Manufacturer'] = manufacturer
        meta['Description'] = description

        return meta, data


def _load_cec(file):
    data_path = os.path.join(os.path.dirname(os.path.abspath(pvsystem.__file__)), 'data')
    data_file = os.path.join(data_path, file)
    return pd.read_csv(data_file, index_col=0, skiprows=[1, 2], low_memory=False)
