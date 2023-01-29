from pandas import read_csv
from os.path import join, dirname

BASE = dirname(__file__)

def _loader(filename):
    def closure():
        return read_csv(join(BASE, 'csv', f'{filename}.csv.gz'), index_col='DATE &TIME', parse_dates=True, infer_datetime_format=True, compression='gzip')
    return closure


merged = _loader('all')
bhe = _loader('bhe')
cooling_loop = _loader('cooling_loop')
cooling_pump = _loader('cooling_pump')
heating_loop = _loader('heating_loop')
heating_pump = _loader('heating_pump')
