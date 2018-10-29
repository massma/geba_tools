"""
Tools to load geba meta data (load_geba_meta)
and data databases (load_geba_dataframe)

see:
https://www.ethz.ch/content/specialinterest/usys/iac/geba/en.html
"""
import pandas as pd
import numpy as np


# meta data
def lon_converter(lon_string):
    lon_float = float(lon_string)
    if lon_float > 180.:
        return lon_float - 360.
    return lon_float

def load_geba_meta(filename):
    """
    loads a geba meta data file into a pandas dataframe, e.g.
    https://iacweb.ethz.ch/data/geba/status_01.txt
    """
    data = [['id', 'name', 'cc', 'lat', 'lon', 'elev', 'start_year',
             'end_year', 'num_month', 'num_inst_change', 'flag_months_1',
             'flag_months_2', 'flag_months_3']]
    geba_meta_start = [0, 5, 67, 70, 78, 86, 91,
                       98, 103, 108, 110, 115, 120]
    geba_meta_end = [4, 66, 69, 77, 85, 90, 95,
                     102, 107, 110, 115, 120, 125]
    converters = [int, str, str, float, lon_converter, float, int,
                  int, int, int, int, int, int]
    f = open(filename, 'r')
    for line in f:
        if str(line[:4]).strip().isnumeric():
            data.append([f(str(line[start:end]).strip()) for
                         start, end, f in zip(geba_meta_start,
                                              geba_meta_end, converters)])
    f.close()
    return pd.DataFrame(data[1:], columns=data[0])


# full db
VAR_NAME = {1 : 'sw_down', 2 : 'sw_direct', 3 : 'snw_diffuse', 4 :
            'albedo', 5 : 'sw_up', 6 : 'lw_down', 7 : 'lw_up', 8 : 'lw_net', 9 :
            'rad_net', 10 : 'sensible', 11 : 'latent', 12 : 'ground', 13 :
            'latent_melt', 14 : 'uv', 15 : 'other', 16 : 'absorbed', 17 :
            'rad_up', 18 : 'turbulent_up', 19 : 'circumglobal'}

def id_loader(line):
    return int(str(line[0:4]).strip())

def type_loader(line_num):
    if (line_num % 2) == 0:
        return 'value'
    return 'flag'

def var_loader(line):
    return VAR_NAME[int(str(line[5:7]).strip())]

def year_loader(line):
    return int(str(line[8:12].strip()))

def geba_string_to_num(string):
    num = float(string)
    if num == 99999:
        return np.nan
    return num

def geba_string_to_flag(string):
    if string == '-------':
        return np.nan
    return int(string)

def month_loader(line_num, line, month):
    start_idx = 13+month*8
    end_idx = 20+month*8
    if (line_num % 2) == 0:
        return geba_string_to_num(str(line[start_idx:
                                           end_idx]))
    return geba_string_to_flag(str(line[start_idx:
                                        end_idx]))

def load_geba_dataframe(filename):
    """
    loads the full geba database, as downloaded from
    https://www.ethz.ch/content/specialinterest/usys/iac/geba/en/data-retrieval/database-access.html
    """
    data = [['id', 'flag_or_value', 'type', 'year', 'month', 'value']]
    f = open(filename, 'r')
    for i, line in enumerate(f):
        new_data = [id_loader(line), type_loader(i), var_loader(line),
                    year_loader(line), 1, month_loader(i, line, 0)]
        data.append(new_data)
        for month in range(1,12):
            new_data = new_data[:-2]
            new_data.extend([month+1, month_loader(i, line, month)])
            data.append(new_data)
    f.close()
    return pd.DataFrame(data[1:], columns=data[0])
