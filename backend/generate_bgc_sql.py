
import os
import pandas as pd
import xarray as xr
import numpy as np
import httpx
import asyncio
from typing import List, Dict

# Paths
BIO_INDEX_PATH = 'argo_bio-profile_index.txt'
DOWNLOADS_DIR = 'downloads'
SQL_OUTPUT_PATH = 'bgc_ingestion.sql'
GDAC_DAC_URL = 'https://data-argo.ifremer.fr/dac/'

# CONFIGURATION: Set to None to process ALL profiles, or a number for testing
PROCESS_LIMIT = 10 

# Ensure downloads directory exists
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

# Mapping from NetCDF Variable to SQL Column
# Format: { 'SQL_COL': ('NetCDF_Var', 'QC_Var', 'Adj_Var', 'Adj_QC_Var') }
MAPPING = {
    'PRESSURE': ('PRES', 'PRES_QC', 'PRES_ADJUSTED', 'PRES_ADJUSTED_QC'),
    'DOXY': ('DOXY', 'DOXY_QC', 'DOXY_ADJUSTED', 'DOXY_ADJUSTED_QC'),
    'CHLA': ('CHLA', 'CHLA_QC', 'CHLA_ADJUSTED', 'CHLA_ADJUSTED_QC'),
    'NITRATE': ('NITRATE', 'NITRATE_QC', 'NITRATE_ADJUSTED', 'NITRATE_ADJUSTED_QC'),
    'BBP700': ('BBP700', 'BBP700_QC', 'BBP700_ADJUSTED', 'BBP700_ADJUSTED_QC'),
    'PH_IN_SITU_TOTAL': ('PH_IN_SITU_TOTAL', 'PH_IN_SITU_TOTAL_QC', 'PH_IN_SITU_TOTAL_ADJUSTED', 'PH_IN_SITU_TOTAL_ADJUSTED_QC'),
    'BBP': ('BBP', 'BBP_QC', 'BBP_ADJUSTED', 'BBP_ADJUSTED_QC'),
    'BBP470': ('BBP470', 'BBP470_QC', 'BBP470_ADJUSTED', 'BBP470_ADJUSTED_QC'),
    'BBP532': ('BBP532', 'BBP532_QC', 'BBP532_ADJUSTED', 'BBP532_ADJUSTED_QC'),
    'TURBIDITY': ('TURBIDITY', 'TURBIDITY_QC', None, None), # Turbidity often lacks adj
    'CP': ('CP', 'CP_QC', 'CP_ADJUSTED', 'CP_ADJUSTED_QC'),
    'CP660': ('CP660', 'CP660_QC', 'CP660_ADJUSTED', 'CP660_ADJUSTED_QC'),
    'CDOM': ('CDOM', 'CDOM_QC', 'CDOM_ADJUSTED', 'CDOM_ADJUSTED_QC'),
    'BISULFIDE': ('BISULFIDE', 'BISULFIDE_QC', 'BISULFIDE_ADJUSTED', 'BISULFIDE_ADJUSTED_QC'),
    'DOWN_IRRADIANCE': ('DOWN_IRRADIANCE', 'DOWN_IRRADIANCE_FLAG', 'DOWN_IRRADIANCE_ADJ', 'DOWN_IRRADIANCE_ADJ_FLAG'), # Generic
    'DOWN_IRRADIANCE380': ('DOWN_IRRADIANCE380', 'DOWN_IRRADIANCE380_QC', 'DOWN_IRRADIANCE380_ADJUSTED', 'DOWN_IRRADIANCE380_ADJUSTED_QC'),
    'DOWN_IRRADIANCE412': ('DOWN_IRRADIANCE412', 'DOWN_IRRADIANCE412_QC', 'DOWN_IRRADIANCE412_ADJUSTED', 'DOWN_IRRADIANCE412_ADJUSTED_QC'),
    'DOWN_IRRADIANCE443': ('DOWN_IRRADIANCE443', 'DOWN_IRRADIANCE443_QC', 'DOWN_IRRADIANCE443_ADJUSTED', 'DOWN_IRRADIANCE443_ADJUSTED_QC'),
    'DOWN_IRRADIANCE490': ('DOWN_IRRADIANCE490', 'DOWN_IRRADIANCE490_QC', 'DOWN_IRRADIANCE490_ADJUSTED', 'DOWN_IRRADIANCE490_ADJUSTED_QC'),
    'DOWN_IRRADIANCE555': ('DOWN_IRRADIANCE555', 'DOWN_IRRADIANCE555_QC', 'DOWN_IRRADIANCE555_ADJUSTED', 'DOWN_IRRADIANCE555_ADJUSTED_QC'),
    'UP_RADIANCE': ('UP_RADIANCE', 'UP_RADIANCE_QC', 'UP_RADIANCE_ADJUSTED', 'UP_RADIANCE_ADJUSTED_QC'),
    'UP_RADIANCE412': ('UP_RADIANCE412', 'UP_RADIANCE412_QC', 'UP_RADIANCE412_ADJUSTED', 'UP_RADIANCE412_ADJUSTED_QC'),
    'UP_RADIANCE443': ('UP_RADIANCE443', 'UP_RADIANCE443_QC', 'UP_RADIANCE443_ADJUSTED', 'UP_RADIANCE443_ADJUSTED_QC'),
    'UP_RADIANCE555': ('UP_RADIANCE555', 'UP_RADIANCE555_QC', 'UP_RADIANCE555_ADJUSTED', 'UP_RADIANCE555_ADJUSTED_QC'),
    'DOWNWELLING_PAR': ('DOWNWELLING_PAR', 'DOWNWELLING_PAR_QC', 'DOWNWELLING_PAR_ADJUSTED', 'DOWNWELLING_PAR_ADJUSTED_QC'),
}

# Variable list for extraction (including flags)
SQL_COLUMNS = [
    'ARGO_OBSERVATION_ID', 'PRESSURE', 'PRESSURE_FLAG', 'PRESSURE_ADJ', 'PRESSURE_ADJ_FLAG',
    'DOXY', 'DOXY_FLAG', 'DOXY_ADJ', 'DOXY_ADJ_FLAG',
    'BBP', 'BBP_FLAG', 'BBP_ADJ', 'BBP_ADJ_FLAG',
    'BBP470', 'BBP470_FLAG', 'BBP470_ADJ', 'BBP470_ADJ_FLAG',
    'BBP532', 'BBP532_FLAG', 'BBP532_ADJ', 'BBP532_ADJ_FLAG',
    'BBP700', 'BBP700_FLAG', 'BBP700_ADJ', 'BBP700_ADJ_FLAG',
    'TURBIDITY', 'CP', 'CP_FLAG', 'CP_ADJ', 'CP_ADJ_FLAG',
    'CP660', 'CP660_FLAG', 'CP660_ADJ', 'CP660_ADJ_FLAG',
    'CHLA', 'CHLA_FLAG', 'CHLA_ADJ', 'CHLA_ADJ_FLAG',
    'CDOM', 'CDOM_FLAG', 'CDOM_ADJ', 'CDOM_ADJ_FLAG',
    'NITRATE', 'NITRATE_FLAG', 'NITRATE_ADJ', 'NITRATE_ADJ_FLAG',
    'BISULFIDE', 'BISULFIDE_FLAG', 'BISULFIDE_ADJ', 'BISULFIDE_ADJ_FLAG',
    'PH_IN_SITU_TOTAL', 'PH_IN_SITU_TOTAL_FLAG', 'PH_IN_SITU_TOTAL_ADJ', 'PH_IN_SITU_TOTAL_ADJ_FLAG',
    'DOWN_IRRADIANCE', 'DOWN_IRRADIANCE_FLAG', 'DOWN_IRRADIANCE_ADJ', 'DOWN_IRRADIANCE_ADJ_FLAG',
    'DOWN_IRRADIANCE380', 'DOWN_IRRADIANCE380_FLAG', 'DOWN_IRRADIANCE380_ADJ', 'DOWN_IRRADIANCE380_ADJ_FLAG',
    'DOWN_IRRADIANCE412', 'DOWN_IRRADIANCE412_FLAG', 'DOWN_IRRADIANCE412_ADJ', 'DOWN_IRRADIANCE412_ADJ_FLAG',
    'DOWN_IRRADIANCE443', 'DOWN_IRRADIANCE443_FLAG', 'DOWN_IRRADIANCE443_ADJ', 'DOWN_IRRADIANCE443_ADJ_FLAG',
    'DOWN_IRRADIANCE490', 'DOWN_IRRADIANCE490_FLAG', 'DOWN_IRRADIANCE490_ADJ', 'DOWN_IRRADIANCE490_ADJ_FLAG',
    'DOWN_IRRADIANCE555', 'DOWN_IRRADIANCE555_FLAG', 'DOWN_IRRADIANCE555_ADJ', 'DOWN_IRRADIANCE555_ADJ_FLAG',
    'UP_RADIANCE', 'UP_RADIANCE_FLAG', 'UP_RADIANCE_ADJ', 'UP_RADIANCE_ADJ_FLAG',
    'UP_RADIANCE412', 'UP_RADIANCE412_FLAG', 'UP_RADIANCE412_ADJ', 'UP_RADIANCE412_ADJ_FLAG',
    'UP_RADIANCE443', 'UP_RADIANCE443_FLAG', 'UP_RADIANCE443_ADJ', 'UP_RADIANCE443_ADJ_FLAG',
    'UP_RADIANCE555', 'UP_RADIANCE555_FLAG', 'UP_RADIANCE555_ADJ', 'UP_RADIANCE555_ADJ_FLAG',
    'DOWNWELLING_PAR', 'DOWNWELLING_PAR_FLAG', 'DOWNWELLING_PAR_ADJ_FLAG', 'DOWNWELLING_PAR_ADJ'
]

async def download_file(file_rel_path: str):
    local_path = os.path.join(DOWNLOADS_DIR, os.path.basename(file_rel_path))
    if os.path.exists(local_path):
        return local_path
    
    url = f"{GDAC_DAC_URL}{file_rel_path}"
    print(f"Downloading {url}...")
    async with httpx.AsyncClient(timeout=60) as client:
        try:
            resp = await client.get(url)
            if resp.status_code == 200:
                with open(local_path, 'wb') as f:
                    f.write(resp.content)
                return local_path
        except Exception as e:
            print(f"Download failed: {e}")
    return None

def get_val(data, idx, level):
    if data is None: return "NULL"
    try:
        val = data[idx, level]
        if np.isnan(val): return "NULL"
        return str(float(val))
    except: return "NULL"

def get_flag(data, idx, level):
    if data is None: return "NULL"
    try:
        val = data[idx, level]
        if isinstance(val, (bytes, np.bytes_)):
            val = val.decode('utf-8')
        val = str(val).strip()
        if not val or val == 'nan': return "NULL"
        return f"'{val}'"
    except: return "NULL"

async def process_file(file_rel_path: str, sql_file):
    local_path = await download_file(file_rel_path)
    if not local_path: return
    
    try:
        ds = xr.open_dataset(local_path)
        if 'PRES' not in ds:
            ds.close()
            return
            
        n_prof = ds.dims.get('N_PROF', 0)
        n_levels = ds.dims.get('N_LEVELS', 0)
        
        platform = str(ds.PLATFORM_NUMBER.values[0].decode('utf-8')).strip()
        
        # Extract variables
        vars_data = {}
        for sql_base, nc_vars in MAPPING.items():
            v_main, v_qc, v_adj, v_adj_qc = nc_vars
            vars_data[sql_base] = ds[v_main].values if v_main in ds else None
            vars_data[f"{sql_base}_FLAG"] = ds[v_qc].values if v_qc in ds else None
            vars_data[f"{sql_base}_ADJ"] = ds[v_adj].values if v_adj in ds else None
            vars_data[f"{sql_base}_ADJ_FLAG"] = ds[v_adj_qc].values if v_adj_qc in ds else None

        for p in range(n_prof):
            cycle = str(ds.CYCLE_NUMBER.values[p])
            obs_id = f"{platform}_{cycle}_{p}"
            
            for l in range(n_levels):
                pres_val = get_val(vars_data['PRESSURE'], p, l)
                if pres_val == "NULL": continue
                
                row_vals = [f"'{obs_id}'"]
                row_vals.append(pres_val)
                row_vals.append(get_flag(vars_data['PRESSURE_FLAG'], p, l))
                row_vals.append(get_val(vars_data['PRESSURE_ADJ'], p, l))
                row_vals.append(get_flag(vars_data['PRESSURE_ADJ_FLAG'], p, l))
                
                # Scientific Vars
                for base in ['DOXY', 'BBP', 'BBP470', 'BBP532', 'BBP700', 'TURBIDITY', 'CP', 'CP660', 'CHLA', 'CDOM', 'NITRATE', 'BISULFIDE', 'PH_IN_SITU_TOTAL', 
                             'DOWN_IRRADIANCE', 'DOWN_IRRADIANCE380', 'DOWN_IRRADIANCE412', 'DOWN_IRRADIANCE443', 
                             'DOWN_IRRADIANCE490', 'DOWN_IRRADIANCE555', 'UP_RADIANCE', 'UP_RADIANCE412', 
                             'UP_RADIANCE443', 'UP_RADIANCE555', 'DOWNWELLING_PAR']:
                    row_vals.append(get_val(vars_data[base], p, l))
                    if base == 'TURBIDITY': continue # Only 1 col for turbidity usually
                    row_vals.append(get_flag(vars_data[f"{base}_FLAG"], p, l))
                    row_vals.append(get_val(vars_data[f"{base}_ADJ"], p, l))
                    row_vals.append(get_flag(vars_data[f"{base}_ADJ_FLAG"], p, l))
                
                sql = f"INSERT IGNORE INTO ARGO.AOD_ARGO_OTHER_DETAILS ({', '.join(SQL_COLUMNS)}) VALUES ({', '.join(row_vals)});\n"
                sql_file.write(sql)
        
        ds.close()
    except Exception as e:
        print(f"Error processing {local_path}: {e}")

async def main():
    print(f"Reading {BIO_INDEX_PATH}...")
    # Read index manually to handle comments
    with open(BIO_INDEX_PATH, 'r') as f:
        lines = f.readlines()
    
    header_idx = -1
    for i, line in enumerate(lines):
        if line.startswith('file,'):
            header_idx = i
            break
    
    if header_idx == -1:
        print("Header not found in index file.")
        return
        
    cols = lines[header_idx].strip().split(',')
    df = pd.DataFrame([l.strip().split(',') for l in lines[header_idx+1:]], columns=cols)
    
    # Filter for Indian Ocean
    indian_ocean_df = df[df['ocean'] == 'I']
    total = len(indian_ocean_df)
    print(f"Found {total} profiles in Indian Ocean.")
    
    # Apply limit if set
    if PROCESS_LIMIT is not None:
        to_process = indian_ocean_df.head(PROCESS_LIMIT)
    else:
        to_process = indian_ocean_df
    
    with open(SQL_OUTPUT_PATH, 'w') as sql_file:
        sql_file.write("-- BGC Argo Data Ingestion\n")
        sql_file.write("SET SQL_MODE='ALLOW_INVALID_DATES';\n\n")
        
        for i, (idx, row) in enumerate(to_process.iterrows()):
            print(f"[{i+1}/{len(to_process)}] Processing {row['file']}...")
            await process_file(row['file'], sql_file)
            
    print(f"SQL file generated: {SQL_OUTPUT_PATH}")

if __name__ == "__main__":
    asyncio.run(main())
