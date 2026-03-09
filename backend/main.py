from fastapi import FastAPI, HTTPException, Body, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import httpx
import pandas as pd
import numpy as np
import io
import os
import bisect
import json
import re
from typing import List, Optional
from pydantic import BaseModel
import xarray as xr

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
LOCAL_INDEX_PATH = 'ar_index_global_prof.txt'
REMOTE_INDEX_URL = 'https://data-argo.ifremer.fr/ar_index_global_prof.txt'
DOWNLOADS_DIR = 'downloads'
BIO_INDEX_PATH = 'argo_bio-profile_index.txt'
# Ensure downloads directory exists
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

#Cached Core and Bio Profiles
CACHED_PROFILES_BIO = []
DATE_SORTED_PROFILES_BIO = []
CACHED_PROFILES_CORE = []
DATE_SORTED_PROFILES_CORE = []


class SearchParams(BaseModel):
    startDate: str
    endDate: str
    minDepth: float
    maxDepth: float
    type: str # 'core' or 'bio'

class Bounds(BaseModel):
    north: float
    south: float
    east: float
    west: float

class ProcessRequest(BaseModel):
    bounds: Bounds
    params: SearchParams
async def load_bio_index():
    "loads the index file from the bio link and sorts it for binary search"
    global CACHED_PROFILES_BIO, DATE_SORTED_PROFILES_BIO

    if CACHED_PROFILES_BIO:
        return
    #Check local first for bio.txt
    content= ""
    if os.path.exists(BIO_INDEX_PATH):
        with open(BIO_INDEX_PATH, 'r', encoding='utf-8') as f:
            content = f.read()
    else:
        async with httpx.AsyncClient() as client:
            resp = await client.get('https://data-argo.ifremer.fr/dac/argo_bio-profile_index.txt', timeout=600.0)
            content = resp.text

    # Parse the bio index content (10-column format: file,date,lat,lon,ocean,profiler_type,institution,parameters,parameter_data_mode,date_update)
    lines = [line for line in content.splitlines() if not line.startswith('#') and 'file,' not in line]

    data = []
    for line in lines:
        parts = line.split(',')
        if len(parts) >= 7:
            try:
                data.append({
                    'file': parts[0],
                    'date': parts[1],
                    'lat': float(parts[2]),
                    'lon': float(parts[3]),
                    'ocean': parts[4],
                    'profiler_type': parts[5],
                    'institution': parts[6],
                    'date_update': parts[-1] if len(parts) >= 10 else parts[7] if len(parts) >= 8 else ''
                })
            except ValueError:
                continue

    CACHED_PROFILES_BIO = data
    DATE_SORTED_PROFILES_BIO = sorted(data, key=lambda x: x['date'])
    print(f'Loaded {len(CACHED_PROFILES_BIO)} bio profiles')

async def load_index():
    """Loads the core index file into memory and sorts it for binary search."""
    global CACHED_PROFILES_CORE, DATE_SORTED_PROFILES_CORE

    if CACHED_PROFILES_CORE:
        return

    content = ""

    if os.path.exists(LOCAL_INDEX_PATH):
        with open(LOCAL_INDEX_PATH, 'r', encoding='utf-8') as f:
            content = f.read()
    else:
        async with httpx.AsyncClient() as client:
            resp = await client.get(REMOTE_INDEX_URL, timeout=600.0)
            content = resp.text

    lines = [line for line in content.splitlines() if not line.startswith('#') and 'file,' not in line]

    data = []

    for line in lines:
        parts = line.split(',')

        if len(parts) >= 8:
            try:
                data.append({
                    'file': parts[0],
                    'date': parts[1],
                    'lat': float(parts[2]),
                    'lon': float(parts[3]),
                    'ocean': parts[4],
                    'profiler_type': parts[5],
                    'institution': parts[6],
                    'date_update': parts[7]
                })
            except ValueError:
                continue

    CACHED_PROFILES_CORE = data
    DATE_SORTED_PROFILES_CORE = sorted(data, key=lambda x: x['date'])
from datetime import datetime

@app.on_event("startup")
async def startup_event():
    await load_index()
    await load_bio_index()

def binary_search_date_range(start_date, end_date, dataset='core'):

    start_str = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y%m%d") + "000000"
    end_str = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m%d") + "235959"

    if dataset == 'bio':
        profiles = DATE_SORTED_PROFILES_BIO
    else:
        profiles = DATE_SORTED_PROFILES_CORE

    dates = [x['date'] for x in profiles]

    left_idx = bisect.bisect_left(dates, start_str)
    right_idx = bisect.bisect_right(dates, end_str)

    return profiles[left_idx:right_idx]
async def download_bio_netcdf(file_path):
    "Download Bio NetCDF file and saves it to the downloads directory."
    url=f"https://data-argo.ifremer.fr/dac/{file_path}"
    filename=os.path.basename(file_path)
    local_path=os.path.join(DOWNLOADS_DIR, filename)
    if os.path.exists(local_path):
        return local_path
    async with httpx.AsyncClient() as client:
        resp=await client.get(url, timeout=60.0)
        resp.raise_for_status()
        with open(local_path, 'wb') as f:
            f.write(resp.content)
        return local_path
    
async def download_netcdf(file_path):
    """Downloads NetCDF file and saves it to the downloads directory."""
    url = f"https://data-argo.ifremer.fr/dac/{file_path}"
    filename = os.path.basename(file_path)
    local_path = os.path.join(DOWNLOADS_DIR, filename)
    
    if os.path.exists(local_path):
        return local_path
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, timeout=60.0)
        resp.raise_for_status()
        with open(local_path, 'wb') as f:
            f.write(resp.content)
        return local_path

def process_netcdf(file_path, params):
    """Extracts ALL data from NetCDF file using xarray with High Accuracy."""
    try:
        ds = xr.open_dataset(file_path)
        data = []
        if 'PRES' not in ds:
            ds.close()
            return []
        pres = ds['PRES'].values
        
        # Extract ALL variables from NetCDF file
        vars_to_extract = {}
        flags_to_extract = {}
        
        # Get measurement variables that share PRES dimensions (N_PROF, N_LEVELS)
        # This excludes per-profile metadata (CYCLE_NUMBER, LATITUDE, etc.) and
        # history variables that have different dimensions
        pres_dims = ds['PRES'].dims  # e.g. ('N_PROF', 'N_LEVELS')
        for var_name in ds.data_vars:
            if var_name not in ['PRES'] and not var_name.endswith('_QC'):
                try:
                    var = ds[var_name]
                    var_data = var.values
                    # Only include numeric variables with same dims as PRES
                    if (var_data.size > 0 and 
                        var_data.dtype.kind in ('f', 'i', 'u') and
                        var.dims == pres_dims):
                        vars_to_extract[var_name] = var_data
                        qc_key = f"{var_name}_QC"
                        if qc_key in ds:
                            qc_data = ds[qc_key].values
                            if qc_data.size > 0:
                                flags_to_extract[qc_key] = qc_data
                except Exception:
                    continue  # Skip problematic variables
        
        # Always include PRES
        vars_to_extract['PRES'] = pres
        if 'PRES_QC' in ds:
            try:
                pres_qc = ds['PRES_QC'].values
                if pres_qc.size > 0:
                    flags_to_extract['PRES_QC'] = pres_qc
            except Exception:
                pass

        # Handle different array dimensions safely
        if pres.ndim == 1:
            # 1D case - single profile
            n_levels = pres.shape[0]
            for l in range(n_levels):
                try:
                    p_val = pres[l]
                    if np.isnan(p_val): continue
                    
                    depth = float(p_val)
                    if params.minDepth <= depth <= params.maxDepth:
                        row = {'depth': depth}
                        for vname, vdata in vars_to_extract.items():
                            try:
                                if vdata.ndim == 1 and l < len(vdata):
                                    val = vdata[l]
                                elif vdata.ndim == 2 and vdata.shape[1] > l:
                                    val = vdata[0, l]
                                else:
                                    val = np.nan
                                row[vname] = float(val) if not np.isnan(val) else ''
                            except (IndexError, ValueError, TypeError):
                                row[vname] = ''
                        
                        for qname, qdata in flags_to_extract.items():
                            try:
                                if qdata.ndim == 1 and l < len(qdata):
                                    val = qdata[l]
                                elif qdata.ndim == 2 and qdata.shape[1] > l:
                                    val = qdata[0, l]
                                else:
                                    val = ''
                                if isinstance(val, (bytes, np.bytes_)):
                                    row[qname] = val.decode('utf-8')
                                else:
                                    row[qname] = str(val) if val != '' else ''
                            except (IndexError, ValueError, UnicodeDecodeError):
                                row[qname] = ''
                        data.append(row)
                except Exception:
                    continue
                    
        elif pres.ndim == 2:
            # 2D case - multiple profiles
            n_prof, n_levels = pres.shape
            for p in range(n_prof):
                for l in range(n_levels):
                    try:
                        p_val = pres[p, l]
                        if np.isnan(p_val): continue
                        
                        depth = float(p_val)
                        if params.minDepth <= depth <= params.maxDepth:
                            row = {'depth': depth}
                            for vname, vdata in vars_to_extract.items():
                                try:
                                    if vdata.ndim >= 2 and p < vdata.shape[0] and l < vdata.shape[1]:
                                        val = vdata[p, l]
                                    elif vdata.ndim == 1 and p < len(vdata):
                                        val = vdata[p]
                                    else:
                                        val = np.nan
                                    row[vname] = float(val) if not np.isnan(val) else ''
                                except (IndexError, ValueError, TypeError):
                                    row[vname] = ''
                            
                            for qname, qdata in flags_to_extract.items():
                                try:
                                    if qdata.ndim >= 2 and p < qdata.shape[0] and l < qdata.shape[1]:
                                        val = qdata[p, l]
                                    elif qdata.ndim == 1 and p < len(qdata):
                                        val = qdata[p]
                                    else:
                                        val = ''
                                    if isinstance(val, (bytes, np.bytes_)):
                                        row[qname] = val.decode('utf-8')
                                    else:
                                        row[qname] = str(val) if val != '' else ''
                                except (IndexError, ValueError, UnicodeDecodeError):
                                    row[qname] = ''
                            data.append(row)
                    except Exception:
                        continue
        
        ds.close()
        return data
    except Exception as e:
        print(f"Error processing NetCDF: {e}")
        return []

def extract_metadata(filename):
    """Extracts Platform and Cycle from filename e.g. R1901839_334.nc"""
    match = re.search(r'([A-Z]*)([0-9]+)_([0-9]+D?)', filename)
    if match:
        return match.group(2), match.group(3)
    return "", ""

@app.websocket("/api/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        data = await websocket.receive_text()
        req_dict = json.loads(data)
        
        bounds = req_dict['bounds']
        params = req_dict['params']
        
        # Ensure numeric types are properly converted (they arrive as strings from JSON)
        params['minDepth'] = float(params.get('minDepth', 0))
        params['maxDepth'] = float(params.get('maxDepth', 2000))
        
        # Params object wrapper for helper functions
        class ParamsObj:
            def __init__(self, d): self.__dict__ = d
        params_obj = ParamsObj(params)

        await websocket.send_json({"type": "log", "message": "Initializing Search..."})
        #await load_index()
        
        # 1. Date Filter
        candidates = binary_search_date_range(
            params['startDate'], 
            params['endDate'],
            params['type'])

        await websocket.send_json({"type": "log", "message": f"Found {len(candidates)} profiles in date range."})
        
        # 2. Geo Filter
        filtered = [
            p for p in candidates
            if bounds['south'] <= p['lat'] <= bounds['north'] and
               bounds['west'] <= p['lon'] <= bounds['east']
        ]
        await websocket.send_json({"type": "log", "message": f"Geographic filter reduced to {len(filtered)} profiles."})
        
        if not filtered:
            await websocket.send_json({"type": "error", "message": "No profiles found in selected area/date."})
            return

        selection = filtered
        all_results = []
        
        for i, profile in enumerate(selection):
            try:
                msg = f"[{i+1}/{len(selection)}] Processing {profile['file']}..."
                await websocket.send_json({"type": "log", "message": msg})
                
                #local_path = await download_netcdf(profile['file'])
                if params['type']=='bio':
                    local_path=await download_bio_netcdf(profile['file'])
                else:
                    local_path=await download_netcdf(profile['file'])

                extracted = process_netcdf(local_path, params_obj)
                
                filename = os.path.basename(profile['file'])
                platform, cycle = extract_metadata(filename)
                
                for row in extracted:
                    row.update({
                        'Date': profile['date'],
                        'Latitude': profile['lat'],
                        'Longitude': profile['lon'],
                        'Platform': platform,
                        'Cycle': cycle,
                        'Institution': profile.get('institution', ''),
                        'Ocean': profile.get('ocean', ''),
                        'File': filename
                    })
                    all_results.append(row)
            except Exception as e:
                await websocket.send_json({"type": "log", "message": f"Error on {profile['file']}: {str(e)}"})
        
        if not all_results:
            await websocket.send_json({"type": "error", "message": "No data extracted from profiles."})
            return
            
        await websocket.send_json({"type": "log", "message": "Generating Excel-compatible CSV..."})
        
        df = pd.DataFrame(all_results)
        
        # Proper Column Ordering with enhanced metadata
        if params.get('type') == 'bio':
            # Priority order for BGC parameters as shown in UI
            bgc_priority = [
                'CHLA', 'CHLA_ADJUSTED', 
                'DOXY', 'DOXY_ADJUSTED', 
                'NITRATE', 'NITRATE_ADJUSTED', 
                'PH', 'PH_ADJUSTED', 
                'BBP700', 'BBP700_ADJUSTED', 
                'IRRADIANCE', 'IRRADIANCE_ADJUSTED',
                'TEMP', 'TEMP_ADJUSTED',
                'PSAL', 'PSAL_ADJUSTED',
                'PRES', 'PRES_ADJUSTED'
            ]
            first_cols = ['Platform', 'Cycle', 'Date', 'Latitude', 'Longitude', 'depth']
            
            # Identify which priority cols actually exist
            available_priority = [c for c in bgc_priority if c in df.columns]
            
            # Other numeric columns not in priority list
            other_param_cols = [c for c in df.columns if c not in first_cols and c not in available_priority and 'QC' not in c and c not in ['File', 'Institution', 'Ocean']]
            
            qc_cols = [c for c in df.columns if 'QC' in c]
            meta_cols = ['Institution', 'Ocean', 'File']
            
            final_cols = first_cols + available_priority + other_param_cols + qc_cols + meta_cols
        else:
            # Standard Core Argo ordering
            first_cols = ['Platform', 'Cycle', 'Date', 'Latitude', 'Longitude', 'depth']
            param_cols = [c for c in df.columns if c not in first_cols and 'QC' not in c and c not in ['File', 'Institution', 'Ocean']]
            qc_cols = [c for c in df.columns if 'QC' in c]
            meta_cols = ['Institution', 'Ocean', 'File']
            final_cols = first_cols + param_cols + qc_cols + meta_cols
        
        # Handle missing cols if any
        existing_cols = [c for c in final_cols if c in df.columns]
        df = df[existing_cols]
        
        # Drop columns that are entirely empty (all blank or NaN)
        df.replace('', np.nan, inplace=True)
        df.dropna(axis=1, how='all', inplace=True)
        
        # Rename for niceness
        df.rename(columns={'depth': 'Depth (dbar)'}, inplace=True)
            
        # CSV String with BOM for Excel
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False) # standard encoding
        
        # Combine BOM + CSV string
        csv_content = csv_buffer.getvalue()
        
        await websocket.send_json({
            "type": "complete", 
            "csv": csv_content,
            "filename": f"argo_complete_dataset_{params['type']}_{len(selection)}_profiles.csv"
        })
        
    except WebSocketDisconnect:
        print("Client disconnected")
    except Exception as e:
        print(f"WS Error: {e}")
        try:
            await websocket.send_json({"type": "error", "message": str(e)})
        except:
            pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
