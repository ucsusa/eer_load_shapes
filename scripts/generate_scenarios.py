import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sb
from glob import glob
import os
from os import listdir
from pathlib import Path
import us
from tqdm import trange, tqdm
import h5py
import datetime
import re

def write_profile_to_h5(df, filename, outfolder, compression_opts=4):
    """Writes dataframe to h5py file format used by ReEDS. Used in ReEDS and hourlize

    This function takes a pandas dataframe and saves to a h5py file. Data is saved to h5 file as follows:
        - the data itself is saved to a dataset named "data"
        - column names are saved to a dataset named "columns"
        - the index of the data is saved to a dataset named "index"; in the case of a multindex,
          each index is saved to a separate dataset with the format "index_{index order}"
        - the names of the index (or multindex) are saved to a dataset named "index_names"

    Parameters
    ----------
    df
        pandas dataframe to save to h5
    filename
        Name of h5 file
    outfolder
        Path to folder to save the file (in ReEDS this is usually the inputs_case folder)

    Returns
    -------
    None
    """
    outfile = os.path.join(outfolder, filename)
    with h5py.File(outfile, 'w') as f:
        # save index or multi-index in the format 'index_{index order}')
        for i in range(df.index.nlevels):
            # get values for specified index level
            indexvals = df.index.get_level_values(i)
            # save index
            if isinstance(indexvals[0], bytes):
                # if already formatted as bytes keep that way
                f.create_dataset(f'index_{i}', data=indexvals, dtype='S30')
            elif indexvals.name in ['datetime', 'weather_datetime']:
                # if we have a formatted datetime index that isn't bytes, save as such
                try:
                    timeindex = (
                        indexvals.to_series().apply(datetime.datetime.isoformat).reset_index(drop=True)
                    )
                    f.create_dataset(f'index_{i}', data=timeindex.str.encode('utf-8'), dtype='S30')
                except TypeError:
                    timeindex = (
                        pd.to_datetime(indexvals\
                                       .to_series())\
                                        .apply(datetime.datetime.isoformat)\
                                            .reset_index(drop=True)
                    )
                    f.create_dataset(f'index_{i}', data=timeindex.str.encode('utf-8'), dtype='S30')
            else:
                # Other indices can be saved using their data type
                f.create_dataset(f'index_{i}', data=indexvals, dtype=indexvals.dtype)

        # save index names
        index_names = pd.Index(df.index.names)
        if len(index_names):
            f.create_dataset(
                'index_names', data=index_names, dtype=f'S{index_names.map(len).max()}'
            )

        # save column names as string type
        if len(df.columns):
            f.create_dataset('columns', data=df.columns, dtype=f'S{df.columns.map(len).max()}')

        # save data if it exists
        if df.empty:
            pass
        elif len(df.dtypes.unique()) == 1:
            dtype = df.dtypes.unique()[0]
            f.create_dataset(
                'data',
                data=df.values,
                dtype=dtype,
                compression='gzip',
                compression_opts=compression_opts,
            )
        else:
            types = df.dtypes.unique()
            print(df)
            raise ValueError(f"{outfile} can only contain one datatype but it contains {types}")

        return df


def read_h5py_file(filename):
    """Return dataframe object for a h5py file.

    This function returns a pandas dataframe of a h5py file. If the file has multiple dataset on it
    means it has yearly index.

    Parameters
    ----------
    filename
        File path to read

    Returns
    -------
    pd.DataFrame
        Pandas dataframe of the file
    """

    valid_data_keys = ["data", "cf", "load", "evload"]

    with h5py.File(filename, "r") as f:
        # Identify keys in h5 file and check for overlap with valid key set
        keys = list(f.keys())
        datakey = list(set(keys).intersection(valid_data_keys))

        # Adding safety check to validate that it only returns one key
        assert len(datakey) <= 1, f"Multiple keys={datakey} found for {filename}"
        datakey = datakey[0] if datakey else None

        if datakey in keys:
            # load data
            df = pd.DataFrame(f[datakey][:])
        else:
            df = pd.DataFrame()

        # add columns to data if supplied
        if 'columns' in keys:
            df.columns = (
                pd.Series(f["columns"])
                .map(lambda x: x if isinstance(x, str) else x.decode("utf-8"))
                .values
            )

        # add any index values
        idx_cols = [c for c in keys if re.match('index_[0-9]', c)]
        if len(idx_cols) > 0:
            idx_cols.sort()
            for idx_col in idx_cols:
                df[idx_col] = pd.Series(f[idx_col]).values
            df = df.set_index(idx_cols)

        # add index names if supplied
        if 'index_names' in keys:
            df.index.names = (
                pd.Series(f["index_names"])
                .map(lambda x: x if isinstance(x, str) else x.decode("utf-8"))
                .values
            )

    return df

if __name__ == "__main__":
    scenarios = ["current policy", "central high data center", "central"]

    path = Path(f"input_data/")

    load_participation = pd.read_csv(snakemake.input.load_participation)
    county_to_ba = pd.read_csv(snakemake.input.county_to_ba)

    name_to_abbr = {state.name.lower():state.abbr for state in us.STATES_CONTINENTAL}

    states = ['alabama', 'alaska',
       'arizona', 'arkansas', 'california', 'colorado', 'connecticut',
       'delaware', 'district of columbia', 'florida', 'georgia', 'hawaii',
       'idaho', 'illinois', 'indiana', 'iowa', 'kansas', 'kentucky',
       'louisiana', 'maine', 'maryland', 'massachusetts', 'michigan',
       'minnesota', 'mississippi', 'missouri', 'montana', 'nebraska', 'nevada',
       'new hampshire', 'new jersey', 'new mexico', 'new york',
       'north carolina', 'north dakota', 'ohio', 'oklahoma', 'oregon',
       'pennsylvania', 'rhode island', 'south carolina', 'south dakota',
       'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington',
       'west virginia', 'wisconsin', 'wyoming']
    
    # load input files
    frames = []
    for scenario in scenarios:
        files = glob(str(path/scenario)+'/*.csv')
        scenario_frames = []
        for f in files:
            if 'summary_shapes' in f:
                continue
            else:
                year = int(f.split('\\')[-1].strip('.csv'))
                print(f'processing {scenario} - {year}', flush=True, end = '\r')
                df = pd.read_csv(f, parse_dates=True)
                df['year'] = year
                df['scenario'] = scenario
                scenario_frames.append(df)
        scenario_df = pd.concat(scenario_frames).reset_index(drop=True)
        frames.append(scenario_df)
        print('\n')


    # set historical data
    df = read_h5py_file(snakemake.input.historical_data)
    df_historical = df.loc[(list(range(2010,2025)), slice(None)),:]
    df_historical = df_historical.loc[(slice(None), (pd.to_datetime(df_historical.index.get_level_values(1).astype(str))).year==2012),:]
    dt_index = (pd.date_range('2012','2013', freq='h')[:8760]).astype(str)
    df_historical.index = pd.MultiIndex.from_product([list(range(2010,2025)), dt_index], names=['year','weather_datetime'])


    electrification_low_dc = frames[2]
    no_electrification_low_dc = frames[0]
    electrification_high_dc = frames[1]
    no_electrification_high_dc = no_electrification_low_dc.copy()

    # replace data center load
    no_electrification_high_dc.loc[no_electrification_high_dc\
                                   .subsector\
                                   .str\
                                   .contains('data center')] = electrification_high_dc.loc[electrification_high_dc\
                                                                                        .subsector\
                                                                                        .str\
                                                                                        .contains('data center')]
    
    output_name = { 'results/EER_Decarb_LowDC_UCS_load_hourly.h5':electrification_low_dc,
                    'results/EER_Current_LowDC_UCS_load_hourly.h5':no_electrification_low_dc,
                    'results/EER_Decarb_HighDC_UCS_load_hourly.h5':electrification_high_dc,
                    'results/EER_Current_HighDC_UCS_load_hourly.h5':no_electrification_high_dc,
                    }

    for i, (save_name, dataset) in enumerate(output_name.items()):
        print(f"Processing {save_name}")
        scenario_df = dataset.drop(columns=['scenario', 
                                            'sector',
                                            'subsector']).groupby(['year',
                                                                    'weather_datetime']).sum()
        scenario_annual = dataset.drop(columns=['scenario', 
                                                'sector',
                                                'subsector', 
                                                'weather_datetime']).groupby(['year']).sum()
        scenario_annual.index = pd.to_datetime(scenario_annual.index, format='%Y')
        print("Interpolating annual load")
        scenario_annual = scenario_annual.resample('YE').mean().interpolate('linear')

        print("Copying and rescaling load data")
        year_bins = [2025, 2030, 2035, 2040, 2045, 2050]
        frames = []
        pbar = trange(2025, 2051)
        for year in pbar:
            # determine which year to copy
            year_select = year_bins[(np.digitize(year, year_bins)-1)]

            pbar.set_description(f"{year} : {year_select}")
            pbar.update()
            
            subset_copy = scenario_df.loc[(year_select, slice(None)),:].copy()
            # update the index
            subset_copy.index = subset_copy.index.remove_unused_levels().set_levels([year], level=0)
            # print(f"{year} looks like {year_select}")
            for col in subset_copy.columns:
                col_total = subset_copy[col].sum()
                ratio = scenario_annual.at[str(year), col][0] / col_total
                subset_copy[col] = subset_copy[col] * ratio
            
            frames.append(subset_copy[:8760]) # truncate data
        
        print("Recombining data")
        scenario_interpolated = pd.concat(frames)

        print("Disaggregating states into balancing areas")
        combined = scenario_interpolated.copy()
        combined['maryland'] = combined['district of columbia'] + combined['maryland']
        combined = combined.drop(columns=['district of columbia'])
        combined = combined.rename(columns=name_to_abbr)
        combined_disagg = pd.DataFrame({row.ba:(combined[row.state].values\
                *load_participation.loc[load_participation['ba']==row.ba,'factor'].values[0]) 
        for row in county_to_ba.itertuples()})
        combined_disagg.index = scenario_interpolated.index
        scenario_final = pd.concat([df_historical, combined_disagg], axis=0)

        print("Saving to h5 file")
        write_profile_to_h5(scenario_final, save_name, outfolder="./")