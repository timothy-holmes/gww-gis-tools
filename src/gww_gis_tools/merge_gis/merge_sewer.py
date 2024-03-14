import tempfile
import time
from typing import Any, Union
from functools import reduce
import os.path

import humanize
import pandas as pd

try:
    import fiona
    import geopandas as gpd
except ImportError:
    HAS_GPD = False
else:
    HAS_GPD = True

class AssetType:
    """ Enum-like class to prevent typos """
    PARCELS = 'parcels'
    PIPES = 'pipes'
    BRANCHES = 'branches'
    NODES = 'nodes'


class Config:
    # files = {
    #     AssetType.PARCELS: [
    #         "C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Central Region\\Cadastre\\Parcels.tab",
    #         "C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Western Region\\Cadastre\\SP_PROPERTY.shp"
    #     ],
    #     AssetType.PIPES: [
    #         "C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Western Region\\Sewer\\SP_SEWGPIPE.shp",
    #         "C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Western Region\\Sewer\\SP_SEWVPIPE.shp",
    #         "C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Western Region\\Sewer\\SP_SEWRPIPE.shp",
    #         "C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Central Region\\Sewer\\Sewer_Pipe.TAB",
    #     ],
    #     AssetType.BRANCHES: [
    #         "C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Western Region\\Sewer\\SP_SEWSERV.shp",
    #         "C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Central Region\\Sewer\\Sewer_Branch.tab",
    #     ],
    #     AssetType.NODES: [
    #         "C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Western Region\\Sewer\\SP_SEWNODE.shp",
    #         "C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Central Region\\Sewer\\Sewer_Node.tab",
    #     ]
    # }

    files = {
        AssetType.PARCELS: [
            "\\\\citywestwater.com.au\\data\\pccommon\\Asset Information\\MUNSYS MapInfo Data\\Production\\Data\\Cadastre\\Parcels.tab",
            "\\\\wro-gisapp\\MunsysExport\\SP_PROPERTY.shp"
        ],
        AssetType.PIPES: [
            "\\\\wro-gisapp\\MunsysExport\\SP_SEWGPIPE.shp",
            "\\\\wro-gisapp\\MunsysExport\\SP_SEWVPIPE.shp",
            "\\\\wro-gisapp\\MunsysExport\\SP_SEWRPIPE.shp",
            "\\\\citywestwater.com.au\\data\\pccommon\\Asset Information\\MUNSYS MapInfo Data\\Production\\Data\\Sewer\\Sewer_Pipe.TAB",
        ],
        AssetType.BRANCHES: [
            "\\\\wro-gisapp\\MunsysExport\\SP_SEWSERV.shp",
            "\\\\citywestwater.com.au\\data\\pccommon\\Asset Information\\MUNSYS MapInfo Data\\Production\\Data\\Sewer\\Sewer_Branch.tab",
        ],
        AssetType.NODES: [
            "\\\\wro-gisapp\\MunsysExport\\SP_SEWNODE.shp",
            "\\\\citywestwater.com.au\\data\\pccommon\\Asset Information\\MUNSYS MapInfo Data\\Production\\Data\\Sewer\\Sewer_Node.tab",
        ]
    }

    output_template = r'C:\Users\holmest1\Greater Western Water\IP - Spatial - Documents\Input\2. GWW GIS Exports\Existing Assets\Merged Regions\Sewer\GWW_{id}.tab'

    asset_uids = {
        'END_NODE', 'START_NODE', 'PIPE_ID', 'GID', # pipes
        'NODE_ID', 'GID',                           # nodes
        'GID', 'SERV_ID'                            # branches
        'GID', 'PRCL_GID', 'PROP_GID',              # parcels
    }

    column_map = {
        'ECOVELEV': 'END_COVELEV', 
        'SCOVELEV': 'START_COVELEV', 
        'E_INVELEV': 'END_INVELEV', 
        'S_INVELEV': 'START_INVELEV', 
        'GEOMLENGTH': 'GEOM_LENGTH', 
        'NODECOVELE': 'NODE_COVELEV'
    }

    @staticmethod
    def possible_outpaths(config):
        possible_ids = list(config.files.keys()) + ['parcels_unserved', 'parcels']

        outpaths = {
            id: os.path.getmtime(config.output_template.format(id=id))
            for id in possible_ids
            if os.path.exists(config.output_template.format(id=id))
        }

        return outpaths


class Region:
    """ Enum-like class to prevent typos """
    def __init__(self, **kwargs) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)

W = Region(SHORT = 'W', COMPANY = 'WW', FULL = 'Western Region', SERVER = 'wro-gisapp')
C = Region(SHORT = 'C', COMPANY = 'CWW', FULL = 'Central Region', SERVER = 'citywestwater.com.au')
regions = [W, C]

class TimeKeeper:
    def __init__(self) -> None:
        self.start_time = time.time()
        self.time_last_reported = self.start_time

    def get_time_since_last_reported(self):
        time_since_last_reported = time.time() - self.time_last_reported
        self.time_last_reported = time.time()
        # pretty print new_time
        return humanize.naturaldelta(time_since_last_reported)


class DataFileHelpers:
    @staticmethod
    def get_filepaths(config: Config, asset_type: str, region: str):
        return [
            f 
            for f in config.files[asset_type] 
            if (region.FULL in f) or (region.SERVER in f)
        ]

    @ staticmethod
    def get_table_name(filepath, asset_type, region):
        if (asset_type, region) == (AssetType.PIPES, W):
            return filepath.split('\\')[-1][:-4]
        else:
            return filepath

    @ staticmethod
    def load_invalid_file(src_path):
        """ Modify read_file to handle invalid geometry """
        # Create a temporary file for writing the modified shapefile
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file = temp_dir + '\modified_file.noext'

            # Parse original file
            with fiona.open(src_path) as src:
                # Start temp file
                with fiona.open(temp_file, 'w', **src.meta) as dst:
                    # Check each feature for valid LineString geometry
                    for feature in src:
                        if feature['geometry']['type'] == 'LineString':
                            if len(feature['geometry']['coordinates']) > 1:
                                dst.write(feature)
                        if feature['geometry']['type'] == 'MultiLineString':
                            feature['geometry']['coordinates'] = [f for f in feature['geometry']['coordinates'] if len(f) > 1]
                            dst.write(feature)

            # Read the modified shapefile
            gdf = gpd.read_file(temp_file)
        return gdf

    @classmethod
    def read_file(cls, *args, **kwargs) -> gpd.GeoDataFrame:
        """ Modified read_file to handle invalid geometry """
        try:
            gdf = gpd.read_file(*args, **kwargs)
            filename = args[0].split("\\")[-1]
            print(f'Loaded {gdf.shape[0]} rows from {filename}')
            return gdf
        except Exception as e:
            if e.args == ('LineStrings must have at least 2 coordinate tuples',):
                print(f'Warning: naughty file found. Fixing error: {e.args}')
                gdf = cls.load_invalid_file(*args, **kwargs)
                print(f'Loaded {gdf.shape[0]} rows from {args[0]}')
                return gdf
            else:
                print(f'Unhandled error {args}, {kwargs}: {e.args}')


class FieldsHelpers:
    @staticmethod
    def intersect(x,y): 
        return list(sorted(set(x.columns).intersection(y.columns)))
    
    @staticmethod
    def diff(x,y): 
        return list(sorted(set(x.columns).difference(y.columns)))
    
    @staticmethod
    def dia_to_int(x):
        try: 
            return int(str(x).split('x')[0])
        except ValueError: # commonly 'UNKN'
            return 0


class NotifyDict(dict):
    """ Dictionary that prints on __setitem__ """
    def __init__(self, timekeeper: TimeKeeper, notify: bool = True, *args, **kwargs) -> None:
        self.timekeeper = timekeeper   
        self.notify = notify       
        super().__init__(*args, **kwargs)

    def __setitem__(self, __key: Any, __value: Any) -> None:
        if self.notify:
            print(f'{self.timekeeper.get_time_since_last_reported()}: {__key}')

        return super().__setitem__(__key, __value)


class Corrections:
    @staticmethod
    def reverse_direction(row):
        row.END_NODE, row.START_NODE = row.START_NODE, row.END_NODE
        row.END_COVELEV, row.START_COVELEV = row.START_COVELEV, row.END_COVELEV
        row.END_INVELEV, row.START_INVELEV = row.START_INVELEV, row.START_INVELEV
        return row

    @staticmethod
    def swap_nodes(row):
        row.END_NODE, row.START_NODE = row.START_NODE, row.END_NODE
        return row


def merge(config, output: Union[NotifyDict, dict]):
    for a in config.files.keys():
        ww_files = {
            DataFileHelpers.get_table_name(fp, a, W): fp 
            for fp in DataFileHelpers.get_filepaths(a, W)
        }
        cww_files = DataFileHelpers.get_filepaths(a, C)

        if (not len(ww_files)) or (not len(cww_files)):
            print('Skipping', a)

        ww_gdfs = [DataFileHelpers.read_file(fp).assign(SRC_TABLE=src) for src, fp in ww_files.items()]
        ww_gdf = gpd.GeoDataFrame(pd.concat(ww_gdfs, ignore_index=True), crs=ww_gdfs[0].crs)

        cww_gdf = DataFileHelpers.read_file(cww_files[0])

        if 'ASSET_OWNER' in cww_gdf.columns:
            cww_owner_mask = cww_gdf[~(cww_gdf['ASSET_OWNER'] == C.COMPANY)]
            cww_gdf.drop(cww_owner_mask.index, inplace=True)
        
        ww_gdf['ASSET_OWNER'] = W.COMPANY

        for c in config.asset_uids:
            if c in cww_gdf.columns:
                cww_gdf[c] = cww_gdf[c].astype(int).astype(str) + '_' + C.COMPANY
            if c in ww_gdf.columns:
                ww_gdf[c] = ww_gdf[c].astype(str) + '_' + W.COMPANY

        ww_gdf.rename(columns={c2: c1 for c2, c1 in config.column_map.items() if c2 in ww_gdf.columns}, inplace=True)

        if 'PIPE_DIA' in cww_gdf.columns:
            cww_gdf['PIPE_DIA'] = cww_gdf['PIPE_DIA'].apply(FieldsHelpers.dia_to_int).astype(int)

        fields = FieldsHelpers.intersect(cww_gdf, ww_gdf)
        
        # concat C and W
        crs = cww_gdf.crs
        ww_gdf.to_crs(crs, inplace=True)

        joined_gdf = gpd.GeoDataFrame(
            pd.concat(
                [cww_gdf[fields], ww_gdf[fields]], 
                ignore_index=True),
            crs=crs
        )
        joined_gdf.reset_index(inplace=True)

        output[a] = joined_gdf

    return output


def make_corrections(config, output: Union[NotifyDict, dict]):
    if AssetType.PIPES in output:
        pipes_gdf = output[AssetType.PIPES]

        pipe_corrections = [
            {'pipe_id': '104368_CWW', 'action': Corrections.swap_nodes}
        ]

        for c in pipe_corrections:
            row = pipes_gdf.loc[pipes_gdf['PIPE_ID'] == c['pipe_id']]
            pipes_gdf.loc[pipes_gdf['PIPE_ID'] == c['pipe_id']] = c['action'](row)

        # temporary fix for START_NODE/END_NODE = 0_CWW, 0_WW
        pipes_gdf.drop(
            pipes_gdf[
                (pipes_gdf['START_NODE'] == '0_CWW') |
                (pipes_gdf['START_NODE'] == '0_WW') |
                (pipes_gdf['END_NODE'] == '0_CWW') |
                (pipes_gdf['END_NODE'] == '0_WW')
            ].index, 
            inplace=True
        )

        output[AssetType.PIPES] = pipes_gdf

    return output


def classify_parcels(config, output: Union[NotifyDict, dict]):
    if AssetType.PARCELS in output and AssetType.BRANCHES in output:
        branches_gdf = output[AssetType.BRANCHES]
        parcels_gdf = output[AssetType.PARCELS]

        drop_parcel_mask = parcels_gdf['GID'].isin(branches_gdf['PRCL_GID'])
        parcels_unserved_gdf = parcels_gdf[~drop_parcel_mask].copy()
        parcels_gdf.drop(parcels_gdf[~drop_parcel_mask].index, inplace=True)

        output['parcels_unserved'] = parcels_unserved_gdf
        output['parcels'] = parcels_gdf

    return output


def save_file(gdf, filename):
    schema = gpd.io.file.infer_schema(gdf) # ordinarily this is inferred within gpd.to_file()
    for col, dtype in schema["properties"].items():
        if dtype == "int" or dtype == "int64":
            schema["properties"][col] = "int32"
    gdf.to_file(filename, driver="MapInfo File", schema=schema)


def save_output(config: Config, output: Union[NotifyDict, dict]):
    outpaths = [config.output_template.format(id=id) for id in output]

    for id, gdf in output.items():
        save_file(
            gdf=gdf,
            filename=config.output_template.format(id=id)
        )
    print(f'{id} saved')

    return outpaths


def possible_outpaths(config):
    possible_ids = list(config.files.keys()) + ['parcels_unserved', 'parcels']

    outpaths = {
        id: os.path.getmtime(config.output_template.format(id=id))
        for id in possible_ids
        if os.path.exists(config.output_template.format(id=id))
    }

    return outpaths


# example usage
def run():
    config = Config()
    timekeeper = TimeKeeper()
    output = NotifyDict(timekeeper) # dict()

    func_list = [merge, make_corrections, classify_parcels, save_output]
    outpaths = reduce(lambda o, func: func(config, o), func_list, output)
    print(humanize.naturaldelta(time.time() - timekeeper.start_time))

    return outpaths


if __name__ == '__main__':
    result = run()
    print(result)