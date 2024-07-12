"""Missing docstring."""

from __future__ import annotations

import tempfile
from contextlib import ExitStack
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

    import pandas as pd


try:
    import fiona  # type: ignore[import-untyped]
    import geopandas as gpd  # type: ignore[import-untyped]
    from shapely.errors import GEOSException
except ImportError as e:
    msg = "geopandas not installed. Use 'conda install -c conda-forge geopandas'"
    raise NotImplementedError(msg) from e


class AssetType(Enum):
    """Enum-like class to prevent typos."""

    PARCELS = 'parcels'
    PIPES = 'pipes'
    BRANCHES = 'branches'
    NODES = 'nodes'


class W(Enum):
    """Enum-like class to prevent typos."""

    SHORT = 'W'
    COMPANY = 'WW'
    FULL = 'Western Region'
    SERVER = 'wro-gisapp'


class C(Enum):
    """Enum-like class to prevent typos."""

    SHORT = 'C'
    COMPANY = 'CWW'
    FULL = 'Central Region'
    SERVER = 'citywestwater.com.au'


class Config:
    """The Config class in your codebase is a configuration class that holds various settings and mappings for your application."""  # noqa: E501

    local_files = MappingProxyType(
        {
            AssetType.PARCELS: {
                W: [
                    'C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Central Region\\Cadastre\\Parcels.tab',  # noqa: E501
                ],
                C: [
                    'C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Western Region\\Cadastre\\SP_PROPERTY.shp',  # noqa: E501
                ],
            },
            AssetType.PIPES: {
                W: [
                    'C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Western Region\\Sewer\\SP_SEWGPIPE.shp',  # noqa: E501
                    'C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Western Region\\Sewer\\SP_SEWVPIPE.shp',  # noqa: E501
                    'C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Western Region\\Sewer\\SP_SEWRPIPE.shp',  # noqa: E501
                ],
                C: [
                    'C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Central Region\\Sewer\\Sewer_Pipe.TAB',  # noqa: E501
                ],
            },
            AssetType.BRANCHES: {
                W: [
                    'C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Western Region\\Sewer\\SP_SEWSERV.shp',  # noqa: E501
                ],
                C: [
                    'C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Central Region\\Sewer\\Sewer_Branch.tab',  # noqa: E501
                ],
            },
            AssetType.NODES: {
                W: [
                    'C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Western Region\\Sewer\\SP_SEWNODE.shp',  # noqa: E501
                ],
                C: [
                    'C:\\Users\\holmest1\\Greater Western Water\\IP - Spatial - Documents\\Input\\2. GWW GIS Exports\\Existing Assets\\Central Region\\Sewer\\Sewer_Node.tab',  # noqa: E501
                ],
            },
        }
    )

    network_files = MappingProxyType(
        {
            AssetType.PARCELS: {
                W: ['\\\\wro-gisapp\\MunsysExport\\SP_PROPERTY.shp'],
                C: [
                    '\\\\citywestwater.com.au\\data\\pccommon\\Asset Information\\MUNSYS MapInfo Data\\Production\\Data\\Cadastre\\Parcels.tab',  # noqa: E501
                ],
            },
            AssetType.PIPES: {
                W: [
                    '\\\\wro-gisapp\\MunsysExport\\SP_SEWGPIPE.shp',
                    '\\\\wro-gisapp\\MunsysExport\\SP_SEWVPIPE.shp',
                    '\\\\wro-gisapp\\MunsysExport\\SP_SEWRPIPE.shp',
                ],
                C: [
                    '\\\\citywestwater.com.au\\data\\pccommon\\Asset Information\\MUNSYS MapInfo Data\\Production\\Data\\Sewer\\Sewer_Pipe.TAB',  # noqa: E501
                ],
            },
            AssetType.BRANCHES: {
                W: ['\\\\wro-gisapp\\MunsysExport\\SP_SEWSERV.shp'],
                C: [
                    '\\\\citywestwater.com.au\\data\\pccommon\\Asset Information\\MUNSYS MapInfo Data\\Production\\Data\\Sewer\\Sewer_Branch.tab',  # noqa: E501
                ],
            },
            AssetType.NODES: {
                W: ['\\\\wro-gisapp\\MunsysExport\\SP_SEWNODE.shp'],
                C: [
                    '\\\\citywestwater.com.au\\data\\pccommon\\Asset Information\\MUNSYS MapInfo Data\\Production\\Data\\Sewer\\Sewer_Node.tab',  # noqa: E501
                ],
            },
        }
    )

    # cloud_queries = {  # noqa: ERA001, RUF100
    #     AssetType.PIPES: {
    #         W: "SELECT * FROM SP_SEWGPIPE",
    #         C: "SELECT * FROM SP_SEWVPIPE UNION SELECT * FROM SP_SEWRPIPE",
    #     },
    #     AssetType.BRANCHES: {
    #         W: "SELECT * FROM SP_SEWSERV",
    #         C: "SELECT * FROM SP_SEWSERV",
    #     },
    #     AssetType.NODES: {W: "SELECT * FROM SP_SEWNODE", C: "SELECT * FROM SP_SEWNODE"}, # noqa: E501
    #     AssetType.PARCELS: {
    #         W: "SELECT * FROM SP_PROPERTY",
    #         C: "SELECT * FROM SP_PROPERTY",
    #     },
    # } # noqa: ERA001, RUF100

    output_template = r'C:\Users\holmest1\Greater Western Water\IP - Spatial - Documents\Input\2. GWW GIS Exports\Existing Assets\Merged Regions\Sewer\GWW_{id}.tab'  # noqa: E501

    asset_uids = tuple(
        {
            'END_NODE',
            'START_NODE',
            'PIPE_ID',
            'GID',  # pipes
            'NODE_ID',
            'GID',  # nodes # noqa: B033
            'SERV_ID',
            'GID',  # branches # noqa: B033
            'PRCL_GID',
            'PROP_GID',  # parcels
        }
    )

    column_map = MappingProxyType(
        {
            # pipes
            'ECOVELEV': 'END_COVELEV',
            'SCOVELEV': 'START_COVELEV',
            'E_INVELEV': 'END_INVELEV',
            'S_INVELEV': 'START_INVELEV',
            'GEOMLENGTH': 'GEOM_LENGTH',
            'PGRADIENT': 'PIPE_GRADIENT',
            # nodes
            'MH_DESC': 'NODE_REF',
            'NODECOVELE': 'NODE_COVELEV',
        }
    )

    def __init__(self) -> None:
        """Initialize the class."""
        self._files: Mapping[AssetType, Mapping[W | C, list[str]]] = {}

    @property
    def files(self) -> Mapping[AssetType, Mapping[W | C, list[str]]]:
        """Get the files."""
        return self._files or self.network_files  # pyright: ignore[reportReturnType]

    @files.setter
    def files(self, data_source: Mapping[AssetType, Mapping[W | C, list[str]]]) -> None:
        self._files = data_source

    def possible_outpaths(self) -> Mapping[AssetType, float]:
        """Get the possible output paths."""
        return {
            asset_type: Path(self.output_template.format(id=id)).stat().st_mtime
            for asset_type in self.files
            if Path(self.output_template.format(id=id)).exists()
        }

    def get_filepaths(self, asset_type: AssetType, region: C | W) -> list[str]:
        """Get the source filepaths."""
        return self.files.get(asset_type, {W: [], C: []}).get(region, [])  # pyright: ignore[reportCallIssue, reportArgumentType]


class DataHelpers:
    """Data helpers class."""

    @staticmethod
    def get_table_name(filepath: str, asset_type: AssetType, region: C | W) -> str:
        """Missing docstring."""
        if (asset_type, region) == (AssetType.PIPES, W):
            return filepath.split('\\')[-1][:-4]
        return filepath

    @staticmethod
    def load_bad_linestring_file(src_path: str) -> gpd.GeoDataFrame:
        """Modify read_file to handle invalid geometry."""
        # Create a temporary file for writing the modified shapefile
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file = temp_dir + '\\modified_file.noext'

            # Parse original file
            with ExitStack() as stack:
                src = stack.enter_context(fiona.open(src_path))
                dst = stack.enter_context(fiona.open(temp_file, 'w', **src.meta))
                # Check each feature for valid LineString geometry
                for feature in src:
                    if (
                        feature['geometry']['type'] == 'LineString'
                        and len(feature['geometry']['coordinates']) > 1
                    ):
                        dst.write(feature)
                    if feature['geometry']['type'] == 'MultiLineString':
                        feature['geometry']['coordinates'] = [
                            f for f in feature['geometry']['coordinates'] if len(f) > 1
                        ]
                        dst.write(feature)

            # Read the modified shapefile
            return gpd.read_file(temp_file)

    @classmethod
    def load_bad_polygon_file(cls, src_path: str) -> gpd.GeoDataFrame:
        """Modify read_file to handle invalid geometry."""
        # Create a temporary file for writing the modified shapefile
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file = temp_dir + '\\modified_file.noext'

            # Parse original file
            with ExitStack() as stack:
                src = stack.enter_context(fiona.open(src_path))
                dst = stack.enter_context(fiona.open(temp_file, 'w', **src.meta))
                # Fix each feature of Polygon geometry
                for feature in src:
                    if feature['geometry']['type'] == 'Polygon':
                        fixed_coords = cls.fix_polygon(
                            feature['geometry']['coordinates']
                        )
                    elif feature['geometry']['type'] == 'MultiPolygon':
                        fixed_coords = [
                            cls.fix_polygon(
                                polygon_coords
                            )
                            for polygon_coords in feature['geometry']['coordinates']
                        ]
                    else:
                        # Unexpected geometry
                        fixed_coords = feature['geometry']['coordinates']

                    feature['geometry']['coordinates'] = fixed_coords
                    dst.write(feature)

            # Read the modified shapefile
            return gpd.read_file(temp_file)

    @staticmethod
    def fix_polygon(polygon_coords: list[list]) -> list[list]:
        """Append first point to ring (list of coords) if last point not the same."""
        for ring_coords in polygon_coords:
            if ring_coords[0] != ring_coords[-1]:
                ring_coords.append(ring_coords[-1])
        return polygon_coords


    @classmethod
    def read_file(cls, *args, **kwargs) -> gpd.GeoDataFrame:  # noqa: ANN002, ANN003
        """Mock read_file to handle invalid geometry."""
        try:
            return gpd.read_file(*args, **kwargs)
        except ValueError as e:
            if e.args == ('LineStrings must have at least 2 coordinate tuples',):
                return cls.load_bad_linestring_file(*args, **kwargs)
            raise Exception(f'Unknown error {e.args}') from e
        except GEOSException as e:
            if e.args == ('IllegalArgumentException: Points of LinearRing do not form a closed linestring',):
                return cls.load_bad_polygon_file(*args, **kwargs)
            raise Exception(f'Unknown error {e.args}') from e


class FieldsHelpers:
    """Organising class for FieldsHelpers."""

    @staticmethod
    def intersect(x: gpd.GeoDataFrame, y: gpd.GeoDataFrame) -> list[str]:
        """Return intersecting column names of given (geo)dataframes."""
        return sorted(set(x.columns).intersection(y.columns))

    @staticmethod
    def diff(x: gpd.GeoDataFrame, y: gpd.GeoDataFrame) -> list[str]:
        """Return difference of column names of given (geo)dataframes."""
        return sorted(set(x.columns).difference(y.columns))

    @staticmethod
    def dia_str_to_height_width(x: str | int) -> tuple[int, int]:
        """Convert diameter to height and width."""
        hw = str(x).split('x')

        if len(hw) == 1:
            hw.append(hw[0])
        elif len(hw) > 1:
            hw = [hw[0], hw[1]]

        try:
            hw = int(hw[0]), int(hw[1])
        except ValueError:  # commonly 'UNKN'
            hw = (0, 0)

        return hw

    @staticmethod
    def height_width_to_dia(x: tuple[int, int]) -> int:
        """Convert height and width to diameter."""
        return int(x[0] + x[1] / 2)

    @classmethod
    def dia_str_to_dia_int(cls, x: str | int) -> int:
        """Convert height and width to diameter."""
        return cls.height_width_to_dia(cls.dia_str_to_height_width(x))


class Corrections:
    """Organising class for corrections on (g)df rows."""

    @staticmethod
    def reverse_direction(row: pd.Series) -> pd.Series:
        """Missing docstring."""
        row.END_NODE, row.START_NODE = row.START_NODE, row.END_NODE
        row.END_COVELEV, row.START_COVELEV = row.START_COVELEV, row.END_COVELEV
        row.END_INVELEV, row.START_INVELEV = row.START_INVELEV, row.START_INVELEV
        return row

    @staticmethod
    def swap_nodes(row: pd.Series) -> pd.Series:
        """Missing docstring."""
        row.END_NODE, row.START_NODE = row.START_NODE, row.END_NODE
        return row
