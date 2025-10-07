"""Merge Sewer Module."""

from datetime import datetime
from functools import reduce

from pandas.errors import InvalidIndexError

try:
    import geopandas as gpd  # pyright: ignore[reportMissingImports]
    import pandas as pd
except ImportError as e:
    msg = "geopandas not installed. Use 'conda install -c conda-forge geopandas'"
    raise NotImplementedError(msg) from e

from gww_gis_tools.merge_gis.sewer_helpers import (
    AssetType,
    C,
    Config,
    Corrections,
    DataHelpers,
    FieldsHelpers,
    W,
)


def merge(config: Config, output: dict) -> dict[AssetType, gpd.GeoDataFrame]:
    """Merge."""
    for a in config.files:  # a == AssetType
        # get file paths
        ww_files = {
            DataHelpers.get_table_name(fp, a, W): fp  # pyright: ignore[reportArgumentType]
            for fp in config.get_filepaths(a, W)  # pyright: ignore[reportArgumentType]
        }
        cww_files = config.get_filepaths(a, C)  # pyright: ignore[reportArgumentType]

        if not ww_files or not cww_files:
            continue

        # load files into single gdf for each company
        ww_gdfs = [
            DataHelpers.read_file(fp).assign(SRC_TABLE=src)
            for src, fp in ww_files.items()
        ]
        ww_gdf = gpd.GeoDataFrame(
            pd.concat(ww_gdfs, axis=0, ignore_index=True),
            crs=ww_gdfs[0].crs,
        ).reset_index(drop=True)

        cww_gdf = DataHelpers.read_file(cww_files[0])


        #  drop WW abandoned assets
        if 'OP_STATUS' in ww_gdf.columns:
            OP_STATUS_ABANDONED = 3  # noqa: N806
            OP_STATUS_INACTIVE = 2  # noqa: N806
            abandoned_where = ww_gdf['OP_STATUS'] == OP_STATUS_ABANDONED
            inactive_where = ww_gdf['OP_STATUS'] == OP_STATUS_INACTIVE
            ww_gdf = ww_gdf.loc[~(abandoned_where | inactive_where), :]

        # set ASSET_OWNER
        if 'ASSET_OWNER' in cww_gdf.columns:
            cww_gdf['ASSET_OWNER'] = cww_gdf[c].astype(str) + f'_{C.COMPANY.value}'
            ww_gdf['ASSET_OWNER'] = W.COMPANY.value

        for c in config.asset_uids:
            if c in cww_gdf.columns:
                cww_gdf[c] = cww_gdf[c].astype(int).astype(str) + f'_{C.COMPANY.value}'
            if c in ww_gdf.columns:
                ww_gdf[c] = ww_gdf[c].astype(str) + f'_{W.COMPANY.value}'

        if 'NODE_REF' in ww_gdf.columns:
            ww_gdf = ww_gdf.drop(columns=['NODE_REF'])
        ww_gdf.columns = [config.column_map.get(c) or c for c in ww_gdf.columns]

        if 'PIPE_DIA' in cww_gdf.columns:
            cww_gdf['PIPE_DIA'] = (
                cww_gdf['PIPE_DIA']
                .apply(FieldsHelpers.dia_str_to_dia_int)
                .astype(int)
            )

        fields = FieldsHelpers.intersect(cww_gdf, ww_gdf)

        # handle datetime columns
        DATE_FORMAT = '%Y-%m-%d'

        for c in cww_gdf.columns:
            if '_DATE' in c or 'DATE_' in c:
                dt_col = pd.to_datetime(
                    cww_gdf[c],
                    format='mixed',
                    dayfirst=True,
                    errors='coerce',
                )
                bad_date_index = dt_col.isna()
                cww_gdf[c] = dt_col
                cww_gdf.loc[bad_date_index, c] = pd.Timestamp.max
                cww_gdf[c] = cww_gdf[c].apply(
                    lambda x, dt_format=DATE_FORMAT: datetime.strftime(x, dt_format)
                )
                print(f'CWW GDF col {c} contained {bad_date_index.sum()} bad dates')

        for c in ww_gdf.columns:
            if '_DATE' in c or 'DATE_' in c:
                dt_col = pd.to_datetime(
                    ww_gdf[c],
                    format='mixed',
                    dayfirst=True,
                    errors='coerce',
                )
                bad_date_index = dt_col.isna()
                ww_gdf[c] = dt_col
                ww_gdf.loc[bad_date_index, c] = pd.Timestamp.max
                ww_gdf[c] = ww_gdf[c].apply(
                    lambda x, dt_format=DATE_FORMAT: datetime.strftime(x, dt_format)
                )
                print(f'WW GDF {c} col contained {bad_date_index.sum()} bad dates')

        # concat C and W
        crs = cww_gdf.crs
        ww_gdf = ww_gdf.to_crs(crs)
        gdfs = [
            cww_gdf[fields].reset_index(drop=True),
            ww_gdf[fields].reset_index(drop=True)
        ]

        try:
            # Check for unique indices before concatenation
            if not gdfs[0].index.is_unique or not gdfs[1].index.is_unique:
                raise ValueError("Indices are not unique. Ensure data frames have unique indices.")

            gdfs = pd.concat(gdfs, axis=0, ignore_index=True)
            output[a] = gpd.GeoDataFrame(gdfs, crs=crs)
        except InvalidIndexError as e:
            print('fields', fields)
            print(
                list(gdfs[0].columns), list(gdfs[1].columns)
            )
            print(gdfs[0].index.is_unique, gdfs[1].index.is_unique)
            raise e

    return output


def make_corrections(config: Config, output: dict) -> dict[AssetType, gpd.GeoDataFrame]:  # noqa: ARG001
    """Make corrections."""
    if AssetType.PIPES in output:
        pipes_gdf = output[AssetType.PIPES]

        pipe_corrections = [{'pipe_id': '104368_CWW', 'action': Corrections.swap_nodes}]

        for c in pipe_corrections:
            index_where = pipes_gdf['PIPE_ID'] == c['pipe_id']
            pipes_gdf.loc[index_where, :] = pipes_gdf.loc[index_where, :].apply(
                c['action'],
                axis=1,
            )

        # temporary fix for START_NODE/END_NODE = 0_CWW, 0_WW
        output[AssetType.PIPES] = pipes_gdf.drop(
            pipes_gdf[
                (pipes_gdf['START_NODE'] == '0_CWW')
                | (pipes_gdf['START_NODE'] == '0_WW')
                | (pipes_gdf['END_NODE'] == '0_CWW')
                | (pipes_gdf['END_NODE'] == '0_WW')
            ].index,
        )

    return output


def classify_parcels(config: Config, output: dict) -> dict[AssetType, gpd.GeoDataFrame]:  # noqa: ARG001
    """Clasify parcels as served and unserved."""
    if AssetType.PARCELS in output and AssetType.BRANCHES in output:
        branches_gdf = output[AssetType.BRANCHES]
        parcels_gdf = output[AssetType.PARCELS]

        drop_parcel_mask = parcels_gdf['GID'].isin(branches_gdf['PRCL_GID'])
        parcels_unserved_gdf = parcels_gdf[~drop_parcel_mask].copy()

        output['parcels_unserved'] = parcels_unserved_gdf
        output['parcels'] = parcels_gdf.drop(parcels_gdf[~drop_parcel_mask].index)

    return output


def save_file(gdf: gpd.GeoDataFrame, filename: str) -> None:
    """Save dataframes to file."""
    # fix for MapInfo layers with Int64 field type
    # ordinarily this is inferred within gpd.to_file()
    schema = gpd.io.file.infer_schema(gdf)

    for col, dtype in schema['properties'].items():
        # force int32 field types
        if dtype in ('int', 'int64'):
            schema['properties'][col] = 'int32'
        # coerce date field
        if col.endswith('_DATE'):
            schema['properties'][col] = 'date'

    gdf.to_file(filename, engine='fiona', driver='MapInfo File', schema=schema)


def save_output(config: Config, output: dict) -> list[str]:
    """Save output."""
    outpaths = [config.output_template.format(id=asset_type.value) for asset_type in output]

    for asset_type, gdf in output.items():
        save_file(gdf=gdf, filename=config.output_template.format(id=asset_type.value))

    return outpaths


# example usage
def run() -> list[str]:
    """Run full Merge operation."""
    config = Config()
    output = {}

    func_list = [merge, make_corrections, classify_parcels, save_output]

    return reduce(lambda o, func: func(config, o), func_list, output)  # pyright: ignore[reportArgumentType]


if __name__ == '__main__':
    result = run()
