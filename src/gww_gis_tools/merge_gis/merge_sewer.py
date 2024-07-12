"""Merge Sewer Module."""

from functools import reduce

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
    # TODO @timothy-holmes: this loop is convoluted, but it works # noqa: TD003, FIX002
    for a in config.files:  # a == AssetType
        # get file paths
        ww_files = {
            DataHelpers.get_table_name(fp, a, W): fp  # pyright: ignore[reportArgumentType]
            for fp in config.get_filepaths(a, W)  # pyright: ignore[reportArgumentType]
        }
        cww_files = config.get_filepaths(a, C)  # pyright: ignore[reportArgumentType]

        if (not len(ww_files)) or (not len(cww_files)):
            pass

        # load files into single gdf for each company
        ww_gdfs = [
            DataHelpers.read_file(fp).assign(SRC_TABLE=src)
            for src, fp in ww_files.items()
        ]
        ww_gdf = gpd.GeoDataFrame(
            pd.concat(ww_gdfs, ignore_index=True),
            crs=ww_gdfs[0].crs,
        )
        cww_gdf = DataHelpers.read_file(cww_files[0])

        #  drop WW abandonded assets
        if 'OP_STATUS' in ww_gdf.columns:
            OP_STATUS_ABANDONED = 3  # noqa: N806
            abandoned_where = ww_gdf['OP_STATUS'] == OP_STATUS_ABANDONED
            ww_gdf = ww_gdf[~abandoned_where]

        # set ASSET_OWNER
        if 'ASSET_OWNER' in cww_gdf.columns:
            cww_owner_mask = ~(cww_gdf['ASSET_OWNER'] == C.COMPANY)
            cww_gdf.loc[cww_owner_mask, 'ASSET_OWNER'] = f'NOT_{C.COMPANY}'
            ww_gdf['ASSET_OWNER'] = W.COMPANY

        for c in config.asset_uids:
            if c in cww_gdf.columns:
                cww_gdf[c] = cww_gdf[c].astype(int).astype(str) + f'_{C.COMPANY}'
            if c in ww_gdf.columns:
                ww_gdf[c] = ww_gdf[c].astype(str) + f'_{W.COMPANY}'

        ww_gdf = ww_gdf.rename(
            columns={
                c2: c1 for c2, c1 in config.column_map.items() if c2 in ww_gdf.columns
            },
        )

        if 'PIPE_DIA' in cww_gdf.columns:
            cww_gdf['PIPE_DIA'] = (
                cww_gdf['PIPE_DIA']
                .apply(FieldsHelpers.dia_str_to_dia_int)
                # .apply(FieldsHelpers.dia_str_to_height_width)
                # .apply(FieldsHelpers.height_width_to_dia)
                .astype(int)
            )
            cww_gdf = cww_gdf.drop(columns='PIPE_DIA')

        fields = FieldsHelpers.intersect(cww_gdf, ww_gdf)

        # concat C and W
        crs = cww_gdf.crs
        ww_gdf.to_crs(crs, inplace=True)

        output[a] = gpd.GeoDataFrame(
            pd.concat([cww_gdf[fields], ww_gdf[fields]], ignore_index=True),
            crs=crs,
        )

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
        if dtype in ('int', 'int64'):
            schema['properties'][col] = 'int32'

    gdf.to_file(filename, driver='MapInfo File', schema=schema)


def save_output(config: Config, output: dict) -> list[str]:
    """Save output."""
    outpaths = [config.output_template.format(id=asset_type) for asset_type in output]

    for asset_type, gdf in output.items():
        save_file(gdf=gdf, filename=config.output_template.format(id=asset_type))

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
