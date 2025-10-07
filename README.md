# gww_gis_tools

Package consisting of two modules for working with GWW (CWW+WW) data:

- merge_sewer (incomplete implementation of business logic to merge the sepreate systems)
- trace_sewer (implementation of depth-first-search traversal in sewer network)

## Merge Sewer

```python
from merge_gis.merge_sewer import AssetType, Config, merge, save_output

config = Config()

# limit to pipes only
config['files'] = {
    asset_type: files 
    for asset_type, files in config.files 
    if asset_type == AssetType.PIPES
}

# merge and save
output = merge(config)
results = save_output(config, output)
print(results)
```

## Trace Sewer

```python
from trace_gis.trace_sewer import Graph, Trace, DIRECTION

# get data from merged pipes
data = output[AssetType.PIPES]

g = Graph(DIRECTION.U).from_gdf()

tr = Trace(g).trace()
```

## Quick Start

```sh
pip install git+https://github.com/timothy-holmes/gww-gis-tools
```
