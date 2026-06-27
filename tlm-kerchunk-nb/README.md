# tlm-kerchunk-nb

Time-series access to Sentinel-2 using pre-computed **TLM headers** (`jp2io`),
driven by a STAC query.

`index.ipynb` builds an xarray `DataTree` for a single MGRS tile (here `33TVN`)
directly from a CDSE STAC search: it intersects the catalogue with the local TLM
index (`33TVN.parquet`), and returns a lazy, dask-backed cube whose chunks are
virtual references into the original JP2s on CDSE S3, decoded on the fly. Unlike
the `odc-stac` / `xcube` notebooks it stays in the tile's native UTM grid (no
reprojection, no mosaicking), so reads only touch the JP2 tiles they need.

## Run

```bash
uv sync
# requires an AWS profile named "eodata" with CDSE S3 keys in ~/.aws/credentials
```

Then open `index.ipynb`.