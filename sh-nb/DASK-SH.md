# Lazy loading with Sentinelhub APIs

Goal: 

- similiar behaviour to odc-stac, stackstac or xcube
- query catalog, build a lazy dask xarray

Design:

- Make extensive use of sentinelhub-py, try to be as light as possible aruond that package do not reimplement logic which is already present there.
- Get valid data collections and bands which can be defined from: sentinelhub data_collections enum https://sentinelhub-py.readthedocs.io/en/latest/reference/sentinelhub.data_collections.html

- User defines a datacube with datacollection name, name of bands, bbox, and width, height or resolution.
- also have option to filter results based on item properties (i.e. eo:cloud_cover)
- Lazy dask array is built by calling catalog and getting number of acquisitions
- To keep down number of requests: 1 request per time-stamp 
    - to do that we need to build an evalscript with all bands. We would also need to specify unit and dtype for all bands. For now let's pick the unit combination which allows for the smallest dtype. (i.e. pick DN for sentinel-2 to accomodate UINT16. But if for example sunAzimuthAngles is also requested (which only offers Float32) switch to Float32 as output. This info also comes from sentinelhub data_collections) 

A basic evalscript can look like this.

"""//VERSION=3

bands = ["B03", "B11", "SCL"]
units = ["DN", "DN", "DN"]
sampleType = "UINT16"

function setup() {
    return {
        input: [{
                bands: bands,
                units: units
            }],
        output: {
            bands: bands.length,
            sampleType: sampleType

        },
        mosaicking: "SIMPLE"
    }
}

function evaluatePixel(samples) {
    return bands.map(band => samples[band]);
}
"""

The dask xarray would just define those requests for each chunk. 

This means that we probably need to have bands as a dimension and not as separate dataarrays? Please explore if this is true. 

## Conclusion (implemented in `sh_datacube.py`)

Yes — bands as a dimension is the right model. One Process API request returns
all requested bands as a single `(y, x, band)` array, so the natural unit of
lazy computation is a `(1, n_bands, y, x)` dask chunk of an array with a `band`
dimension. Separate DataArrays would either issue one request per band (breaking
"1 request per timestamp") or require a shared compute xarray can't express
across variables.

A single request also carries a single output `sampleType`, so all bands share
one dtype. We pick the smallest dtype that represents every requested band: for
each band choose the unit whose output type is smallest, then the response dtype
is the max of those (DN-only S2 → `UINT16`; adding e.g. `sunAzimuthAngles`,
which is only Float32, promotes the whole cube to `FLOAT32`). Band/unit/dtype
metadata comes straight from `DataCollection.bands` + `.metabands`.

`.to_dataset(dim="band")` gives a per-variable `Dataset` view when wanted (dtype
stays uniform).



