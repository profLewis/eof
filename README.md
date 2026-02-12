# eof - EO Fetch

Download and cache Sentinel-2 L2A data from multiple sources.

[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/profLewis/eof/blob/main/notebooks/demo.ipynb)

## Data Sources

| Source | `source=` | Format | Auth Required | Speed |
|--------|-----------|--------|---------------|-------|
| [AWS Earth Search](https://earth-search.aws.element84.com/v1) | `'aws'` | COG | None | Fast |
| [CDSE](https://dataspace.copernicus.eu) | `'cdse'` | JP2 | [S3 keys](https://eodata.dataspace.copernicus.eu) or [login](https://identity.dataspace.copernicus.eu) | Moderate |
| [Planetary Computer](https://planetarycomputer.microsoft.com/) | `'planetary'` | COG | Auto ([pip install planetary-computer](https://pypi.org/project/planetary-computer/)) | Fast |
| [Google Earth Engine](https://earthengine.google.com/) | `'gee'` | GeoTIFF | [GEE account](https://signup.earthengine.google.com/) | Fast |

## Installation

```bash
pip install https://github.com/profLewis/eof/archive/refs/heads/main.zip
```

With optional dependencies:
```bash
pip install "eof[planetary]"   # adds planetary-computer
pip install "eof[gee]"         # adds earthengine-api
pip install "eof[all]"         # adds both
```

## Quick Start

```python
import eof

# Uses bundled test field and defaults to AWS (no credentials needed)
result = eof.get_s2_data(
    start_date='2022-07-15',
    end_date='2022-11-30',
    geojson_path=eof.TEST_GEOJSON,
)

print(result.reflectance.shape)  # (N_images, 10, H, W)
print(result.doys)               # day-of-year array
```

When no credentials are configured, `source='auto'` defaults to **AWS Earth Search** which requires no authentication.

## API

### `eof.get_s2_data(start_date, end_date, geojson_path, data_folder=None, source='auto', max_cloud_cover=80)`

Returns an `S2Result` dataclass:

| Field | Shape | Description |
|-------|-------|-------------|
| `reflectance` | (N, 10, H, W) | Float32 surface reflectance [0, 1] |
| `uncertainty` | (N, 10, H, W) | Float32 uncertainties (10% of reflectance) |
| `angles` | (3, N) | Float64 [SZA, VZA, RAA] in degrees |
| `doys` | (N,) | Int64 day of year |
| `mask` | (H, W) | Bool, True where all-NaN |
| `geotransform` | 6-tuple | GDAL geotransform |
| `crs` | str | WKT projection string |

### `eof.get_s2_official_data(start_date, end_date, geojson_path, S2_data_folder='./', source='auto')`

Backward-compatible wrapper that returns a 7-tuple instead of `S2Result`.

### Other Functions

- `eof.get_available_sources()` — list sources with working credentials
- `eof.select_data_source(geojson_path=None, probe=False)` — pick the best source
- `eof.probe_download_speed(geojson_path, sources=None)` — time each source
- `eof.print_config_status()` — show credential status

## Credential Management

Credentials are stored in `~/.eof/config.json` with restricted file permissions.
Environment variables take priority over the config file.

### CDSE Credentials

```bash
export CDSE_S3_ACCESS_KEY="your-key"
export CDSE_S3_SECRET_KEY="your-secret"
```

Or save to config:
```python
import eof
config = eof.load_config()
config['cdse']['s3_access_key'] = 'your-key'
config['cdse']['s3_secret_key'] = 'your-secret'
eof.save_config(config)
```

## Test Data

A South African wheat field GeoJSON is bundled with the package:

```python
import eof
print(eof.TEST_GEOJSON)   # path to SF_field.geojson
print(eof.TEST_DATA_DIR)  # directory containing test data
```

## Caching

Downloaded bands are cached as compressed NPZ files (integer DNs) in the `data_folder`.
Conversion to float reflectance happens on load, keeping cache files small.
If `data_folder=None`, a temporary directory is created (e.g. `/tmp/s2_abc123/`).

Existing cache files are compatible with ARC's NPZ format.
