# eof - EO Fetch

Download and cache multi-sensor Earth Observation data from multiple platforms.

[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/profLewis/eof/blob/main/notebooks/demo.ipynb)

## Supported Sensors

| Sensor | `sensor=` | Bands | Native Resolution | Platforms |
|--------|-----------|-------|-------------------|-----------|
| Sentinel-2 L2A | `"sentinel2"` | 10 (B02-B12) | 10m / 20m | AWS, CDSE, Planetary, GEE |
| Landsat 8/9 C2 L2 | `"landsat"` | 7 (SR_B1-B7) | 30m | AWS, Planetary, GEE |
| MODIS MOD09GA | `"modis"` | 7 (bands 1-7) | 250m / 500m | Planetary, GEE |
| VIIRS VNP09GA | `"viirs"` | 12 (I + M bands) | 500m / 1000m | GEE |
| Sentinel-3 OLCI | `"s3olci"` | 21 (Oa01-Oa21) | 300m | GEE |

All data is resampled to a common **10m grid** aligned with Sentinel-2, with **footprint ID maps** that identify which coarse-resolution pixel each 10m pixel belongs to.

## Data Platforms

| Platform | `source=` | Auth Required | Speed |
|----------|-----------|---------------|-------|
| [AWS Earth Search](https://earth-search.aws.element84.com/v1) | `"aws"` | None | Fast |
| [CDSE](https://dataspace.copernicus.eu) | `"cdse"` | [S3 keys](https://eodata.dataspace.copernicus.eu) or [login](https://identity.dataspace.copernicus.eu) | Moderate |
| [Planetary Computer](https://planetarycomputer.microsoft.com/) | `"planetary"` | Auto ([pip install planetary-computer](https://pypi.org/project/planetary-computer/)) | Fast |
| [Google Earth Engine](https://earthengine.google.com/) | `"gee"` | [GEE account](https://signup.earthengine.google.com/) | Fast |

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

### Sentinel-2 (original API)

```python
import eof

result = eof.get_s2_data(
    start_date='2022-07-15',
    end_date='2022-11-30',
    geojson_path=eof.TEST_GEOJSON,
)
print(result.reflectance.shape)  # (N, 10, H, W)
```

### Any sensor (multi-sensor API)

```python
import eof

# Landsat surface reflectance at 10m
result = eof.get_eo_data(
    'landsat',
    start_date='2022-07-15',
    end_date='2022-11-30',
    geojson_path=eof.TEST_GEOJSON,
)
print(result.reflectance.shape)   # (N, 7, H, W) float32
print(result.band_names)          # ('SR_B1', ..., 'SR_B7')
print(result.footprints)          # {30: array(H, W, dtype=uint8)}
```

### Multiple sensors at once

```python
results = eof.get_multi_sensor_data(
    ['sentinel2', 'landsat'],
    start_date='2022-07-15',
    end_date='2022-11-30',
    geojson_path=eof.TEST_GEOJSON,
)
for sensor, result in results.items():
    print(f"{sensor}: {result.reflectance.shape}")
```

### Race multiple platforms

```python
# Download from the fastest responding platform
result = eof.get_eo_data_fastest(
    'landsat',
    start_date='2022-07-15',
    end_date='2022-11-30',
    geojson_path=eof.TEST_GEOJSON,
)
```

## API Reference

### `eof.get_s2_data(start_date, end_date, geojson_path, data_folder=None, source='auto', max_cloud_cover=80)`

Fetch Sentinel-2 L2A data. Returns `S2Result`:

| Field | Shape | Description |
|-------|-------|-------------|
| `reflectance` | (N, 10, H, W) | Float32 surface reflectance [0, 1] |
| `uncertainty` | (N, 10, H, W) | Float32 uncertainties (10% of reflectance) |
| `angles` | (3, N) | Float64 [SZA, VZA, RAA] in degrees |
| `doys` | (N,) | Int64 day of year |
| `mask` | (H, W) | Bool, True where all-NaN |
| `geotransform` | 6-tuple | GDAL geotransform |
| `crs` | str | WKT projection string |

### `eof.get_eo_data(sensor, start_date, end_date, geojson_path, data_folder=None, source='auto', max_cloud_cover=80)`

Fetch data for any sensor. Returns `EOResult`:

| Field | Shape | Description |
|-------|-------|-------------|
| `reflectance` | (N, B, H, W) | Float32 surface reflectance [0, 1] |
| `uncertainty` | (N, B, H, W) | Float32 uncertainties |
| `angles` | (3, N) | Float64 [SZA, VZA, RAA] in degrees |
| `doys` | (N,) | Int64 day of year |
| `mask` | (H, W) | Bool, True where all-NaN |
| `geotransform` | 6-tuple | GDAL geotransform (10m grid) |
| `crs` | str | WKT projection string |
| `sensor` | str | Sensor name |
| `band_names` | tuple | Band names in order |
| `native_resolutions` | dict | {resolution_m: [band_indices]} |
| `footprints` | dict | {resolution_m: array(H, W)} footprint ID maps |

### `eof.get_multi_sensor_data(sensors, start_date, end_date, geojson_path, ...)`

Fetch multiple sensors concurrently. Returns `dict[str, EOResult]`.

### `eof.get_eo_data_fastest(sensor, start_date, end_date, geojson_path, ...)`

Race all available platforms, return result from fastest.

### `eof.get_s2_official_data(start_date, end_date, geojson_path, S2_data_folder='./', source='auto')`

Backward-compatible wrapper returning a 7-tuple.

### Utility Functions

- `eof.get_available_sources()` — list platforms with working credentials
- `eof.select_data_source(geojson_path=None, probe=False)` — pick the best S2 source
- `eof.probe_download_speed(geojson_path, sources=None)` — benchmark each platform
- `eof.print_config_status()` — show credential status

### Constants

- `eof.SENSORS` — list of supported sensor names
- `eof.SENSOR_PLATFORMS` — dict mapping sensor → list of available platforms
- `eof.TEST_GEOJSON` — path to bundled South African wheat field GeoJSON
- `eof.TEST_DATA_DIR` — directory containing test data

## Footprint ID Maps

When sensors have coarser native resolution than the 10m output grid, each `EOResult` includes **footprint ID maps** in `result.footprints`. These integer arrays (at 10m resolution) identify which coarse-resolution pixel each 10m pixel belongs to. This enables multi-scale data assimilation.

```python
result = eof.get_eo_data('landsat', ...)
fp = result.footprints[30]  # 30m footprint IDs
print(fp.shape)   # (H, W) — same as reflectance spatial dims
print(fp.dtype)   # uint8 (auto-selected: uint8/uint16/uint32)
```

Sensors with multiple native resolutions get multiple footprint maps:

| Sensor | Footprint keys | Example |
|--------|---------------|---------|
| Sentinel-2 | `{10, 20}` | 10m bands + 20m bands |
| Landsat | `{30}` | All bands at 30m |
| MODIS | `{250, 500}` | Bands 1-2 at 250m, bands 3-7 at 500m |
| VIIRS | `{500, 1000}` | I-bands at 500m, M-bands at 1000m |
| S3 OLCI | `{300}` | All bands at 300m |

The dtype is automatically selected: `uint8` if <= 255 footprints, `uint16` if <= 65535, else `uint32`.

## Credential Management

Credentials are stored in `~/.eof/config.json` with restricted file permissions.
Environment variables take priority over the config file.

### CDSE

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

### Google Earth Engine

```bash
earthengine authenticate
```

### Planetary Computer

```bash
pip install planetary-computer
```

No further configuration needed — URLs are signed automatically.

## Caching

Downloaded bands are cached as compressed NPZ files (integer DNs) in the `data_folder`.
Conversion to float reflectance happens on load, keeping cache files small.
If `data_folder=None`, a temporary directory is created (e.g. `/tmp/landsat_abc123/`).

Cache files include the sensor name in the filename to avoid collisions when
fetching multiple sensors to the same folder. Existing S2 cache files from
ARC are compatible.
