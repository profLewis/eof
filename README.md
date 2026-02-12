# eof - EO Fetch

Download and cache multi-sensor Earth Observation data from multiple platforms.

[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/profLewis/eof/blob/main/notebooks/demo.ipynb)

## Supported Sensors

| Sensor | `sensor=` | Bands | Native Resolution | Platforms |
|--------|-----------|-------|-------------------|-----------|
| Sentinel-2 L2A | `"sentinel2"` | 10 (B02-B12) | 10m / 20m | AWS, CDSE, Planetary, Earthdata, GEE |
| Landsat 8/9 C2 L2 | `"landsat"` | 7 (SR_B1-B7) | 30m | AWS, Planetary, Earthdata, GEE |
| MODIS MOD09GA | `"modis"` | 7 (bands 1-7) | 250m / 500m | Planetary*, Earthdata, GEE |
| VIIRS VNP09GA | `"viirs"` | 12 (I + M bands) | 500m / 1000m | Earthdata, GEE |
| Sentinel-3 OLCI | `"s3olci"` | 21 (Oa01-Oa21) | 300m | GEE |

\* Planetary MODIS is 8-day composite (MOD09A1), not daily. Earthdata and GEE provide daily MOD09GA.

All data is resampled to a common **10m grid** aligned with Sentinel-2, with **footprint ID maps** that identify which coarse-resolution pixel each 10m pixel belongs to. Each result includes **spectral response functions** (bandpass data) for the sensor.

## Data Platforms

| Platform | `source=` | Auth Required | Cost | Speed |
|----------|-----------|---------------|------|-------|
| [AWS Earth Search](https://earth-search.aws.element84.com/v1) | `"aws"` | None (S2) / AWS credentials (Landsat) | Free (S2) / Requester-pays (Landsat) | Fast |
| [CDSE](https://dataspace.copernicus.eu) | `"cdse"` | [S3 keys](https://eodata.dataspace.copernicus.eu) or [login](https://identity.dataspace.copernicus.eu) | Free | Moderate |
| [Planetary Computer](https://planetarycomputer.microsoft.com/) | `"planetary"` | Auto ([pip install planetary-computer](https://pypi.org/project/planetary-computer/)) | Free | Fast |
| [NASA Earthdata](https://www.earthdata.nasa.gov/) | `"earthdata"` | [Earthdata Login](https://urs.earthdata.nasa.gov/) | Free | Fast |
| [Google Earth Engine](https://earthengine.google.com/) | `"gee"` | [GEE account](https://signup.earthengine.google.com/) | Free (non-commercial) | Fast |

**Note on AWS Landsat**: The USGS Landsat bucket on AWS is [requester-pays](https://docs.aws.amazon.com/AmazonS3/latest/userguide/RequesterPaysBuckets.html), meaning your AWS account is billed for data transfer. Sentinel-2 on AWS is free and requires no credentials. For free Landsat access, use Planetary Computer, Earthdata, or GEE instead.

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
print(result.bandpass.keys())     # dict_keys(['wavelength_nm', 'response', ...])
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

### Benchmark platforms

```python
# Time all available sensor x platform combinations
timings = eof.benchmark_sources()
# Or test specific sensors:
timings = eof.benchmark_sources(sensors=['sentinel2', 'landsat'])
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
| `bandpass` | dict | Spectral response functions (see below) |

### `eof.get_multi_sensor_data(sensors, start_date, end_date, geojson_path, ...)`

Fetch multiple sensors concurrently. Returns `dict[str, EOResult]`.

### `eof.get_eo_data_fastest(sensor, start_date, end_date, geojson_path, ...)`

Race all available platforms, return result from fastest.

### Utility Functions

- `eof.get_available_sources()` -- list platforms with working credentials
- `eof.select_data_source(geojson_path=None, probe=False)` -- pick the best S2 source
- `eof.benchmark_sources(geojson_path=None, sensors=None)` -- time all sensor x platform combos
- `eof.probe_download_speed(geojson_path, sources=None)` -- benchmark S2 per platform
- `eof.print_config_status()` -- show credential status
- `eof.load_srf(sensor, satellite=None)` -- load spectral response functions
- `eof.get_srf_summary(sensor)` -- quick center wavelength / FWHM lookup
- `eof.get_dataset_info(sensor, platform)` -- dataset version info

### Constants

- `eof.SENSORS` -- list of supported sensor names
- `eof.SENSOR_PLATFORMS` -- dict mapping sensor -> list of available platforms
- `eof.TEST_GEOJSON` -- path to bundled South African wheat field GeoJSON
- `eof.TEST_DATA_DIR` -- directory containing test data

## Spectral Response Functions (Bandpass)

Each `EOResult` includes a `bandpass` dict with the sensor's spectral response functions:

```python
result = eof.get_eo_data('sentinel2', ...)
bp = result.bandpass
print(bp['wavelength_nm'].shape)         # (2251,) — 350..2600 nm at 1nm
print(bp['response'].shape)              # (10, 2251) — per-band response
print(bp['center_wavelength_nm'])        # (10,) — center wavelengths
print(bp['fwhm_nm'])                     # (10,) — full width at half max
print(bp['band_names'])                  # ('B02', 'B03', ..., 'B12')
```

You can also load SRF data independently:

```python
srf = eof.load_srf('landsat', satellite='9')  # full response curves
summary = eof.get_srf_summary('modis')         # just center + FWHM
```

The bundled SRF data uses Gaussian approximations from published center wavelengths and FWHM. For higher fidelity, replace with measured SRFs via `scripts/build_srf_data.py`.

## Footprint ID Maps

When sensors have coarser native resolution than the 10m output grid, each `EOResult` includes **footprint ID maps** in `result.footprints`. These integer arrays (at 10m resolution) identify which coarse-resolution pixel each 10m pixel belongs to. This enables multi-scale data assimilation.

```python
result = eof.get_eo_data('landsat', ...)
fp = result.footprints[30]  # 30m footprint IDs
print(fp.shape)   # (H, W) — same as reflectance spatial dims
print(fp.dtype)   # uint8 (auto-selected: uint8/uint16/uint32)
```

**Edge exclusion**: Footprints where less than 50% of the coarse pixel falls inside the field boundary are marked invalid (set to the dtype's max value, e.g. 255 for uint8). This prevents edge artifacts in assimilation.

Sensors with multiple native resolutions get multiple footprint maps:

| Sensor | Footprint keys | Example |
|--------|---------------|---------|
| Sentinel-2 | `{10, 20}` | 10m bands + 20m bands |
| Landsat | `{30}` | All bands at 30m |
| MODIS | `{250, 500}` | Bands 1-2 at 250m, bands 3-7 at 500m |
| VIIRS | `{500, 1000}` | I-bands at 500m, M-bands at 1000m |
| S3 OLCI | `{300}` | All bands at 300m |

The dtype is automatically selected: `uint8` if <= 254 footprints, `uint16` if <= 65534, else `uint32`.

## Dataset Versions

Collection IDs and version numbers for each sensor x platform are stored in
`src/eof/data/dataset_versions.json`. To check for newer dataset versions:

```bash
python scripts/update_dataset_versions.py           # report only
python scripts/update_dataset_versions.py --update   # apply updates
```

The script queries each STAC endpoint to find available collections and suggests
updates when newer versions are found.

## Credential Management

Credentials are stored in `~/.eof/config.json` with restricted file permissions.
Environment variables take priority over the config file.

### AWS Earth Search — No Auth Required

[Element 84 Earth Search](https://earth-search.aws.element84.com/v1) provides Sentinel-2 L2A as Cloud Optimized GeoTIFFs. **No account or credentials needed** — just install and run.

Note: Landsat on AWS is [requester-pays](https://docs.aws.amazon.com/AmazonS3/latest/userguide/RequesterPaysBuckets.html) and requires AWS credentials. For free Landsat, use Planetary Computer or Earthdata instead.

### Planetary Computer

[Planetary Computer](https://planetarycomputer.microsoft.com/) provides Sentinel-2, Landsat, and MODIS as COGs on Azure. Authentication is handled automatically by the `planetary-computer` package — no account needed.

```bash
pip install planetary-computer
```

### CDSE (Copernicus Data Space Ecosystem)

CDSE provides free Sentinel-2 L2A data via STAC API and S3 storage. Requires credentials.

**Method 1: S3 Access Keys (recommended)**

1. Register for a free account at [Copernicus Data Space Ecosystem](https://dataspace.copernicus.eu)
2. Go to the [S3 credentials dashboard](https://eodata.dataspace.copernicus.eu)
3. Click "Generate S3 credentials" to create an access key and secret key
4. Set them as environment variables:

```bash
export CDSE_S3_ACCESS_KEY="your-access-key"
export CDSE_S3_SECRET_KEY="your-secret-key"
```

**Method 2: Username / Password**

1. Register at [Copernicus Data Space Ecosystem](https://dataspace.copernicus.eu)
2. Set your login credentials as environment variables:

```bash
export CDSE_USERNAME="your-email@example.com"
export CDSE_PASSWORD="your-password"
```

You can also save credentials to the eof config file:

```python
import eof
config = eof.load_config()
config['cdse']['s3_access_key'] = 'your-access-key'
config['cdse']['s3_secret_key'] = 'your-secret-key'
eof.save_config(config)
```

### NASA Earthdata

[NASA Earthdata](https://www.earthdata.nasa.gov/) provides daily MODIS (MOD09GA), VIIRS (VNP09GA), and Landsat C2 L2. Requires a free Earthdata Login account.

1. Register at [urs.earthdata.nasa.gov](https://urs.earthdata.nasa.gov/)
2. Set credentials using one of three methods:

```bash
# Option 1: environment variables
export EARTHDATA_USERNAME="your-username"
export EARTHDATA_PASSWORD="your-password"

# Option 2: .netrc file
echo "machine urs.earthdata.nasa.gov login YOUR_USER password YOUR_PASS" >> ~/.netrc
chmod 600 ~/.netrc

# Option 3: eof config
python -c "
import eof
config = eof.load_config()
config['earthdata']['username'] = 'your-username'
config['earthdata']['password'] = 'your-password'
eof.save_config(config)
"
```

### Google Earth Engine

[Google Earth Engine](https://earthengine.google.com/) provides access to VIIRS and Sentinel-3 OLCI (GEE-only sensors), plus Sentinel-2, Landsat, and MODIS.

1. Create a [Google Earth Engine](https://signup.earthengine.google.com/) account
2. Run: `earthengine authenticate`
3. Follow the URL in the terminal, sign in, and paste the verification code

## Caching

Downloaded bands are cached as compressed NPZ files (integer DNs) in the `data_folder`.
Conversion to float reflectance happens on load, keeping cache files small.
If `data_folder=None`, a temporary directory is created (e.g. `/tmp/landsat_abc123/`).

Cache files include the sensor name in the filename to avoid collisions when
fetching multiple sensors to the same folder. Existing S2 cache files from
ARC are compatible.
