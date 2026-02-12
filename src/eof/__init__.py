"""
eof - EO Fetch

Download and cache Earth Observation data from multiple sources and sensors:

Sensors: Sentinel-2, Landsat 8/9, MODIS, VIIRS, Sentinel-3 OLCI
Sources: AWS Earth Search, CDSE, Microsoft Planetary Computer, Google Earth Engine

Usage:
    import eof

    # Sentinel-2 (original API)
    result = eof.get_s2_data('2022-07-15', '2022-11-30', 'field.geojson', source='aws')
    result.reflectance  # (N, 10, H, W) float32

    # Any sensor (new API)
    result = eof.get_eo_data('landsat', '2022-07-15', '2022-11-30', 'field.geojson')
    result.footprints   # {30: array(H, W)} footprint ID maps
"""

from pathlib import Path as _Path

from eof._types import S2Result, EOResult, BAND_NAMES
from eof._credentials import (
    load_config, save_config, get_cdse_credentials,
    get_earthdata_credentials,
    get_available_sources, select_data_source,
    probe_download_speed, benchmark_sources,
    print_config_status,
    get_data_source_preference, create_default_config,
    CONFIG_FILE,
)
from eof._platform_bindings import (
    SUPPORTED_SENSORS as SENSORS,
    SENSOR_PLATFORMS,
    get_dataset_info,
)
from eof._bandpass import load_srf, get_srf_summary

# Bundled test data
TEST_DATA_DIR = _Path(__file__).parent / "test_data"
TEST_GEOJSON = str(TEST_DATA_DIR / "SF_field.geojson")


# -----------------------------------------------------------------------
# Sentinel-2 API (unchanged)
# -----------------------------------------------------------------------

def get_s2_data(start_date, end_date, geojson_path,
                data_folder=None, source="auto",
                max_cloud_cover=80):
    """
    Fetch Sentinel-2 L2A data for a field boundary.

    Args:
        start_date: Start date 'YYYY-MM-DD'.
        end_date: End date 'YYYY-MM-DD'.
        geojson_path: Path to GeoJSON file with field boundary.
        data_folder: Cache directory. None creates a temp dir.
        source: 'aws', 'cdse', 'planetary', 'gee', or 'auto'.
        max_cloud_cover: Max cloud cover percentage (default 80).

    Returns:
        S2Result dataclass with reflectance, uncertainty, angles, doys,
        mask, geotransform, crs.
    """
    reader = _get_reader(source, geojson_path)
    return reader.fetch(start_date, end_date, geojson_path,
                        data_folder, max_cloud_cover)



# -----------------------------------------------------------------------
# Multi-sensor API (new)
# -----------------------------------------------------------------------

def get_eo_data(sensor, start_date, end_date, geojson_path,
                data_folder=None, source="auto", max_cloud_cover=80):
    """
    Fetch EO data for any supported sensor.

    All data is resampled to 10m resolution. Footprint ID maps are
    included for multi-scale processing.

    Args:
        sensor: Sensor name — one of:
            "sentinel2", "landsat", "modis", "viirs", "s3olci"
        start_date: Start date 'YYYY-MM-DD'.
        end_date: End date 'YYYY-MM-DD'.
        geojson_path: Path to GeoJSON file with field boundary.
        data_folder: Cache directory. None creates a temp dir.
        source: Platform — 'aws', 'cdse', 'planetary', 'gee', or 'auto'.
            'auto' picks the best available platform for this sensor.
        max_cloud_cover: Max cloud cover percentage (default 80).

    Returns:
        EOResult dataclass with reflectance, uncertainty, angles, doys,
        mask, geotransform, crs, sensor, band_names, native_resolutions,
        footprints.
    """
    from eof._sensor_configs import get_sensor_config
    from eof._platform_bindings import get_binding, get_platforms_for_sensor

    sensor_config = get_sensor_config(sensor)

    if source == "auto":
        source = _select_sensor_platform(sensor, geojson_path)
        print(f"Auto-selected platform for {sensor}: {source}")

    if source == "gee":
        from eof._gee_reader import GEEReader
        reader = GEEReader()
        return reader.fetch_sensor(
            sensor, start_date, end_date, geojson_path,
            data_folder, max_cloud_cover,
        )
    else:
        from eof._stac_reader import STACReader
        from eof._source_configs import get_config
        binding = get_binding(sensor, source)

        # Check if this is an HDF-based Earthdata product
        dataset_info = get_dataset_info(sensor, source)
        fmt = dataset_info.get("format", "cog")
        if source == "earthdata" and fmt in ("hdf4", "hdf5"):
            from eof._earthdata_hdf_reader import EarthdataHDFReader
            reader = EarthdataHDFReader(get_config(source))
            return reader.fetch_sensor(
                sensor_config, binding,
                start_date, end_date, geojson_path,
                data_folder, max_cloud_cover,
            )

        reader = STACReader(get_config(source))
        return reader.fetch_sensor(
            sensor_config, binding,
            start_date, end_date, geojson_path,
            data_folder, max_cloud_cover,
        )


def get_multi_sensor_data(sensors, start_date, end_date, geojson_path,
                          data_folder=None, source="auto",
                          max_cloud_cover=80):
    """
    Fetch data from multiple sensors, all on the same 10m grid.

    Args:
        sensors: List of sensor names, e.g. ["sentinel2", "landsat"].
        start_date: Start date 'YYYY-MM-DD'.
        end_date: End date 'YYYY-MM-DD'.
        geojson_path: Path to GeoJSON file with field boundary.
        data_folder: Cache directory.
        source: Platform or 'auto'.
        max_cloud_cover: Max cloud cover percentage.

    Returns:
        dict: {sensor_name: EOResult} for each requested sensor.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results = {}

    def _fetch_one(sensor_name):
        return sensor_name, get_eo_data(
            sensor_name, start_date, end_date, geojson_path,
            data_folder, source, max_cloud_cover,
        )

    with ThreadPoolExecutor(max_workers=len(sensors)) as executor:
        futures = {
            executor.submit(_fetch_one, s): s
            for s in sensors
        }
        for future in as_completed(futures):
            sensor_name = futures[future]
            try:
                name, result = future.result()
                results[name] = result
            except Exception as e:
                print(f"Warning: {sensor_name} fetch failed: {e}")

    return results


def get_eo_data_fastest(sensor, start_date, end_date, geojson_path,
                        data_folder=None, max_cloud_cover=80,
                        platforms=None):
    """
    Fetch from all available platforms concurrently, return first success.

    Args:
        sensor: Sensor name.
        start_date: Start date 'YYYY-MM-DD'.
        end_date: End date 'YYYY-MM-DD'.
        geojson_path: Path to GeoJSON field boundary.
        data_folder: Cache directory.
        max_cloud_cover: Max cloud cover percentage.
        platforms: List of platforms to race. None = all available.

    Returns:
        EOResult from the first platform to succeed.
    """
    from eof._parallel_fetch import fetch_fastest
    return fetch_fastest(
        sensor, start_date, end_date, geojson_path,
        data_folder, max_cloud_cover, platforms,
    )


# -----------------------------------------------------------------------
# Internal helpers
# -----------------------------------------------------------------------

def _get_reader(source, geojson_path=None):
    """Resolve source name to S2 reader instance."""
    if source == "auto":
        resolved = select_data_source(geojson_path)
        print(f"Auto-selected data source: {resolved}")
        return _get_reader(resolved)
    elif source == "gee":
        from eof._gee_reader import GEEReader
        return GEEReader()
    elif source in ("aws", "cdse", "planetary", "earthdata"):
        from eof._stac_reader import STACReader
        from eof._source_configs import get_config
        return STACReader(get_config(source))
    else:
        raise ValueError(
            f"Unknown source '{source}'. "
            f"Must be 'aws', 'cdse', 'planetary', 'earthdata', 'gee', or 'auto'."
        )


def _select_sensor_platform(sensor, geojson_path=None):
    """Select the best available platform for a given sensor."""
    from eof._platform_bindings import get_platforms_for_sensor

    available_platforms = get_platforms_for_sensor(sensor)
    available_sources = get_available_sources()

    # Prefer STAC sources (faster) over GEE
    stac_priority = ["aws", "planetary", "cdse", "earthdata"]
    for platform in stac_priority:
        if platform in available_platforms and platform in available_sources:
            return platform

    # Fall back to GEE
    if "gee" in available_platforms and "gee" in available_sources:
        return "gee"

    # Last resort: try AWS (no auth needed)
    if "aws" in available_platforms:
        return "aws"

    raise RuntimeError(
        f"No available platform for sensor '{sensor}'. "
        f"Supported: {available_platforms}, "
        f"Available: {available_sources}"
    )
