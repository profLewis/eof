"""Parallel multi-platform fetch.

When multiple platforms can serve the same sensor, launch downloads
concurrently and return the first successful result.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed

from eof._types import EOResult
from eof._sensor_configs import get_sensor_config
from eof._platform_bindings import get_binding, get_platforms_for_sensor
from eof._credentials import get_available_sources
from eof._source_configs import SOURCE_CONFIGS, get_config


def _fetch_from_stac(sensor, platform, start_date, end_date, geojson_path,
                     data_folder, max_cloud_cover):
    """Attempt to fetch via STAC reader from a given platform."""
    from eof._stac_reader import STACReader

    source_config = get_config(platform)
    sensor_config = get_sensor_config(sensor)
    binding = get_binding(sensor, platform)
    reader = STACReader(source_config)
    return reader.fetch_sensor(
        sensor_config, binding,
        start_date, end_date, geojson_path,
        data_folder, max_cloud_cover,
    )


def _fetch_from_gee(sensor, start_date, end_date, geojson_path,
                    data_folder, max_cloud_cover):
    """Attempt to fetch via GEE reader."""
    from eof._gee_reader import GEEReader

    reader = GEEReader()
    return reader.fetch_sensor(
        sensor, start_date, end_date, geojson_path,
        data_folder, max_cloud_cover,
    )


def fetch_fastest(sensor: str, start_date: str, end_date: str,
                  geojson_path: str, data_folder: str = None,
                  max_cloud_cover: int = 80,
                  platforms: list = None) -> EOResult:
    """
    Fetch from all available platforms concurrently, return first success.

    Args:
        sensor: Sensor name (e.g. "landsat", "modis").
        start_date: Start date 'YYYY-MM-DD'.
        end_date: End date 'YYYY-MM-DD'.
        geojson_path: Path to GeoJSON field boundary.
        data_folder: Cache directory.
        max_cloud_cover: Max cloud cover percentage.
        platforms: List of platforms to try. None = all available.

    Returns:
        EOResult from the first platform to succeed.
    """
    if platforms is None:
        all_platforms = get_platforms_for_sensor(sensor)
        available = get_available_sources()
        # Filter to platforms that are both supported and have credentials
        platforms = [p for p in all_platforms if p in available]
        if not platforms:
            # Fall back to platforms that don't need credentials
            no_auth = {"aws"}
            platforms = [p for p in all_platforms if p in no_auth]
        if not platforms:
            raise RuntimeError(
                f"No available platforms for sensor '{sensor}'. "
                f"Supported platforms: {all_platforms}"
            )

    if len(platforms) == 1:
        platform = platforms[0]
        if platform == "gee":
            return _fetch_from_gee(
                sensor, start_date, end_date, geojson_path,
                data_folder, max_cloud_cover,
            )
        else:
            return _fetch_from_stac(
                sensor, platform, start_date, end_date, geojson_path,
                data_folder, max_cloud_cover,
            )

    # Race multiple platforms
    print(f"Racing {len(platforms)} platforms for {sensor}: {platforms}")
    with ThreadPoolExecutor(max_workers=len(platforms)) as executor:
        futures = {}
        for platform in platforms:
            if platform == "gee":
                future = executor.submit(
                    _fetch_from_gee,
                    sensor, start_date, end_date, geojson_path,
                    data_folder, max_cloud_cover,
                )
            else:
                future = executor.submit(
                    _fetch_from_stac,
                    sensor, platform, start_date, end_date, geojson_path,
                    data_folder, max_cloud_cover,
                )
            futures[future] = platform

        errors = {}
        for future in as_completed(futures):
            platform = futures[future]
            try:
                result = future.result()
                # Cancel remaining futures
                for other in futures:
                    if other is not future:
                        other.cancel()
                print(f"Winner: {platform}")
                return result
            except Exception as e:
                errors[platform] = e
                print(f"  {platform} failed: {e}")

    # All failed
    msg = "; ".join(f"{p}: {e}" for p, e in errors.items())
    raise RuntimeError(f"All platforms failed for {sensor}: {msg}")
