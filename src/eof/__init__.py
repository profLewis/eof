"""
eof - EO Fetch

Download and cache Sentinel-2 L2A data from multiple sources:
- AWS Earth Search (no auth required)
- CDSE (Copernicus Data Space Ecosystem)
- Microsoft Planetary Computer
- Google Earth Engine

Usage:
    import eof
    result = eof.get_s2_data('2022-07-15', '2022-11-30', 'field.geojson', source='aws')
    result.reflectance  # (N, 10, H, W) float32
"""

from pathlib import Path as _Path

from eof._types import S2Result, BAND_NAMES
from eof._credentials import (
    load_config, save_config, get_cdse_credentials,
    get_available_sources, select_data_source,
    probe_download_speed, print_config_status,
    get_data_source_preference, create_default_config,
    CONFIG_FILE,
)

# Bundled test data
TEST_DATA_DIR = _Path(__file__).parent / "test_data"
TEST_GEOJSON = str(TEST_DATA_DIR / "SF_field.geojson")


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


def get_s2_official_data(start_date, end_date, geojson_path,
                         S2_data_folder='./', source='auto'):
    """
    Backward-compatible wrapper matching ARC's legacy interface.

    Returns:
        tuple: (s2_refs, s2_uncs, s2_angles, doys, mask, geotransform, crs)
    """
    result = get_s2_data(start_date, end_date, geojson_path,
                         data_folder=S2_data_folder, source=source)
    return result.to_tuple()


def _get_reader(source, geojson_path=None):
    """Resolve source name to reader instance."""
    if source == "auto":
        resolved = select_data_source(geojson_path)
        print(f"Auto-selected data source: {resolved}")
        return _get_reader(resolved)
    elif source == "gee":
        from eof._gee_reader import GEEReader
        return GEEReader()
    elif source in ("aws", "cdse", "planetary"):
        from eof._stac_reader import STACReader
        from eof._source_configs import get_config
        return STACReader(get_config(source))
    else:
        raise ValueError(
            f"Unknown source '{source}'. "
            f"Must be 'aws', 'cdse', 'planetary', 'gee', or 'auto'."
        )
