"""Sensor × platform binding registry.

Maps each (sensor, platform) pair to the specific collection name,
asset keys, and QA band key needed to fetch data from that platform.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True)
class SensorPlatformBinding:
    """Glue between a sensor and a specific data platform."""
    sensor: str                    # "sentinel2", "landsat", etc.
    platform: str                  # "aws", "cdse", "planetary", "gee"
    collection: str                # STAC collection or GEE collection ID
    band_asset_keys: tuple         # asset keys in sensor band order
    qa_asset_key: str              # QA/cloud mask asset key
    tile_filter_keys: tuple = ()   # property keys for tile filtering
    extra_query: dict = field(default_factory=dict)  # additional STAC query params
    resample_alg: str = "bilinear" # default resampling for spectral bands
    qa_resample_alg: str = "nearest"  # resampling for QA band


# -----------------------------------------------------------------------
# Sentinel-2 bindings (existing sources)
# -----------------------------------------------------------------------

S2_AWS = SensorPlatformBinding(
    sensor="sentinel2", platform="aws",
    collection="sentinel-2-l2a",
    band_asset_keys=("blue", "green", "red", "rededge1", "rededge2",
                     "rededge3", "nir", "nir08", "swir16", "swir22"),
    qa_asset_key="scl",
    tile_filter_keys=("grid:code", "mgrs:grid_square", "s2:mgrs_tile"),
)

S2_CDSE = SensorPlatformBinding(
    sensor="sentinel2", platform="cdse",
    collection="sentinel-2-l2a",
    band_asset_keys=("B02_10m", "B03_10m", "B04_10m", "B05_20m", "B06_20m",
                     "B07_20m", "B08_10m", "B8A_20m", "B11_20m", "B12_20m"),
    qa_asset_key="SCL_20m",
    tile_filter_keys=("grid:code",),
)

S2_PLANETARY = SensorPlatformBinding(
    sensor="sentinel2", platform="planetary",
    collection="sentinel-2-l2a",
    band_asset_keys=("B02", "B03", "B04", "B05", "B06", "B07",
                     "B08", "B8A", "B11", "B12"),
    qa_asset_key="SCL",
    tile_filter_keys=("grid:code", "s2:mgrs_tile"),
)

S2_GEE = SensorPlatformBinding(
    sensor="sentinel2", platform="gee",
    collection="COPERNICUS/S2_SR_HARMONIZED",
    band_asset_keys=("B2", "B3", "B4", "B5", "B6", "B7",
                     "B8", "B8A", "B11", "B12"),
    qa_asset_key="QA60",
    tile_filter_keys=("MGRS_TILE",),
)


# -----------------------------------------------------------------------
# Landsat bindings
# -----------------------------------------------------------------------

LANDSAT_AWS = SensorPlatformBinding(
    sensor="landsat", platform="aws",
    collection="landsat-c2-l2",
    band_asset_keys=("coastal", "blue", "green", "red",
                     "nir08", "swir16", "swir22"),
    qa_asset_key="qa_pixel",
)

LANDSAT_PLANETARY = SensorPlatformBinding(
    sensor="landsat", platform="planetary",
    collection="landsat-c2-l2",
    band_asset_keys=("coastal", "blue", "green", "red",
                     "nir08", "swir16", "swir22"),
    qa_asset_key="qa_pixel",
)

LANDSAT_GEE = SensorPlatformBinding(
    sensor="landsat", platform="gee",
    collection="LANDSAT/LC09/C02/T1_L2",
    band_asset_keys=("SR_B1", "SR_B2", "SR_B3", "SR_B4",
                     "SR_B5", "SR_B6", "SR_B7"),
    qa_asset_key="QA_PIXEL",
)


# -----------------------------------------------------------------------
# MODIS bindings
# -----------------------------------------------------------------------

MODIS_PLANETARY = SensorPlatformBinding(
    sensor="modis", platform="planetary",
    collection="modis-09A1-061",  # 8-day composite (daily not available)
    band_asset_keys=("sur_refl_b01", "sur_refl_b02", "sur_refl_b03",
                     "sur_refl_b04", "sur_refl_b05", "sur_refl_b06",
                     "sur_refl_b07"),
    qa_asset_key="sur_refl_state_500m",
)

MODIS_GEE = SensorPlatformBinding(
    sensor="modis", platform="gee",
    collection="MODIS/061/MOD09GA",  # daily
    band_asset_keys=("sur_refl_b01", "sur_refl_b02", "sur_refl_b03",
                     "sur_refl_b04", "sur_refl_b05", "sur_refl_b06",
                     "sur_refl_b07"),
    qa_asset_key="state_1km",
)


# -----------------------------------------------------------------------
# VIIRS bindings (GEE only)
# -----------------------------------------------------------------------

VIIRS_GEE = SensorPlatformBinding(
    sensor="viirs", platform="gee",
    collection="NASA/VIIRS/002/VNP09GA",
    band_asset_keys=("I1", "I2", "I3", "M1", "M2", "M3", "M4",
                     "M5", "M7", "M8", "M10", "M11"),
    qa_asset_key="QF1",
)


# -----------------------------------------------------------------------
# Sentinel-3 OLCI bindings (GEE only)
# -----------------------------------------------------------------------

S3_OLCI_GEE = SensorPlatformBinding(
    sensor="s3olci", platform="gee",
    collection="COPERNICUS/S3/OLCI",
    band_asset_keys=tuple(f"Oa{i:02d}_radiance" for i in range(1, 22)),
    qa_asset_key="quality_flags",
)


# -----------------------------------------------------------------------
# Registry
# -----------------------------------------------------------------------

BINDINGS: Dict[tuple, SensorPlatformBinding] = {
    # Sentinel-2
    ("sentinel2", "aws"): S2_AWS,
    ("sentinel2", "cdse"): S2_CDSE,
    ("sentinel2", "planetary"): S2_PLANETARY,
    ("sentinel2", "gee"): S2_GEE,
    # Landsat
    ("landsat", "aws"): LANDSAT_AWS,
    ("landsat", "planetary"): LANDSAT_PLANETARY,
    ("landsat", "gee"): LANDSAT_GEE,
    # MODIS
    ("modis", "planetary"): MODIS_PLANETARY,
    ("modis", "gee"): MODIS_GEE,
    # VIIRS
    ("viirs", "gee"): VIIRS_GEE,
    # Sentinel-3 OLCI
    ("s3olci", "gee"): S3_OLCI_GEE,
}


def get_binding(sensor: str, platform: str) -> SensorPlatformBinding:
    """Get binding for a sensor+platform combination."""
    key = (sensor, platform)
    if key not in BINDINGS:
        available = [p for (s, p) in BINDINGS if s == sensor]
        raise ValueError(
            f"No binding for sensor='{sensor}' on platform='{platform}'. "
            f"Available platforms for {sensor}: {available}"
        )
    return BINDINGS[key]


def get_platforms_for_sensor(sensor: str) -> list:
    """List available platforms for a given sensor."""
    return [p for (s, p) in BINDINGS if s == sensor]


def get_sensors_for_platform(platform: str) -> list:
    """List available sensors on a given platform."""
    return [s for (s, p) in BINDINGS if p == platform]


# Convenience lookups
SENSOR_PLATFORMS = {
    sensor: get_platforms_for_sensor(sensor)
    for sensor in {"sentinel2", "landsat", "modis", "viirs", "s3olci"}
}

SUPPORTED_SENSORS = list(SENSOR_PLATFORMS.keys())
