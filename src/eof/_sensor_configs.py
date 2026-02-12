"""Sensor-specific configuration for multi-sensor EO data retrieval.

Each SensorConfig captures the intrinsic properties of a sensor:
band names, native resolutions, DN-to-reflectance conversion,
cloud masking, and uncertainty estimation.
"""

from dataclasses import dataclass
from typing import Callable, Dict, Tuple, List

import numpy as np

from eof._cloud_masks import (
    mask_landsat_qa_pixel,
    mask_modis_state,
    mask_viirs_qf,
    mask_s3_olci,
)


# -----------------------------------------------------------------------
# DN-to-reflectance conversion functions
# -----------------------------------------------------------------------

def _s2_dn_to_reflectance(dn: np.ndarray, metadata: dict = None) -> np.ndarray:
    """Sentinel-2 DN to reflectance. Handled separately by _scl.py."""
    # This is a placeholder — S2 uses the existing pipeline in _scl.py
    # with processing baseline logic. Called only from the generic path.
    from eof._scl import dn_to_reflectance
    baseline = (metadata or {}).get("processing_baseline", "05.00")
    return dn_to_reflectance(dn, baseline)


def _landsat_dn_to_reflectance(dn: np.ndarray, metadata: dict = None) -> np.ndarray:
    """Landsat Collection 2 Level 2 SR: scale=0.0000275, offset=-0.2."""
    refl = dn.astype(np.float32) * 0.0000275 - 0.2
    refl = np.clip(refl, 0.0, None)
    refl[dn == 0] = np.nan  # fill value
    return refl


def _modis_dn_to_reflectance(dn: np.ndarray, metadata: dict = None) -> np.ndarray:
    """MODIS MOD09GA / MOD09A1: scale=0.0001, valid range [-100, 16000]."""
    refl = dn.astype(np.float32) * 0.0001
    refl = np.clip(refl, 0.0, None)
    # Fill value is typically -28672 or 32767
    refl[(dn < -100) | (dn > 16000)] = np.nan
    return refl


def _viirs_dn_to_reflectance(dn: np.ndarray, metadata: dict = None) -> np.ndarray:
    """VIIRS VNP09GA: scale=0.0001, valid range [-100, 16000]."""
    refl = dn.astype(np.float32) * 0.0001
    refl = np.clip(refl, 0.0, None)
    refl[(dn < -100) | (dn > 16000)] = np.nan
    return refl


def _s3_olci_dn_to_reflectance(dn: np.ndarray, metadata: dict = None) -> np.ndarray:
    """Sentinel-3 OLCI: TOA radiance. Simple scaling as placeholder.

    GEE provides radiance in W/m2/sr/um. A full conversion would need
    solar irradiance per band + SZA. We apply a simple scaling here
    that gives approximate TOA reflectance.
    """
    # GEE OLCI values are already scaled radiances
    # Approximate TOA reflectance: radiance * pi / (solar_irradiance * cos(SZA))
    # For a first approximation, we just use a rough scaling
    refl = dn.astype(np.float32) * 0.01  # rough scaling
    refl = np.clip(refl, 0.0, None)
    return refl


# -----------------------------------------------------------------------
# Uncertainty functions
# -----------------------------------------------------------------------

def _default_uncertainty(reflectance: np.ndarray) -> np.ndarray:
    """Default uncertainty: 10% of reflectance value."""
    return np.abs(reflectance) * 0.1


# -----------------------------------------------------------------------
# SensorConfig dataclass
# -----------------------------------------------------------------------

@dataclass(frozen=True)
class SensorConfig:
    """Intrinsic properties of a satellite sensor."""
    name: str                                # "sentinel2", "landsat", etc.
    band_names: tuple                        # output band names
    resolution_groups: Dict[int, List[int]]  # {native_res_m: [band_indices]}
    target_resolution: int                   # output resolution (10m)
    dn_to_reflectance: Callable              # (dn_array, metadata) -> float32
    cloud_mask_fn: Callable                  # (qa_array) -> bool mask
    uncertainty_fn: Callable                 # (refl_array) -> unc array
    qa_dtype: type                           # numpy dtype for QA band


# -----------------------------------------------------------------------
# Pre-built sensor configs
# -----------------------------------------------------------------------

SENTINEL2_CONFIG = SensorConfig(
    name="sentinel2",
    band_names=("B02", "B03", "B04", "B05", "B06", "B07",
                "B08", "B8A", "B11", "B12"),
    resolution_groups={
        10: [0, 1, 2, 6],      # B02, B03, B04, B08
        20: [3, 4, 5, 7, 8, 9], # B05, B06, B07, B8A, B11, B12
    },
    target_resolution=10,
    dn_to_reflectance=_s2_dn_to_reflectance,
    cloud_mask_fn=None,  # S2 uses SCL, not a simple bitmask
    uncertainty_fn=_default_uncertainty,
    qa_dtype=np.uint8,  # SCL is uint8
)

LANDSAT_CONFIG = SensorConfig(
    name="landsat",
    band_names=("SR_B1", "SR_B2", "SR_B3", "SR_B4",
                "SR_B5", "SR_B6", "SR_B7"),
    resolution_groups={
        30: [0, 1, 2, 3, 4, 5, 6],  # all bands at 30m
    },
    target_resolution=10,
    dn_to_reflectance=_landsat_dn_to_reflectance,
    cloud_mask_fn=mask_landsat_qa_pixel,
    uncertainty_fn=_default_uncertainty,
    qa_dtype=np.uint16,
)

MODIS_CONFIG = SensorConfig(
    name="modis",
    band_names=("sur_refl_b01", "sur_refl_b02", "sur_refl_b03",
                "sur_refl_b04", "sur_refl_b05", "sur_refl_b06",
                "sur_refl_b07"),
    resolution_groups={
        250: [0, 1],           # bands 1-2 at 250m
        500: [2, 3, 4, 5, 6],  # bands 3-7 at 500m
    },
    target_resolution=10,
    dn_to_reflectance=_modis_dn_to_reflectance,
    cloud_mask_fn=mask_modis_state,
    uncertainty_fn=_default_uncertainty,
    qa_dtype=np.uint16,
)

VIIRS_CONFIG = SensorConfig(
    name="viirs",
    band_names=("I1", "I2", "I3", "M1", "M2", "M3", "M4",
                "M5", "M7", "M8", "M10", "M11"),
    resolution_groups={
        500: [0, 1, 2],                  # I-bands at ~375m (served at 500m)
        1000: [3, 4, 5, 6, 7, 8, 9, 10], # M-bands at ~750m (served at 1000m)
    },
    target_resolution=10,
    dn_to_reflectance=_viirs_dn_to_reflectance,
    cloud_mask_fn=mask_viirs_qf,
    uncertainty_fn=_default_uncertainty,
    qa_dtype=np.uint16,
)

S3_OLCI_CONFIG = SensorConfig(
    name="s3olci",
    band_names=tuple(f"Oa{i:02d}" for i in range(1, 22)),  # Oa01..Oa21
    resolution_groups={
        300: list(range(21)),  # all 21 bands at 300m
    },
    target_resolution=10,
    dn_to_reflectance=_s3_olci_dn_to_reflectance,
    cloud_mask_fn=mask_s3_olci,
    uncertainty_fn=_default_uncertainty,
    qa_dtype=np.uint32,
)


SENSOR_CONFIGS = {
    "sentinel2": SENTINEL2_CONFIG,
    "landsat": LANDSAT_CONFIG,
    "modis": MODIS_CONFIG,
    "viirs": VIIRS_CONFIG,
    "s3olci": S3_OLCI_CONFIG,
}


def get_sensor_config(sensor: str) -> SensorConfig:
    """Get the SensorConfig for a named sensor."""
    if sensor not in SENSOR_CONFIGS:
        raise ValueError(
            f"Unknown sensor '{sensor}'. "
            f"Available: {list(SENSOR_CONFIGS.keys())}"
        )
    return SENSOR_CONFIGS[sensor]
