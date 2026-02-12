"""Spectral response function (bandpass) data for all supported sensors.

Provides per-band spectral response curves, center wavelengths, and FWHM
values. SRF data is bundled as a compressed .npz file (~38 KB).
"""

import numpy as np
from pathlib import Path

_SRF_FILE = Path(__file__).parent / "data" / "srf.npz"
_SRF_CACHE = None


def _load_srf_data():
    """Lazy-load the SRF data file."""
    global _SRF_CACHE
    if _SRF_CACHE is None:
        _SRF_CACHE = dict(np.load(_SRF_FILE, allow_pickle=False))
    return _SRF_CACHE


# Mapping from sensor name + optional satellite to npz keys
_SENSOR_MAP = {
    # (sensor, satellite) -> (response_key, center_key, fwhm_key)
    ("sentinel2", "2a"):  ("sentinel2a", "sentinel2a_center", "sentinel2a_fwhm"),
    ("sentinel2", "2b"):  ("sentinel2b", "sentinel2b_center", "sentinel2b_fwhm"),
    ("sentinel2", None):  ("sentinel2a", "sentinel2a_center", "sentinel2a_fwhm"),
    ("landsat", "8"):     ("landsat8", "landsat8_center", "landsat8_fwhm"),
    ("landsat", "9"):     ("landsat9", "landsat9_center", "landsat9_fwhm"),
    ("landsat", None):    ("landsat9", "landsat9_center", "landsat9_fwhm"),
    ("modis", None):      ("modis", "modis_center", "modis_fwhm"),
    ("viirs", None):      (("viirs_i", "viirs_m"),
                           ("viirs_i_center", "viirs_m_center"),
                           ("viirs_i_fwhm", "viirs_m_fwhm")),
    ("s3olci", "a"):      ("s3olci_a", "s3olci_center", "s3olci_fwhm"),
    ("s3olci", "b"):      ("s3olci_b", "s3olci_center", "s3olci_fwhm"),
    ("s3olci", None):     ("s3olci_a", "s3olci_center", "s3olci_fwhm"),
}

# Band names per sensor (matching _sensor_configs.py order)
_BAND_NAMES = {
    "sentinel2": ("B02", "B03", "B04", "B05", "B06", "B07",
                  "B08", "B8A", "B11", "B12"),
    "landsat":   ("SR_B1", "SR_B2", "SR_B3", "SR_B4",
                  "SR_B5", "SR_B6", "SR_B7"),
    "modis":     ("sur_refl_b01", "sur_refl_b02", "sur_refl_b03",
                  "sur_refl_b04", "sur_refl_b05", "sur_refl_b06",
                  "sur_refl_b07"),
    "viirs":     ("I1", "I2", "I3", "M1", "M2", "M3", "M4",
                  "M5", "M7", "M8", "M10", "M11"),
    "s3olci":    tuple(f"Oa{i:02d}" for i in range(1, 22)),
}


def load_srf(sensor: str, satellite: str = None) -> dict:
    """Load spectral response functions for a sensor.

    Args:
        sensor: "sentinel2", "landsat", "modis", "viirs", "s3olci"
        satellite: Optional satellite ID (e.g. "2a", "2b", "8", "9", "a", "b").
            Defaults to the primary satellite for each sensor.

    Returns:
        dict with:
            'wavelength_nm': (N,) float32 array — wavelength axis (350-2600 nm)
            'response': (n_bands, N) float32 array — normalized SRF per band
            'band_names': tuple of band name strings
            'center_wavelength_nm': (n_bands,) float32 array
            'fwhm_nm': (n_bands,) float32 array
    """
    data = _load_srf_data()

    key = (sensor, satellite)
    if key not in _SENSOR_MAP:
        key = (sensor, None)
    if key not in _SENSOR_MAP:
        available = [s for (s, _) in _SENSOR_MAP if s not in (None,)]
        raise ValueError(
            f"Unknown sensor '{sensor}'. Available: {sorted(set(available))}"
        )

    resp_key, center_key, fwhm_key = _SENSOR_MAP[key]

    # VIIRS has split I + M bands
    if isinstance(resp_key, tuple):
        response = np.concatenate([data[k] for k in resp_key], axis=0)
        centers = np.concatenate([data[k] for k in center_key])
        fwhms = np.concatenate([data[k] for k in fwhm_key])
    else:
        response = data[resp_key]
        centers = data[center_key]
        fwhms = data[fwhm_key]

    return {
        'wavelength_nm': data['wavelength_nm'],
        'response': response,
        'band_names': _BAND_NAMES[sensor],
        'center_wavelength_nm': centers,
        'fwhm_nm': fwhms,
    }


def get_srf_summary(sensor: str, satellite: str = None) -> dict:
    """Quick summary of spectral characteristics (no full response curves).

    Args:
        sensor: Sensor name.
        satellite: Optional satellite ID.

    Returns:
        dict with:
            'band_names': tuple of band name strings
            'center_wavelength_nm': (n_bands,) array
            'fwhm_nm': (n_bands,) array
    """
    srf = load_srf(sensor, satellite)
    return {
        'band_names': srf['band_names'],
        'center_wavelength_nm': srf['center_wavelength_nm'],
        'fwhm_nm': srf['fwhm_nm'],
    }
