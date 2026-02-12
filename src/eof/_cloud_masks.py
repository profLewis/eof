"""Sensor-specific cloud masking functions.

Each function takes a QA/mask array and returns a boolean mask
where True = cloudy/invalid pixel to be masked out.
"""

import numpy as np


def mask_landsat_qa_pixel(qa_pixel: np.ndarray) -> np.ndarray:
    """Decode Landsat Collection 2 QA_PIXEL bitmask.

    Masks: cloud (bit 3), cloud shadow (bit 4), cirrus (bit 2).
    Valid data must have fill bit (0) unset.

    Args:
        qa_pixel: uint16 QA_PIXEL band array.

    Returns:
        Boolean mask, True = cloudy/invalid.
    """
    fill = (qa_pixel & (1 << 0)) != 0
    cirrus = (qa_pixel & (1 << 2)) != 0
    cloud = (qa_pixel & (1 << 3)) != 0
    shadow = (qa_pixel & (1 << 4)) != 0
    return fill | cloud | shadow | cirrus


def mask_modis_state(state_1km: np.ndarray) -> np.ndarray:
    """Decode MODIS MOD09GA state_1km QA bitmask.

    Masks: cloud (bits 0-1 != 00), cloud shadow (bit 2),
    internal cloud (bit 10), adjacent cloud (bit 13).

    Args:
        state_1km: uint16 state_1km band array.

    Returns:
        Boolean mask, True = cloudy/invalid.
    """
    cloud_state = state_1km & 0b11  # bits 0-1: 00=clear, 01=cloudy, 10=mixed
    cloud = cloud_state != 0
    shadow = (state_1km & (1 << 2)) != 0
    internal_cloud = (state_1km & (1 << 10)) != 0
    adjacent_cloud = (state_1km & (1 << 13)) != 0
    return cloud | shadow | internal_cloud | adjacent_cloud


def mask_viirs_qf(qf1: np.ndarray) -> np.ndarray:
    """Decode VIIRS VNP09GA QF1 quality flag bitmask.

    Masks: cloud (bits 2-3 != 00), cloud shadow (bit 3),
    adjacent cloud (bit 4).

    Args:
        qf1: uint16 QF1 quality flag array.

    Returns:
        Boolean mask, True = cloudy/invalid.
    """
    cloud_mask = (qf1 & (0b11 << 2)) != 0   # bits 2-3: cloud confidence
    shadow = (qf1 & (1 << 3)) != 0
    adjacent = (qf1 & (1 << 4)) != 0
    return cloud_mask | shadow | adjacent


def mask_s3_olci(quality_flags: np.ndarray) -> np.ndarray:
    """Decode Sentinel-3 OLCI quality_flags bitmask.

    Masks: invalid (bit 25), cloud (bit 27), cloud_ambiguous (bit 28),
    cloud_margin (bit 29), snow_ice (bit 30).

    Args:
        quality_flags: uint32 quality_flags band array.

    Returns:
        Boolean mask, True = cloudy/invalid.
    """
    invalid = (quality_flags & (1 << 25)) != 0
    cloud = (quality_flags & (1 << 27)) != 0
    cloud_ambiguous = (quality_flags & (1 << 28)) != 0
    cloud_margin = (quality_flags & (1 << 29)) != 0
    snow_ice = (quality_flags & (1 << 30)) != 0
    return invalid | cloud | cloud_ambiguous | cloud_margin | snow_ice
