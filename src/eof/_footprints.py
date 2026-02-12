"""Footprint ID map generation for multi-resolution sensors.

For each native resolution, generates an integer array at the target (10m)
grid where each pixel gets the ID of the coarse-resolution pixel it belongs
to. This enables multi-scale processing (e.g. data assimilation across
sensors with different native resolutions).
"""

import math
import numpy as np


def compute_footprint_ids(geotransform, shape_hw, native_resolution,
                          target_resolution=10):
    """Compute footprint ID map for a given native resolution.

    Each 10m pixel is assigned the ID of the coarse-resolution pixel it
    falls within. IDs are sequential (row-major) starting from 0.

    Args:
        geotransform: 6-tuple GDAL geotransform of the 10m grid.
        shape_hw: (H, W) shape of the 10m grid.
        native_resolution: Native pixel size in metres (e.g. 30 for Landsat).
        target_resolution: Output grid resolution in metres (default 10).

    Returns:
        np.ndarray of shape (H, W) with dtype auto-selected:
        uint8 if n_footprints <= 255, uint16 if <= 65535, else uint32.
    """
    H, W = shape_hw
    pixel_size = abs(geotransform[1])  # assumes square pixels

    # How many coarse pixels span the extent
    extent_x = W * pixel_size
    extent_y = H * pixel_size
    n_coarse_cols = math.ceil(extent_x / native_resolution)
    n_coarse_rows = math.ceil(extent_y / native_resolution)
    n_total = n_coarse_rows * n_coarse_cols

    # Select minimal dtype
    if n_total <= 255:
        dtype = np.uint8
    elif n_total <= 65535:
        dtype = np.uint16
    else:
        dtype = np.uint32

    # Ratio of native to target resolution
    ratio = native_resolution / pixel_size

    # Build row and column index arrays
    cols = np.arange(W, dtype=np.float64)
    rows = np.arange(H, dtype=np.float64)

    coarse_col = np.floor(cols / ratio).astype(np.int64)
    coarse_row = np.floor(rows / ratio).astype(np.int64)

    # Clip to valid range (edge pixels)
    np.clip(coarse_col, 0, n_coarse_cols - 1, out=coarse_col)
    np.clip(coarse_row, 0, n_coarse_rows - 1, out=coarse_row)

    # Broadcast to full grid: ID = row * n_cols + col
    footprint_ids = (coarse_row[:, np.newaxis] * n_coarse_cols
                     + coarse_col[np.newaxis, :])

    return footprint_ids.astype(dtype)


def compute_all_footprints(geotransform, shape_hw, resolution_groups,
                           target_resolution=10):
    """Compute footprint ID maps for all resolution groups.

    Args:
        geotransform: 6-tuple GDAL geotransform of the 10m grid.
        shape_hw: (H, W) shape of the 10m grid.
        resolution_groups: dict mapping {resolution_m: [band_indices]}.
        target_resolution: Output grid resolution in metres (default 10).

    Returns:
        dict: {resolution_m: np.ndarray} footprint ID maps.
    """
    footprints = {}
    for resolution in resolution_groups:
        footprints[resolution] = compute_footprint_ids(
            geotransform, shape_hw, resolution, target_resolution,
        )
    return footprints
