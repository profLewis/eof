"""Footprint ID map generation for multi-resolution sensors.

For each native resolution, generates an integer array at the target (10m)
grid where each pixel gets the ID of the coarse-resolution pixel it belongs
to. This enables multi-scale processing (e.g. data assimilation across
sensors with different native resolutions).

Edge footprints that extend significantly outside the valid field boundary
are marked as invalid (ID = -1 in signed representation, max value in
unsigned dtype).
"""

import math
import numpy as np


def compute_footprint_ids(geotransform, shape_hw, native_resolution,
                          target_resolution=10, mask=None,
                          min_valid_fraction=0.5):
    """Compute footprint ID map for a given native resolution.

    Each 10m pixel is assigned the ID of the coarse-resolution pixel it
    falls within. IDs are sequential (row-major) starting from 0.

    Footprints where less than `min_valid_fraction` of the pixels within
    the field boundary are valid (i.e. the coarse pixel extends mostly
    outside the field) are marked invalid with the maximum value for the
    chosen dtype.

    Args:
        geotransform: 6-tuple GDAL geotransform of the 10m grid.
        shape_hw: (H, W) shape of the 10m grid.
        native_resolution: Native pixel size in metres (e.g. 30 for Landsat).
        target_resolution: Output grid resolution in metres (default 10).
        mask: Optional (H, W) bool array. True where pixel is outside the
            field boundary (all-NaN). If None, no edge filtering is applied.
        min_valid_fraction: Minimum fraction of within-field pixels a
            footprint must contain to be kept. Default 0.5 (50%).

    Returns:
        np.ndarray of shape (H, W) with dtype auto-selected:
        uint8 if n_footprints <= 254, uint16 if <= 65534, else uint32.
        Invalid footprints are set to the dtype's max value.
    """
    H, W = shape_hw
    pixel_size = abs(geotransform[1])  # assumes square pixels

    # How many coarse pixels span the extent
    extent_x = W * pixel_size
    extent_y = H * pixel_size
    n_coarse_cols = math.ceil(extent_x / native_resolution)
    n_coarse_rows = math.ceil(extent_y / native_resolution)
    n_total = n_coarse_rows * n_coarse_cols

    # Select minimal dtype (reserve max value for invalid sentinel)
    if n_total <= 254:
        dtype = np.uint8
    elif n_total <= 65534:
        dtype = np.uint16
    else:
        dtype = np.uint32

    invalid_id = np.iinfo(dtype).max

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

    footprint_ids = footprint_ids.astype(dtype)

    # If native resolution equals target, no edge filtering needed
    if native_resolution <= target_resolution or mask is None:
        return footprint_ids

    # Edge footprint exclusion: for each footprint ID, compute the fraction
    # of pixels that are inside the field boundary (where mask is False).
    # If a footprint has less than min_valid_fraction of its pixels inside
    # the field, mark it as invalid.
    valid_pixels = ~mask  # True where inside field

    # Count total and valid pixels per footprint
    unique_ids = np.unique(footprint_ids)
    for fid in unique_ids:
        fp_mask = (footprint_ids == fid)
        n_in_field = np.count_nonzero(fp_mask & valid_pixels)
        n_total_fp = np.count_nonzero(fp_mask)
        if n_total_fp > 0 and (n_in_field / n_total_fp) < min_valid_fraction:
            footprint_ids[fp_mask] = invalid_id

    return footprint_ids


def compute_all_footprints(geotransform, shape_hw, resolution_groups,
                           target_resolution=10, mask=None,
                           min_valid_fraction=0.5):
    """Compute footprint ID maps for all resolution groups.

    Args:
        geotransform: 6-tuple GDAL geotransform of the 10m grid.
        shape_hw: (H, W) shape of the 10m grid.
        resolution_groups: dict mapping {resolution_m: [band_indices]}.
        target_resolution: Output grid resolution in metres (default 10).
        mask: Optional (H, W) bool mask. True where outside field boundary.
        min_valid_fraction: Minimum fraction of valid pixels per footprint.

    Returns:
        dict: {resolution_m: np.ndarray} footprint ID maps.
    """
    footprints = {}
    for resolution in resolution_groups:
        footprints[resolution] = compute_footprint_ids(
            geotransform, shape_hw, resolution, target_resolution,
            mask=mask, min_valid_fraction=min_valid_fraction,
        )
    return footprints
