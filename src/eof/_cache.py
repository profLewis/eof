"""NPZ caching for downloaded EO band data."""

import os
import numpy as np


def get_cache_path(item_id: str, data_folder: str, sensor: str = None) -> str:
    """Return the local cache file path for a STAC item.

    Args:
        item_id: STAC item ID or GEE image ID.
        data_folder: Cache directory.
        sensor: Optional sensor name prefix (e.g. "landsat").
            If None, uses legacy format compatible with S2 caches.
    """
    safe_id = item_id.replace('/', '_').replace('\\', '_')
    if sensor and sensor != "sentinel2":
        return os.path.join(data_folder, f"{sensor}_{safe_id}.npz")
    return os.path.join(data_folder, f"{safe_id}.npz")


def save_to_cache(cache_path: str, band_data: np.ndarray, qa_data: np.ndarray,
                  geotransform: tuple, crs: str):
    """Save cropped integer bands + QA to local cache."""
    np.savez_compressed(
        cache_path,
        band_data=band_data,
        scl_data=qa_data,  # keep key name for backward compat with S2 caches
        geotransform=np.array(geotransform),
        crs=np.array(crs),
    )


def load_from_cache(cache_path: str):
    """Load cached band data.

    Returns:
        tuple: (band_data, qa_data, geotransform, crs)
    """
    f = np.load(cache_path, allow_pickle=True)
    return (
        f['band_data'],
        f['scl_data'],
        tuple(f['geotransform']),
        str(f['crs']),
    )
