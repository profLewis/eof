"""NPZ caching for downloaded Sentinel-2 band data."""

import os
import numpy as np


def get_cache_path(item_id: str, data_folder: str) -> str:
    """Return the local cache file path for a STAC item."""
    safe_id = item_id.replace('/', '_').replace('\\', '_')
    return os.path.join(data_folder, f"{safe_id}.npz")


def save_to_cache(cache_path: str, band_data: np.ndarray, scl_data: np.ndarray,
                  geotransform: tuple, crs: str):
    """Save cropped integer bands + SCL to local cache."""
    np.savez_compressed(
        cache_path,
        band_data=band_data,
        scl_data=scl_data,
        geotransform=np.array(geotransform),
        crs=np.array(crs),
    )


def load_from_cache(cache_path: str):
    """Load cached band data."""
    f = np.load(cache_path, allow_pickle=True)
    return (
        f['band_data'],
        f['scl_data'],
        tuple(f['geotransform']),
        str(f['crs']),
    )
