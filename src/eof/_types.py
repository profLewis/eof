"""Result types and constants for eof."""

from dataclasses import dataclass
import numpy as np

# The 10 Sentinel-2 spectral bands in ARC order
BAND_NAMES = ('B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B11', 'B12')

# SCL classes to mask: NoData, Saturated, CloudShadow, CloudMedium, CloudHigh, Cirrus, Snow
SCL_MASK_VALUES = frozenset({0, 1, 3, 8, 9, 10, 11})

QUANTIFICATION_VALUE = 10000.0
BASELINE_OFFSET = -1000


@dataclass(frozen=True)
class S2Result:
    """Result of a Sentinel-2 data retrieval."""
    reflectance: np.ndarray     # (N_images, 10, H, W) float32 [0, 1]
    uncertainty: np.ndarray     # (N_images, 10, H, W) float32
    angles: np.ndarray          # (3, N_images) float64 [SZA, VZA, RAA] degrees
    doys: np.ndarray            # (N_images,) int64
    mask: np.ndarray            # (H, W) bool
    geotransform: tuple         # 6-tuple GDAL geotransform
    crs: str                    # WKT projection string

    def to_tuple(self):
        """Unpack to the 7-tuple expected by ARC's legacy interface."""
        return (self.reflectance, self.uncertainty, self.angles,
                self.doys, self.mask, self.geotransform, self.crs)


@dataclass(frozen=True)
class EOResult:
    """Result of a multi-sensor EO data retrieval.

    All reflectance data is resampled to 10m resolution at the S2 grid.
    Footprint ID maps identify which coarse-resolution pixel each 10m
    pixel belongs to, enabling multi-scale processing.
    """
    reflectance: np.ndarray       # (N_images, n_bands, H, W) float32 [0, 1]
    uncertainty: np.ndarray       # (N_images, n_bands, H, W) float32
    angles: np.ndarray            # (3, N_images) float64 [SZA, VZA, RAA] degrees
    doys: np.ndarray              # (N_images,) int64
    mask: np.ndarray              # (H, W) bool — True where all-NaN
    geotransform: tuple           # 6-tuple GDAL geotransform (10m grid)
    crs: str                      # WKT projection string
    sensor: str                   # "sentinel2", "landsat", "modis", "viirs", "s3olci"
    band_names: tuple             # e.g. ("B02", "B03", ...) or ("SR_B1", ...)
    native_resolutions: dict      # {resolution_m: [band_indices]}
    footprints: dict              # {resolution_m: np.ndarray} int ID maps (H, W)
    bandpass: dict = None         # {'wavelength_nm': (N,), 'response': (B,N),
                                  #  'band_names': tuple, 'center_wavelength_nm': (B,),
                                  #  'fwhm_nm': (B,)} — spectral response functions

    def to_s2result(self):
        """Convert to S2Result (drops sensor-specific metadata)."""
        return S2Result(
            reflectance=self.reflectance,
            uncertainty=self.uncertainty,
            angles=self.angles,
            doys=self.doys,
            mask=self.mask,
            geotransform=self.geotransform,
            crs=self.crs,
        )
