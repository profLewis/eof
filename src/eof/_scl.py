"""SCL cloud masking and DN-to-reflectance conversion."""

import numpy as np
from eof._types import SCL_MASK_VALUES, QUANTIFICATION_VALUE, BASELINE_OFFSET


def apply_scl_cloud_mask(reflectance: np.ndarray, scl: np.ndarray) -> np.ndarray:
    """
    Mask pixels using the Scene Classification Layer (SCL).

    Masked classes: 0=NoData, 1=Saturated, 3=CloudShadow,
    8=CloudMedium, 9=CloudHigh, 10=Cirrus, 11=Snow
    """
    mask = np.isin(scl, list(SCL_MASK_VALUES))
    reflectance[:, mask] = np.nan
    return reflectance


def dn_to_reflectance(dn: np.ndarray, processing_baseline: str) -> np.ndarray:
    """
    Convert integer DN values to float32 reflectance [0, 1].

    For processing baseline >= 04.00, applies BOA_ADD_OFFSET of -1000.
    """
    try:
        baseline_major = int(processing_baseline.split('.')[0])
    except (ValueError, AttributeError):
        baseline_major = 5  # default to recent baseline

    refl = dn.astype(np.float32)
    if baseline_major >= 4:
        refl = (refl + BASELINE_OFFSET) / QUANTIFICATION_VALUE
    else:
        refl = refl / QUANTIFICATION_VALUE

    refl = np.clip(refl, 0.0, None)
    return refl
