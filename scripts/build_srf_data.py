#!/usr/bin/env python3
"""Build spectral response function (SRF) data for all eof sensors.

Generates src/eof/data/srf.npz from published spectral parameters.

Uses Gaussian approximations based on official center wavelengths and FWHM
values. For higher fidelity, replace the Gaussian curves with measured SRF
data from:
  - Sentinel-2: ESA S2-SRF v3.1 (sentinel.esa.int)
  - Landsat 8/9: USGS RSR (landsat.gsfc.nasa.gov)
  - MODIS: NASA MCST (mcst.gsfc.nasa.gov)
  - VIIRS: NOAA NESDIS (ncc.nesdis.noaa.gov)
  - S3 OLCI: ESA (sentinel.esa.int)

Run: python scripts/build_srf_data.py
"""

import os
import numpy as np

# Common wavelength axis: 350-2600 nm at 1 nm resolution
WAVELENGTH_NM = np.arange(350, 2601, dtype=np.float64)


def gaussian_srf(center, fwhm, wavelength=WAVELENGTH_NM):
    """Generate a Gaussian spectral response function."""
    sigma = fwhm / (2.0 * np.sqrt(2.0 * np.log(2.0)))
    response = np.exp(-0.5 * ((wavelength - center) / sigma) ** 2)
    # Zero out values below 0.001 to keep clean profiles
    response[response < 0.001] = 0.0
    return response.astype(np.float32)


# -------------------------------------------------------------------------
# Sentinel-2A — ESA official central wavelengths and FWHM (nm)
# Source: S2-SRF v3.1, Sentinel-2A
# Band order: B02, B03, B04, B05, B06, B07, B08, B8A, B11, B12
# -------------------------------------------------------------------------
S2A_CENTER = [492.4, 559.8, 664.6, 704.1, 740.5, 782.8, 832.8, 864.7, 1613.7, 2202.4]
S2A_FWHM   = [ 66.0,  36.0,  31.0,  15.0,  15.0,  20.0, 106.0,  21.0,  91.0, 175.0]

S2B_CENTER = [492.1, 559.0, 665.0, 703.8, 739.1, 779.7, 833.0, 864.0, 1610.4, 2185.7]
S2B_FWHM   = [ 66.0,  36.0,  31.0,  16.0,  15.0,  20.0, 106.0,  22.0,  94.0, 185.0]

# -------------------------------------------------------------------------
# Landsat 8 (OLI) and Landsat 9 (OLI-2)
# Source: USGS spectral characteristics
# Band order: B1(coastal), B2(blue), B3(green), B4(red), B5(NIR), B6(SWIR1), B7(SWIR2)
# -------------------------------------------------------------------------
L8_CENTER = [443.0, 482.0, 561.5, 654.5, 865.0, 1608.5, 2200.5]
L8_FWHM   = [ 16.0,  60.0,  57.0,  37.0,  28.0,   85.0,  187.0]

L9_CENTER = [443.0, 482.0, 561.5, 654.5, 865.0, 1608.5, 2200.5]
L9_FWHM   = [ 16.0,  60.0,  57.0,  37.0,  28.0,   85.0,  187.0]

# -------------------------------------------------------------------------
# MODIS (Terra and Aqua) — bands 1-7
# Source: NASA MCST calibration parameters
# Band order: B1(red), B2(NIR), B3(blue), B4(green), B5(SWIR), B6(SWIR), B7(SWIR)
# -------------------------------------------------------------------------
MODIS_CENTER = [645.0, 858.5, 469.0, 555.0, 1240.0, 1640.0, 2130.0]
MODIS_FWHM   = [ 50.0,  35.0,  20.0,  20.0,   20.0,   24.0,   50.0]

# -------------------------------------------------------------------------
# VIIRS (Suomi NPP / NOAA-20)
# Source: NOAA NESDIS
# I-bands: I1, I2, I3
# M-bands: M1, M2, M3, M4, M5, M7, M8, M10, M11
# -------------------------------------------------------------------------
VIIRS_I_CENTER = [640.0, 862.0, 1610.0]
VIIRS_I_FWHM   = [ 80.0,  39.0,   60.0]

VIIRS_M_CENTER = [412.0, 445.0, 488.0, 555.0, 672.0, 865.0, 1240.0, 1610.0, 2250.0]
VIIRS_M_FWHM   = [ 20.0,  18.0,  20.0,  20.0,  20.0,  39.0,   20.0,   60.0,   50.0]

# -------------------------------------------------------------------------
# Sentinel-3 OLCI (A and B) — 21 bands
# Source: ESA OLCI spectral characterisation
# -------------------------------------------------------------------------
S3_OLCI_CENTER = [
    400.0, 412.5, 442.5, 490.0, 510.0, 560.0, 620.0, 665.0, 673.75,
    681.25, 708.75, 753.75, 761.25, 764.375, 767.5, 778.75, 865.0,
    885.0, 900.0, 940.0, 1020.0,
]
S3_OLCI_FWHM = [
    15.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 7.5,
    7.5, 10.0, 7.5, 2.5, 3.75, 2.5, 15.0, 20.0,
    10.0, 10.0, 20.0, 40.0,
]


def build_srf_array(centers, fwhms):
    """Build (n_bands, N_wavelength) SRF array from center/FWHM lists."""
    return np.stack([gaussian_srf(c, f) for c, f in zip(centers, fwhms)])


def main():
    data = {
        'wavelength_nm': WAVELENGTH_NM.astype(np.float32),
        # Sentinel-2
        'sentinel2a': build_srf_array(S2A_CENTER, S2A_FWHM),
        'sentinel2b': build_srf_array(S2B_CENTER, S2B_FWHM),
        # Landsat
        'landsat8': build_srf_array(L8_CENTER, L8_FWHM),
        'landsat9': build_srf_array(L9_CENTER, L9_FWHM),
        # MODIS
        'modis': build_srf_array(MODIS_CENTER, MODIS_FWHM),
        # VIIRS
        'viirs_i': build_srf_array(VIIRS_I_CENTER, VIIRS_I_FWHM),
        'viirs_m': build_srf_array(VIIRS_M_CENTER, VIIRS_M_FWHM),
        # Sentinel-3 OLCI
        's3olci_a': build_srf_array(S3_OLCI_CENTER, S3_OLCI_FWHM),
        's3olci_b': build_srf_array(S3_OLCI_CENTER, S3_OLCI_FWHM),
        # Center wavelengths and FWHM for quick lookup
        'sentinel2a_center': np.array(S2A_CENTER, dtype=np.float32),
        'sentinel2a_fwhm': np.array(S2A_FWHM, dtype=np.float32),
        'sentinel2b_center': np.array(S2B_CENTER, dtype=np.float32),
        'sentinel2b_fwhm': np.array(S2B_FWHM, dtype=np.float32),
        'landsat8_center': np.array(L8_CENTER, dtype=np.float32),
        'landsat8_fwhm': np.array(L8_FWHM, dtype=np.float32),
        'landsat9_center': np.array(L9_CENTER, dtype=np.float32),
        'landsat9_fwhm': np.array(L9_FWHM, dtype=np.float32),
        'modis_center': np.array(MODIS_CENTER, dtype=np.float32),
        'modis_fwhm': np.array(MODIS_FWHM, dtype=np.float32),
        'viirs_i_center': np.array(VIIRS_I_CENTER, dtype=np.float32),
        'viirs_i_fwhm': np.array(VIIRS_I_FWHM, dtype=np.float32),
        'viirs_m_center': np.array(VIIRS_M_CENTER, dtype=np.float32),
        'viirs_m_fwhm': np.array(VIIRS_M_FWHM, dtype=np.float32),
        's3olci_center': np.array(S3_OLCI_CENTER, dtype=np.float32),
        's3olci_fwhm': np.array(S3_OLCI_FWHM, dtype=np.float32),
    }

    out_path = os.path.join(
        os.path.dirname(__file__), '..', 'src', 'eof', 'data', 'srf.npz'
    )
    out_path = os.path.abspath(out_path)
    np.savez_compressed(out_path, **data)

    file_size = os.path.getsize(out_path)
    print(f"SRF data saved to {out_path}")
    print(f"File size: {file_size / 1024:.1f} KB")
    print(f"Wavelength range: {WAVELENGTH_NM[0]}-{WAVELENGTH_NM[-1]} nm ({len(WAVELENGTH_NM)} points)")
    print(f"Sensors: sentinel2a/b, landsat8/9, modis, viirs_i/m, s3olci_a/b")


if __name__ == "__main__":
    main()
