"""NASA Earthdata HDF reader for MODIS and VIIRS daily surface reflectance.

MODIS MOD09GA and VIIRS VNP09GA are served as HDF4/HDF5 files on Earthdata,
not as per-band COGs. This reader:
1. Uses CMR STAC to find items
2. Downloads the HDF files
3. Extracts subdatasets via GDAL
4. Resamples to 10m grid
"""

import os
import json
import tempfile
import threading
import numpy as np
from osgeo import gdal
import pystac_client
import shapely

from eof._types import EOResult
from eof._source_configs import SourceConfig
from eof._sensor_configs import SensorConfig
from eof._platform_bindings import SensorPlatformBinding
from eof._geojson import load_geojson
from eof._footprints import compute_all_footprints
from eof._bandpass import load_srf

gdal.PushErrorHandler('CPLQuietErrorHandler')


# MODIS HDF4 subdataset templates
_MODIS_BAND_SUBDATASETS = {
    "sur_refl_b01_1": ("MODIS_Grid_500m_2D", "sur_refl_b01_1"),
    "sur_refl_b02_1": ("MODIS_Grid_500m_2D", "sur_refl_b02_1"),
    "sur_refl_b03_1": ("MODIS_Grid_500m_2D", "sur_refl_b03_1"),
    "sur_refl_b04_1": ("MODIS_Grid_500m_2D", "sur_refl_b04_1"),
    "sur_refl_b05_1": ("MODIS_Grid_500m_2D", "sur_refl_b05_1"),
    "sur_refl_b06_1": ("MODIS_Grid_500m_2D", "sur_refl_b06_1"),
    "sur_refl_b07_1": ("MODIS_Grid_500m_2D", "sur_refl_b07_1"),
    "state_1km_1": ("MODIS_Grid_1km_2D", "state_1km_1"),
}

# VIIRS HDF5 subdataset paths
_VIIRS_BAND_GROUPS = {
    "I1": "HDFEOS/GRIDS/VNP_Grid_500m_2D/Data Fields/SurfReflect_I1",
    "I2": "HDFEOS/GRIDS/VNP_Grid_500m_2D/Data Fields/SurfReflect_I2",
    "I3": "HDFEOS/GRIDS/VNP_Grid_500m_2D/Data Fields/SurfReflect_I3",
    "M1": "HDFEOS/GRIDS/VNP_Grid_1km_2D/Data Fields/SurfReflect_M1",
    "M2": "HDFEOS/GRIDS/VNP_Grid_1km_2D/Data Fields/SurfReflect_M2",
    "M3": "HDFEOS/GRIDS/VNP_Grid_1km_2D/Data Fields/SurfReflect_M3",
    "M4": "HDFEOS/GRIDS/VNP_Grid_1km_2D/Data Fields/SurfReflect_M4",
    "M5": "HDFEOS/GRIDS/VNP_Grid_1km_2D/Data Fields/SurfReflect_M5",
    "M7": "HDFEOS/GRIDS/VNP_Grid_1km_2D/Data Fields/SurfReflect_M7",
    "M8": "HDFEOS/GRIDS/VNP_Grid_1km_2D/Data Fields/SurfReflect_M8",
    "M10": "HDFEOS/GRIDS/VNP_Grid_1km_2D/Data Fields/SurfReflect_M10",
    "M11": "HDFEOS/GRIDS/VNP_Grid_1km_2D/Data Fields/SurfReflect_M11",
    "QF1": "HDFEOS/GRIDS/VNP_Grid_1km_2D/Data Fields/SurfReflect_QF1",
}


class EarthdataHDFReader:
    """Fetch MODIS/VIIRS daily data from NASA Earthdata (HDF format)."""

    def __init__(self, config: SourceConfig):
        self.config = config

    def fetch_sensor(self, sensor_config: SensorConfig,
                     binding: SensorPlatformBinding,
                     start_date: str, end_date: str, geojson_path: str,
                     data_folder: str = None,
                     max_cloud_cover: int = 80) -> EOResult:
        """Fetch HDF-based sensor data from NASA Earthdata."""
        import requests

        sensor = sensor_config.name
        cfg = self.config

        # Configure GDAL for Earthdata access
        cfg.configure_gdal()

        if data_folder is None:
            data_folder = tempfile.mkdtemp(prefix=f"{sensor}_earthdata_")
            print(f"Cache folder: {data_folder}")
        os.makedirs(data_folder, exist_ok=True)

        # Load geometry
        geometry = load_geojson(geojson_path)
        geojson_dict = json.loads(shapely.to_geojson(geometry))

        # STAC search
        client = pystac_client.Client.open(cfg.stac_url)
        search = client.search(
            collections=[binding.collection],
            intersects=geojson_dict,
            datetime=f"{start_date}/{end_date}",
            max_items=500,
        )
        items = list(search.items())
        items.sort(key=lambda x: x.datetime)

        print(f"Earthdata STAC search ({sensor}): {len(items)} items found")

        if not items:
            raise RuntimeError(
                f"No {sensor} items found on Earthdata for "
                f"{start_date} to {end_date}."
            )

        # Extract DOYs
        doys = np.array([
            item.datetime.timetuple().tm_yday for item in items
        ], dtype=np.int64)

        # Download HDF files and extract bands
        reflectances = []
        geotransform = None
        crs = None
        szas, vzas, raas = [], [], []

        for item in items:
            hdf_path = self._download_hdf(item, data_folder)
            if hdf_path is None:
                continue

            bands, qa, gt, proj, angles = self._extract_bands(
                hdf_path, sensor, binding, geojson_path,
                sensor_config.target_resolution,
            )

            if gt is not None and geotransform is None:
                geotransform = gt
                crs = proj

            # DN to reflectance
            refl = sensor_config.dn_to_reflectance(bands)

            # Cloud masking
            if sensor_config.cloud_mask_fn is not None and qa is not None:
                cloud_mask = sensor_config.cloud_mask_fn(qa)
                refl[:, cloud_mask] = np.nan

            reflectances.append(refl)
            szas.append(angles[0])
            vzas.append(angles[1])
            raas.append(angles[2])

        if not reflectances:
            raise RuntimeError(f"No valid {sensor} data extracted from Earthdata.")

        refs = np.array(reflectances, dtype=np.float32)
        uncs = sensor_config.uncertainty_fn(refs)
        mask = np.all(np.isnan(refs), axis=(0, 1))
        angles = np.array([szas, vzas, raas], dtype=np.float64)

        # Footprint maps
        shape_hw = refs.shape[2:]
        footprints = compute_all_footprints(
            geotransform, shape_hw, sensor_config.resolution_groups,
            sensor_config.target_resolution, mask=mask,
        )

        # Bandpass
        bandpass = load_srf(sensor)

        return EOResult(
            reflectance=refs,
            uncertainty=uncs,
            angles=angles,
            doys=doys[:len(reflectances)],
            mask=mask,
            geotransform=geotransform,
            crs=crs,
            sensor=sensor,
            band_names=sensor_config.band_names,
            native_resolutions=sensor_config.resolution_groups,
            footprints=footprints,
            bandpass=bandpass,
        )

    def _download_hdf(self, item, data_folder):
        """Download the HDF data file for a STAC item."""
        import requests

        # Find the data asset (not browse/thumbnail/metadata)
        data_href = None
        for key, asset in item.assets.items():
            href = asset.href
            # Skip non-data assets
            if key in ("browse", "metadata") or "thumbnail" in key:
                continue
            if key.startswith("s3_"):
                continue
            # Accept HDF files
            if any(href.endswith(ext) for ext in (".hdf", ".h5", ".he5")):
                data_href = href
                break
            # Also accept assets with data type
            media_type = asset.media_type or ""
            if "hdf" in media_type.lower() or "application/x-hdf" in media_type:
                data_href = href
                break

        if data_href is None:
            # Try the first non-metadata asset
            for key, asset in item.assets.items():
                if key not in ("browse", "metadata") and "thumbnail" not in key:
                    if not key.startswith("s3_"):
                        data_href = asset.href
                        break

        if data_href is None:
            print(f"  Warning: no data asset found for {item.id}")
            return None

        # Filename from URL
        filename = data_href.split("/")[-1]
        local_path = os.path.join(data_folder, filename)

        if os.path.exists(local_path):
            return local_path

        print(f"  Downloading {filename}...")
        try:
            # Use GDAL's vsicurl which handles auth via configured headers/cookies
            # Or download via requests with auth
            from eof._credentials import get_earthdata_credentials
            import base64

            creds = get_earthdata_credentials()
            session = requests.Session()

            # Try bearer token first
            token = os.environ.get("EARTHDATA_TOKEN", "")
            if token:
                session.headers["Authorization"] = f"Bearer {token}"
            elif creds["username"] and creds["password"]:
                auth_str = base64.b64encode(
                    f"{creds['username']}:{creds['password']}".encode()
                ).decode()
                # Get token
                try:
                    resp = session.post(
                        "https://urs.earthdata.nasa.gov/api/users/find_or_create_token",
                        headers={"Authorization": f"Basic {auth_str}"},
                        timeout=30,
                    )
                    resp.raise_for_status()
                    token = resp.json().get("access_token", "")
                    session.headers["Authorization"] = f"Bearer {token}"
                except Exception:
                    # Fall back to basic auth with redirects
                    session.auth = (creds["username"], creds["password"])

            resp = session.get(data_href, stream=True, timeout=120,
                               allow_redirects=True)
            resp.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            return local_path

        except Exception as e:
            print(f"  Warning: failed to download {filename}: {e}")
            return None

    def _extract_bands(self, hdf_path, sensor, binding, geojson_path,
                       target_resolution):
        """Extract spectral bands and QA from an HDF file.

        Returns:
            (bands_array, qa_array, geotransform, crs, (sza, vza, raa))
        """
        if sensor == "modis":
            return self._extract_modis_bands(hdf_path, binding, geojson_path,
                                             target_resolution)
        elif sensor == "viirs":
            return self._extract_viirs_bands(hdf_path, binding, geojson_path,
                                             target_resolution)
        else:
            raise ValueError(f"HDF extraction not supported for sensor: {sensor}")

    def _extract_modis_bands(self, hdf_path, binding, geojson_path,
                             target_resolution):
        """Extract bands from MODIS HDF4 file."""
        ds = gdal.Open(hdf_path)
        if ds is None:
            return None, None, None, None, (0.0, 0.0, 0.0)

        subdatasets = dict(ds.GetSubDatasets())
        ds = None

        # Map subdataset names to GDAL paths
        sds_map = {}
        for sds_path, sds_desc in subdatasets.items() if isinstance(subdatasets, dict) else []:
            for key, (grid, layer) in _MODIS_BAND_SUBDATASETS.items():
                if grid in sds_path and layer in sds_path:
                    sds_map[key] = sds_path

        # Re-parse subdatasets properly
        ds = gdal.Open(hdf_path)
        sds_list = ds.GetSubDatasets()
        ds = None
        sds_map = {}
        for sds_path, sds_desc in sds_list:
            for key, (grid, layer) in _MODIS_BAND_SUBDATASETS.items():
                if layer in sds_path:
                    sds_map[key] = sds_path

        bands = []
        geotransform = None
        crs = None

        for asset_key in binding.band_asset_keys:
            sds_path = sds_map.get(asset_key)
            if sds_path is None:
                print(f"  Warning: subdataset {asset_key} not found in {hdf_path}")
                return None, None, None, None, (0.0, 0.0, 0.0)

            warped = gdal.Warp(
                '', sds_path,
                format='MEM',
                cutlineDSName=geojson_path,
                cropToCutline=True,
                dstNodata=0,
                xRes=target_resolution, yRes=target_resolution,
                outputType=gdal.GDT_Int16,
            )
            if warped is None:
                return None, None, None, None, (0.0, 0.0, 0.0)

            data = warped.ReadAsArray()
            if geotransform is None:
                geotransform = warped.GetGeoTransform()
                crs = warped.GetProjection()
            bands.append(data)
            warped = None

        # QA band
        qa_sds = sds_map.get(binding.qa_asset_key)
        qa_data = None
        if qa_sds:
            warped = gdal.Warp(
                '', qa_sds,
                format='MEM',
                cutlineDSName=geojson_path,
                cropToCutline=True,
                dstNodata=0,
                xRes=target_resolution, yRes=target_resolution,
                resampleAlg='nearest',
                outputType=gdal.GDT_UInt16,
            )
            if warped:
                qa_data = warped.ReadAsArray()
                warped = None

        band_array = np.stack(bands, axis=0) if bands else None

        # Try to extract angles from HDF metadata
        sza, vza, raa = 0.0, 0.0, 0.0
        # Angle subdatasets in 1km grid
        for sds_path, sds_desc in sds_list:
            if "SolarZenith" in sds_path:
                try:
                    d = gdal.Open(sds_path)
                    arr = d.ReadAsArray().astype(np.float64) * 0.01
                    sza = np.nanmean(arr)
                    d = None
                except Exception:
                    pass
            elif "SensorZenith" in sds_path:
                try:
                    d = gdal.Open(sds_path)
                    arr = d.ReadAsArray().astype(np.float64) * 0.01
                    vza = np.nanmean(arr)
                    d = None
                except Exception:
                    pass
            elif "SolarAzimuth" in sds_path:
                try:
                    d = gdal.Open(sds_path)
                    saa = np.nanmean(d.ReadAsArray().astype(np.float64) * 0.01)
                    d = None
                except Exception:
                    saa = 0.0
            elif "SensorAzimuth" in sds_path:
                try:
                    d = gdal.Open(sds_path)
                    vaa = np.nanmean(d.ReadAsArray().astype(np.float64) * 0.01)
                    d = None
                    raa = (vaa - saa) % 360
                except Exception:
                    pass

        return band_array, qa_data, geotransform, crs, (sza, vza, raa)

    def _extract_viirs_bands(self, hdf_path, binding, geojson_path,
                             target_resolution):
        """Extract bands from VIIRS HDF5 file."""
        ds = gdal.Open(hdf_path)
        if ds is None:
            return None, None, None, None, (0.0, 0.0, 0.0)

        sds_list = ds.GetSubDatasets()
        ds = None

        # Map band names to subdataset paths
        sds_map = {}
        for sds_path, sds_desc in sds_list:
            for key in list(binding.band_asset_keys) + [binding.qa_asset_key]:
                vpath = _VIIRS_BAND_GROUPS.get(key, key)
                if vpath in sds_path or f"SurfReflect_{key}" in sds_path:
                    sds_map[key] = sds_path

        bands = []
        geotransform = None
        crs = None

        for asset_key in binding.band_asset_keys:
            sds_path = sds_map.get(asset_key)
            if sds_path is None:
                print(f"  Warning: subdataset {asset_key} not found in {hdf_path}")
                return None, None, None, None, (0.0, 0.0, 0.0)

            warped = gdal.Warp(
                '', sds_path,
                format='MEM',
                cutlineDSName=geojson_path,
                cropToCutline=True,
                dstNodata=0,
                xRes=target_resolution, yRes=target_resolution,
                outputType=gdal.GDT_Int16,
            )
            if warped is None:
                return None, None, None, None, (0.0, 0.0, 0.0)

            data = warped.ReadAsArray()
            if geotransform is None:
                geotransform = warped.GetGeoTransform()
                crs = warped.GetProjection()
            bands.append(data)
            warped = None

        # QA band
        qa_sds = sds_map.get(binding.qa_asset_key)
        qa_data = None
        if qa_sds:
            warped = gdal.Warp(
                '', qa_sds,
                format='MEM',
                cutlineDSName=geojson_path,
                cropToCutline=True,
                dstNodata=0,
                xRes=target_resolution, yRes=target_resolution,
                resampleAlg='nearest',
                outputType=gdal.GDT_UInt16,
            )
            if warped:
                qa_data = warped.ReadAsArray()
                warped = None

        band_array = np.stack(bands, axis=0) if bands else None

        # Extract angles
        sza, vza, raa = 0.0, 0.0, 0.0
        saa, vaa = 0.0, 0.0
        for sds_path, sds_desc in sds_list:
            if "SolarZenith" in sds_path and "SolarZenith" in sds_desc:
                try:
                    d = gdal.Open(sds_path)
                    sza = np.nanmean(d.ReadAsArray().astype(np.float64) * 0.01)
                    d = None
                except Exception:
                    pass
            elif "SensorZenith" in sds_path:
                try:
                    d = gdal.Open(sds_path)
                    vza = np.nanmean(d.ReadAsArray().astype(np.float64) * 0.01)
                    d = None
                except Exception:
                    pass
            elif "SolarAzimuth" in sds_path:
                try:
                    d = gdal.Open(sds_path)
                    saa = np.nanmean(d.ReadAsArray().astype(np.float64) * 0.01)
                    d = None
                except Exception:
                    pass
            elif "SensorAzimuth" in sds_path:
                try:
                    d = gdal.Open(sds_path)
                    vaa = np.nanmean(d.ReadAsArray().astype(np.float64) * 0.01)
                    d = None
                except Exception:
                    pass
        raa = (vaa - saa) % 360

        return band_array, qa_data, geotransform, crs, (sza, vza, raa)
