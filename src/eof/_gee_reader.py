"""Google Earth Engine Sentinel-2 reader.

Uses the GEE API (not STAC) so this is a separate implementation
from the unified STAC reader. The ee module is imported lazily.
"""

import os
import json
import tempfile
import datetime
import numpy as np
from osgeo import gdal
from functools import partial
from shapely.geometry import shape
import shapely
import mgrs
from concurrent.futures import ThreadPoolExecutor

from eof._types import S2Result
from eof._geojson import load_geojson

gdal.PushErrorHandler('CPLQuietErrorHandler')


class GEEReader:
    """Fetch Sentinel-2 L2A data via Google Earth Engine."""

    def fetch(self, start_date: str, end_date: str, geojson_path: str,
              data_folder: str = None, max_cloud_cover: int = 80) -> S2Result:
        """
        Fetch Sentinel-2 L2A data for a field boundary via GEE.

        Args:
            start_date: Start date 'YYYY-MM-DD'.
            end_date: End date 'YYYY-MM-DD'.
            geojson_path: Path to GeoJSON field boundary.
            data_folder: Cache directory. None creates a temp dir.
            max_cloud_cover: Max cloud cover percentage (not used by GEE filter).

        Returns:
            S2Result with reflectance, uncertainty, angles, doys, mask, geotransform, crs.
        """
        import ee
        import requests
        import shutil

        try:
            ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com')
        except Exception:
            raise EnvironmentError(
                "GEE not authenticated. Run: earthengine authenticate"
            )

        # Set up data folder
        if data_folder is None:
            data_folder = tempfile.mkdtemp(prefix="s2_")
            print(f"Cache folder: {data_folder}")
        os.makedirs(data_folder, exist_ok=True)

        # Load geometry
        geometry = load_geojson(geojson_path)
        centroid = geometry.centroid.coords[0]
        longitude, latitude = centroid

        # Convert to EE geometry
        coords = list(geometry.exterior.coords)
        ee_geometry = ee.Geometry.Polygon(coords)

        # Get MGRS tile
        m = mgrs.MGRS()
        mgrs_tile = m.toMGRS(latitude, longitude)[:5]

        # Filter S2 collection
        s2 = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        s2 = (s2.filterBounds(ee_geometry)
                .filterDate(start_date, end_date)
                .filterMetadata('MGRS_TILE', 'equals', mgrs_tile)
                .sort("system:time_start"))
        features = s2.getInfo()['features']

        if not features:
            raise RuntimeError(
                f"No Sentinel-2 L2A items found on GEE for the given "
                f"field and date range ({start_date} to {end_date})."
            )

        print(f"GEE search: {len(features)} images found on tile {mgrs_tile}")

        # Calculate angles
        szas, vzas, raas = self._calculate_angles(features)
        s2_angles = np.array([szas, vzas, raas])

        # Get DOYs
        doys = np.array([
            datetime.datetime.strptime(
                feat['properties']['PRODUCT_ID'].split('_')[2][:8], '%Y%m%d'
            ).timetuple().tm_yday
            for feat in features
        ])

        # Download images
        bands = ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B8A',
                 'B11', 'B12', 'probability', 'cs', 'cs_cdf', 'B10']

        filenames = []
        for feat in features:
            image_id = feat['id']
            product_id = feat['properties']['PRODUCT_ID']
            filename = os.path.join(data_folder, product_id + '.tif')
            filenames.append(filename)

            if not os.path.exists(filename):
                image = ee.Image(image_id)
                cloud = ee.Image(
                    'COPERNICUS/S2_CLOUD_PROBABILITY/%s' % feat['properties']['system:index']
                )
                image = image.addBands(cloud)
                cloud_score = ee.Image(
                    'GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED/%s' % feat['properties']['system:index']
                )
                image = image.addBands(cloud_score.multiply(100).int16())
                b10 = ee.Image(
                    'COPERNICUS/S2_HARMONIZED/%s' % feat['properties']['system:index']
                ).select('B10')
                image = image.addBands(b10)

                download_option = {
                    'name': product_id,
                    'scale': 10,
                    'bands': bands,
                    'region': ee_geometry,
                    'format': 'GEO_TIFF',
                    'maxPixels': 1e16,
                }
                url = image.getDownloadURL(download_option)
                r = requests.get(url, stream=True)
                r.raise_for_status()
                with open(filename, 'wb') as out_file:
                    shutil.copyfileobj(r.raw, out_file)

        # Read and process downloaded files
        geojson_str = shapely.to_geojson(geometry)
        s2_reflectances = []
        for filename in filenames:
            g = gdal.Warp(
                '', filename,
                format='MEM',
                cutlineDSName=geojson_str,
                cropToCutline=True,
                dstNodata=np.nan,
                outputType=gdal.GDT_Float32,
            )
            if g is None:
                raise IOError(f"Error reading file: {filename}")
            data = g.ReadAsArray()
            cloud = data[-2]
            cloud_mask = (cloud < 70) | (data[0] > 3000) | (data[-1] > 100)
            data = np.where(cloud_mask, np.nan, data[:-4] / 10000.0)
            s2_reflectances.append(data)

        s2_refs = np.array(s2_reflectances, dtype=np.float32)
        s2_uncs = np.abs(s2_refs) * 0.1

        # Get geotransform and CRS from first file
        g = gdal.Open(filenames[0])
        geotransform = g.GetGeoTransform()
        crs = g.GetProjection()
        g = None

        mask = np.all(np.isnan(s2_refs), axis=(0, 1))

        return S2Result(
            reflectance=s2_refs,
            uncertainty=s2_uncs,
            angles=s2_angles,
            doys=doys,
            mask=mask,
            geotransform=geotransform,
            crs=crs,
        )

    def _calculate_angles(self, features):
        """Calculate SZA, VZA, RAA from GEE feature properties."""
        s2_bands = ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B8A', 'B11', 'B12']
        mean_vaa_keys = [f'MEAN_INCIDENCE_AZIMUTH_ANGLE_{band}' for band in s2_bands]
        mean_vza_keys = [f'MEAN_INCIDENCE_ZENITH_ANGLE_{band}' for band in s2_bands]

        szas, vzas, raas = [], [], []
        for feat in features:
            props = feat['properties']
            vaa = np.nanmean([props.get(key, np.nan) for key in mean_vaa_keys])
            vza = np.nanmean([props.get(key, np.nan) for key in mean_vza_keys])
            saa = props['MEAN_SOLAR_AZIMUTH_ANGLE']
            sza = props['MEAN_SOLAR_ZENITH_ANGLE']
            raa = (vaa - saa) % 360
            szas.append(sza)
            vzas.append(vza)
            raas.append(raa)

        return np.array(szas), np.array(vzas), np.array(raas)
