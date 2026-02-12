"""Google Earth Engine EO data reader.

Uses the GEE API (not STAC) so this is a separate implementation
from the unified STAC reader. The ee module is imported lazily.

Supports Sentinel-2 (original), Landsat, MODIS, VIIRS, and Sentinel-3 OLCI.
"""

import os
import tempfile
import datetime
import numpy as np
from osgeo import gdal
import mgrs

from eof._types import S2Result, EOResult
from eof._geojson import load_geojson
from eof._footprints import compute_all_footprints

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

        # Read and process downloaded files — read as int16, convert client-side
        s2_reflectances = []
        geotransform = None
        crs = None
        for filename in filenames:
            g = gdal.Warp(
                '', filename,
                format='MEM',
                cutlineDSName=geojson_path,
                cropToCutline=True,
                dstNodata=0,
                outputType=gdal.GDT_Int16,
            )
            if g is None:
                raise IOError(f"Error reading file: {filename}")
            data = g.ReadAsArray()  # int16: (14, H, W)
            if geotransform is None:
                geotransform = g.GetGeoTransform()
                crs = g.GetProjection()
            g = None

            # Cloud masking on int data
            # bands: B2-B12(10), probability(10), cs(11), cs_cdf(12), B10(13)
            cs_cdf = data[12]   # cloud score CDF (0-100 scaled)
            cloud_mask = (cs_cdf < 70) | (data[0] > 3000) | (data[13] > 100)

            # Convert spectral bands (first 10) to float reflectance
            spectral = data[:10].astype(np.float32) / 10000.0
            spectral[:, cloud_mask] = np.nan
            s2_reflectances.append(spectral)

        s2_refs = np.array(s2_reflectances, dtype=np.float32)
        s2_uncs = np.abs(s2_refs) * 0.1

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

    def fetch_sensor(self, sensor: str, start_date: str, end_date: str,
                     geojson_path: str, data_folder: str = None,
                     max_cloud_cover: int = 80) -> EOResult:
        """
        Fetch multi-sensor EO data via Google Earth Engine.

        Args:
            sensor: One of "sentinel2", "landsat", "modis", "viirs", "s3olci".
            start_date: Start date 'YYYY-MM-DD'.
            end_date: End date 'YYYY-MM-DD'.
            geojson_path: Path to GeoJSON field boundary.
            data_folder: Cache directory. None creates a temp dir.
            max_cloud_cover: Max cloud cover percentage.

        Returns:
            EOResult with reflectance, footprints, etc.
        """
        import ee
        import requests
        import shutil
        from eof._sensor_configs import get_sensor_config
        from eof._platform_bindings import get_binding

        sensor_config = get_sensor_config(sensor)
        binding = get_binding(sensor, "gee")

        try:
            ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com')
        except Exception:
            raise EnvironmentError(
                "GEE not authenticated. Run: earthengine authenticate"
            )

        if data_folder is None:
            data_folder = tempfile.mkdtemp(prefix=f"{sensor}_")
            print(f"Cache folder: {data_folder}")
        os.makedirs(data_folder, exist_ok=True)

        geometry = load_geojson(geojson_path)
        centroid = geometry.centroid.coords[0]
        longitude, latitude = centroid
        coords = list(geometry.exterior.coords)
        ee_geometry = ee.Geometry.Polygon(coords)

        # Build sensor-specific collection
        collection, band_names, qa_band = self._build_collection(
            sensor, binding, ee_geometry, start_date, end_date,
        )
        features = collection.getInfo()['features']

        if not features:
            raise RuntimeError(
                f"No {sensor} items found on GEE for the given "
                f"field and date range ({start_date} to {end_date})."
            )

        print(f"GEE search ({sensor}): {len(features)} images found")

        # Extract angles and DOYs
        szas, vzas, raas, doys = [], [], [], []
        for feat in features:
            props = feat['properties']
            sza, vza, raa = self._extract_sensor_angles(sensor, props)
            szas.append(sza)
            vzas.append(vza)
            raas.append(raa)
            # DOY from system:time_start
            ts = props.get('system:time_start', 0)
            dt = datetime.datetime.utcfromtimestamp(ts / 1000)
            doys.append(dt.timetuple().tm_yday)

        angles = np.array([szas, vzas, raas], dtype=np.float64)
        doys_arr = np.array(doys, dtype=np.int64)

        # Download images
        all_bands = list(band_names) + [qa_band]
        scale = sensor_config.target_resolution

        reflectances = []
        geotransform = None
        crs = None

        for feat in features:
            image_id = feat['id']
            safe_id = image_id.replace('/', '_')
            filename = os.path.join(data_folder, f"{sensor}_{safe_id}.tif")

            if not os.path.exists(filename):
                image = ee.Image(image_id).select(all_bands)
                download_option = {
                    'name': safe_id,
                    'scale': scale,
                    'bands': all_bands,
                    'region': ee_geometry,
                    'format': 'GEO_TIFF',
                    'maxPixels': 1e16,
                }
                url = image.getDownloadURL(download_option)
                r = requests.get(url, stream=True)
                r.raise_for_status()
                with open(filename, 'wb') as out_file:
                    shutil.copyfileobj(r.raw, out_file)

            # Read and crop
            g = gdal.Warp(
                '', filename,
                format='MEM',
                cutlineDSName=geojson_path,
                cropToCutline=True,
                dstNodata=0,
                outputType=gdal.GDT_Int16,
            )
            if g is None:
                raise IOError(f"Error reading file: {filename}")
            data = g.ReadAsArray()  # (n_bands+1, H, W)
            if geotransform is None:
                geotransform = g.GetGeoTransform()
                crs = g.GetProjection()
            g = None

            # Separate spectral and QA
            n_spectral = len(band_names)
            spectral_dn = data[:n_spectral]
            qa_data = data[n_spectral]

            # DN to reflectance
            refl = sensor_config.dn_to_reflectance(spectral_dn)

            # Cloud masking
            if sensor_config.cloud_mask_fn is not None:
                cloud_mask = sensor_config.cloud_mask_fn(qa_data)
                refl[:, cloud_mask] = np.nan

            reflectances.append(refl)

        refs = np.array(reflectances, dtype=np.float32)
        uncs = sensor_config.uncertainty_fn(refs)
        mask = np.all(np.isnan(refs), axis=(0, 1))

        # Footprint maps
        shape_hw = refs.shape[2:]
        footprints = compute_all_footprints(
            geotransform, shape_hw, sensor_config.resolution_groups,
            sensor_config.target_resolution,
        )

        return EOResult(
            reflectance=refs,
            uncertainty=uncs,
            angles=angles,
            doys=doys_arr,
            mask=mask,
            geotransform=geotransform,
            crs=crs,
            sensor=sensor,
            band_names=sensor_config.band_names,
            native_resolutions=sensor_config.resolution_groups,
            footprints=footprints,
        )

    def _build_collection(self, sensor, binding, ee_geometry,
                          start_date, end_date):
        """Build a filtered GEE ImageCollection for the given sensor.

        Returns:
            (ee.ImageCollection, band_names_list, qa_band_name)
        """
        import ee

        collection = (ee.ImageCollection(binding.collection)
                      .filterBounds(ee_geometry)
                      .filterDate(start_date, end_date)
                      .sort("system:time_start"))

        band_names = list(binding.band_asset_keys)
        qa_band = binding.qa_asset_key

        if sensor == "sentinel2":
            # Special handling: use MGRS tile filter
            m = mgrs.MGRS()
            centroid = ee_geometry.centroid().coordinates().getInfo()
            mgrs_tile = m.toMGRS(centroid[1], centroid[0])[:5]
            collection = collection.filterMetadata(
                'MGRS_TILE', 'equals', mgrs_tile
            )

        if sensor == "landsat":
            # Include both Landsat 8 and 9
            l8 = (ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
                  .filterBounds(ee_geometry)
                  .filterDate(start_date, end_date)
                  .sort("system:time_start"))
            l9 = (ee.ImageCollection("LANDSAT/LC09/C02/T1_L2")
                  .filterBounds(ee_geometry)
                  .filterDate(start_date, end_date)
                  .sort("system:time_start"))
            collection = l8.merge(l9).sort("system:time_start")

        return collection, band_names, qa_band

    def _extract_sensor_angles(self, sensor, properties):
        """Extract (SZA, VZA, RAA) for any sensor from GEE properties."""
        if sensor == "sentinel2":
            return self._s2_angles_from_props(properties)
        elif sensor == "landsat":
            sza = properties.get('SUN_ELEVATION', 45.0)
            sza = 90.0 - sza if sza < 90 else sza
            saa = properties.get('SUN_AZIMUTH', 0.0)
            return sza, 0.0, saa
        elif sensor == "modis":
            sza = properties.get('SolarZenith', 0.0) * 0.01  # scale factor
            vza = properties.get('SensorZenith', 0.0) * 0.01
            saa = properties.get('SolarAzimuth', 0.0) * 0.01
            vaa = properties.get('SensorAzimuth', 0.0) * 0.01
            raa = (vaa - saa) % 360
            return sza, vza, raa
        elif sensor == "viirs":
            sza = properties.get('SolarZenith', 0.0) * 0.01
            vza = properties.get('SensorZenith', 0.0) * 0.01
            saa = properties.get('SolarAzimuth', 0.0) * 0.01
            vaa = properties.get('SensorAzimuth', 0.0) * 0.01
            raa = (vaa - saa) % 360
            return sza, vza, raa
        elif sensor == "s3olci":
            sza = properties.get('SZA', 0.0)
            vza = properties.get('OZA', 0.0)
            saa = properties.get('SAA', 0.0)
            vaa = properties.get('OAA', 0.0)
            raa = (vaa - saa) % 360
            return sza, vza, raa
        return 0.0, 0.0, 0.0

    def _s2_angles_from_props(self, props):
        """Extract S2 angles from GEE properties."""
        s2_bands = ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B8A', 'B11', 'B12']
        mean_vaa_keys = [f'MEAN_INCIDENCE_AZIMUTH_ANGLE_{b}' for b in s2_bands]
        mean_vza_keys = [f'MEAN_INCIDENCE_ZENITH_ANGLE_{b}' for b in s2_bands]
        vaa = np.nanmean([props.get(key, np.nan) for key in mean_vaa_keys])
        vza = np.nanmean([props.get(key, np.nan) for key in mean_vza_keys])
        saa = props.get('MEAN_SOLAR_AZIMUTH_ANGLE', 0.0)
        sza = props.get('MEAN_SOLAR_ZENITH_ANGLE', 0.0)
        raa = (vaa - saa) % 360
        return sza, vza, raa

    def _calculate_angles(self, features):
        """Calculate SZA, VZA, RAA from GEE feature properties (S2 only)."""
        szas, vzas, raas = [], [], []
        for feat in features:
            sza, vza, raa = self._s2_angles_from_props(feat['properties'])
            szas.append(sza)
            vzas.append(vza)
            raas.append(raa)

        return np.array(szas), np.array(vzas), np.array(raas)
