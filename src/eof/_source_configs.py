"""Source-specific configuration for STAC-based EO data sources."""

from dataclasses import dataclass, field
from typing import Optional, Callable, List
from osgeo import gdal


@dataclass
class SourceConfig:
    """Configuration for a STAC-based Sentinel-2 data source."""
    name: str
    stac_url: str
    collection: str
    band_assets: List[str]
    scl_asset_key: str
    max_concurrent_reads: int
    processing_baseline_property: str
    mgrs_property_keys: List[str]
    configure_gdal: Callable
    transform_href: Optional[Callable] = None
    vsi_prefix: str = "/vsicurl"
    s3_endpoint: Optional[str] = None


# ---------------------------------------------------------------------------
# GDAL configuration functions
# ---------------------------------------------------------------------------

def _configure_gdal_for_cdse():
    """Configure GDAL for CDSE S3 or token access. Returns runtime overrides."""
    from eof._credentials import get_cdse_credentials

    creds = get_cdse_credentials()
    s3_key = creds["s3_access_key"]
    s3_secret = creds["s3_secret_key"]

    overrides = {}

    if s3_key and s3_secret:
        gdal.SetConfigOption("AWS_S3_ENDPOINT", "eodata.dataspace.copernicus.eu")
        gdal.SetConfigOption("AWS_ACCESS_KEY_ID", s3_key)
        gdal.SetConfigOption("AWS_SECRET_ACCESS_KEY", s3_secret)
        gdal.SetConfigOption("AWS_VIRTUAL_HOSTING", "FALSE")
        gdal.SetConfigOption("AWS_HTTPS", "YES")
        overrides["vsi_prefix"] = "/vsis3"
    else:
        username = creds["username"]
        password = creds["password"]
        if not (username and password):
            raise EnvironmentError(
                "CDSE credentials not found. Set either:\n"
                "  CDSE_S3_ACCESS_KEY + CDSE_S3_SECRET_KEY  (preferred), or\n"
                "  CDSE_USERNAME + CDSE_PASSWORD\n"
                "Generate S3 keys at https://eodata.dataspace.copernicus.eu"
            )
        import requests
        resp = requests.post(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/"
            "protocol/openid-connect/token",
            data={
                "client_id": "cdse-public",
                "grant_type": "password",
                "username": username,
                "password": password,
            },
            timeout=30,
        )
        resp.raise_for_status()
        token = resp.json()["access_token"]
        gdal.SetConfigOption("GDAL_HTTP_HEADERS", f"Authorization: Bearer {token}")
        overrides["vsi_prefix"] = "/vsicurl"

    # Common optimizations
    gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
    gdal.SetConfigOption("CPL_VSIL_CURL_ALLOWED_EXTENSIONS", ".tif,.jp2,.xml")
    gdal.SetConfigOption("GDAL_HTTP_MAX_RETRY", "5")
    gdal.SetConfigOption("GDAL_HTTP_RETRY_DELAY", "5")
    gdal.SetConfigOption("GDAL_CACHEMAX", "256")
    gdal.SetConfigOption("CPL_VSIL_CURL_CACHE_SIZE", "134217728")
    gdal.SetConfigOption("GDAL_NUM_THREADS", "ALL_CPUS")
    gdal.SetConfigOption("VSI_CACHE", "TRUE")
    gdal.SetConfigOption("VSI_CACHE_SIZE", "134217728")

    return overrides


def _configure_gdal_for_aws():
    """Configure GDAL for AWS public S3 access."""
    gdal.SetConfigOption("AWS_NO_SIGN_REQUEST", "YES")
    gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
    gdal.SetConfigOption("CPL_VSIL_CURL_ALLOWED_EXTENSIONS", ".tif,.xml")
    gdal.SetConfigOption("GDAL_HTTP_MAX_RETRY", "5")
    gdal.SetConfigOption("GDAL_HTTP_RETRY_DELAY", "2")
    gdal.SetConfigOption("GDAL_CACHEMAX", "256")
    gdal.SetConfigOption("CPL_VSIL_CURL_CACHE_SIZE", "134217728")
    gdal.SetConfigOption("GDAL_NUM_THREADS", "ALL_CPUS")
    gdal.SetConfigOption("VSI_CACHE", "TRUE")
    gdal.SetConfigOption("VSI_CACHE_SIZE", "134217728")
    return {}


def _configure_gdal_for_planetary():
    """Configure GDAL for Planetary Computer signed URL access."""
    gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
    gdal.SetConfigOption("CPL_VSIL_CURL_ALLOWED_EXTENSIONS", ".tif,.xml")
    gdal.SetConfigOption("GDAL_HTTP_MAX_RETRY", "5")
    gdal.SetConfigOption("GDAL_HTTP_RETRY_DELAY", "2")
    gdal.SetConfigOption("GDAL_CACHEMAX", "256")
    gdal.SetConfigOption("CPL_VSIL_CURL_CACHE_SIZE", "134217728")
    gdal.SetConfigOption("GDAL_NUM_THREADS", "ALL_CPUS")
    gdal.SetConfigOption("VSI_CACHE", "TRUE")
    gdal.SetConfigOption("VSI_CACHE_SIZE", "134217728")
    return {}


def _planetary_sign_href(href):
    """Sign an Azure Blob Storage URL using planetary_computer."""
    try:
        import planetary_computer
        return planetary_computer.sign_url(href)
    except ImportError:
        return href


# ---------------------------------------------------------------------------
# Pre-built source configurations
# ---------------------------------------------------------------------------

CDSE_CONFIG = SourceConfig(
    name="cdse",
    stac_url="https://stac.dataspace.copernicus.eu/v1",
    collection="sentinel-2-l2a",
    band_assets=['B02_10m', 'B03_10m', 'B04_10m', 'B05_20m', 'B06_20m',
                 'B07_20m', 'B08_10m', 'B8A_20m', 'B11_20m', 'B12_20m'],
    scl_asset_key='SCL_20m',
    max_concurrent_reads=4,
    processing_baseline_property='processing:version',
    mgrs_property_keys=['grid:code'],
    configure_gdal=_configure_gdal_for_cdse,
    vsi_prefix="/vsis3",
    s3_endpoint="eodata.dataspace.copernicus.eu",
)

AWS_CONFIG = SourceConfig(
    name="aws",
    stac_url="https://earth-search.aws.element84.com/v1",
    collection="sentinel-2-l2a",
    band_assets=['blue', 'green', 'red', 'rededge1', 'rededge2', 'rededge3',
                 'nir', 'nir08', 'swir16', 'swir22'],
    scl_asset_key='scl',
    max_concurrent_reads=8,
    processing_baseline_property='s2:processing_baseline',
    mgrs_property_keys=['grid:code', 'mgrs:grid_square', 's2:mgrs_tile'],
    configure_gdal=_configure_gdal_for_aws,
)

PLANETARY_CONFIG = SourceConfig(
    name="planetary",
    stac_url="https://planetarycomputer.microsoft.com/api/stac/v1",
    collection="sentinel-2-l2a",
    band_assets=['B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B11', 'B12'],
    scl_asset_key='SCL',
    max_concurrent_reads=8,
    processing_baseline_property='s2:processing_baseline',
    mgrs_property_keys=['grid:code', 's2:mgrs_tile'],
    configure_gdal=_configure_gdal_for_planetary,
    transform_href=_planetary_sign_href,
)

def _configure_gdal_for_earthdata():
    """Configure GDAL for NASA Earthdata access via bearer token or .netrc.

    Supports:
    1. Bearer token (from env var EARTHDATA_TOKEN or obtained via EDL API)
    2. .netrc file (machine urs.earthdata.nasa.gov)
    3. Credentials from env vars EARTHDATA_USERNAME + EARTHDATA_PASSWORD
    4. Credentials from ~/.eof/config.json

    Returns runtime overrides dict.
    """
    import os

    overrides = {}

    # Try bearer token from env first
    token = os.environ.get("EARTHDATA_TOKEN", "")

    if not token:
        # Try to get credentials and fetch a token
        from eof._credentials import get_earthdata_credentials
        creds = get_earthdata_credentials()
        username = creds.get("username", "")
        password = creds.get("password", "")

        if username and password:
            import base64
            import requests
            auth_str = base64.b64encode(
                f"{username}:{password}".encode()
            ).decode()
            try:
                resp = requests.post(
                    "https://urs.earthdata.nasa.gov/api/users/find_or_create_token",
                    headers={"Authorization": f"Basic {auth_str}"},
                    timeout=30,
                )
                resp.raise_for_status()
                token = resp.json().get("access_token", "")
            except Exception:
                # Fall back to cookie-based auth via .netrc
                pass

    if token:
        gdal.SetConfigOption("GDAL_HTTP_HEADERS",
                             f"Authorization: Bearer {token}")
    else:
        # Rely on .netrc for cookie-based auth
        cookie_file = os.path.expanduser("~/.eof/earthdata_cookies.txt")
        gdal.SetConfigOption("GDAL_HTTP_COOKIEFILE", cookie_file)
        gdal.SetConfigOption("GDAL_HTTP_COOKIEJAR", cookie_file)

    # Common optimizations
    gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
    gdal.SetConfigOption("CPL_VSIL_CURL_ALLOWED_EXTENSIONS", ".tif,.TIF,.hdf,.h5")
    gdal.SetConfigOption("GDAL_HTTP_MAX_RETRY", "5")
    gdal.SetConfigOption("GDAL_HTTP_RETRY_DELAY", "5")
    gdal.SetConfigOption("GDAL_CACHEMAX", "256")
    gdal.SetConfigOption("CPL_VSIL_CURL_CACHE_SIZE", "134217728")
    gdal.SetConfigOption("GDAL_NUM_THREADS", "ALL_CPUS")
    gdal.SetConfigOption("VSI_CACHE", "TRUE")
    gdal.SetConfigOption("VSI_CACHE_SIZE", "134217728")

    return overrides


EARTHDATA_CONFIG = SourceConfig(
    name="earthdata",
    stac_url="https://cmr.earthdata.nasa.gov/stac/LPCLOUD",
    collection="HLSL30_2.0",  # default; overridden per sensor by bindings
    band_assets=[],  # overridden per sensor
    scl_asset_key="Fmask",
    max_concurrent_reads=4,
    processing_baseline_property="",
    mgrs_property_keys=[],
    configure_gdal=_configure_gdal_for_earthdata,
)


SOURCE_CONFIGS = {
    "cdse": CDSE_CONFIG,
    "aws": AWS_CONFIG,
    "planetary": PLANETARY_CONFIG,
    "earthdata": EARTHDATA_CONFIG,
}


def get_config(source: str) -> SourceConfig:
    """Get the SourceConfig for a named data source."""
    if source not in SOURCE_CONFIGS:
        raise ValueError(
            f"Unknown source '{source}'. Available: {list(SOURCE_CONFIGS.keys())}"
        )
    return SOURCE_CONFIGS[source]
