"""
Credential management for EO Fetch data sources.

Credentials are stored in ~/.eof/config.json with restricted file permissions.
Environment variables take priority over the config file.

Supported data sources and their credentials:
    cdse:       CDSE_S3_ACCESS_KEY, CDSE_S3_SECRET_KEY (or CDSE_USERNAME, CDSE_PASSWORD)
    gee:        Managed by earthengine-api (ee.Authenticate)
    aws:        No credentials needed
    planetary:  No credentials needed (handled by planetary-computer package)
"""

import os
import json
import stat
import time
from pathlib import Path

CONFIG_DIR = Path.home() / ".eof"
CONFIG_FILE = CONFIG_DIR / "config.json"

# Legacy ARC config for migration
_OLD_ARC_CONFIG = Path.home() / ".arc" / "config.json"

ALL_SOURCES = ["aws", "cdse", "planetary", "gee", "earthdata"]

DEFAULT_CONFIG = {
    "data_source_preference": ["aws", "cdse", "planetary", "earthdata", "gee"],
    "cdse": {
        "s3_access_key": "",
        "s3_secret_key": "",
        "username": "",
        "password": "",
    },
    "gee": {
        "note": "GEE authentication is managed by 'earthengine authenticate'. No keys needed here."
    },
    "aws": {
        "note": "AWS Earth Search requires no authentication."
    },
    "planetary": {
        "note": "Planetary Computer auth is handled by the planetary-computer package."
    },
    "earthdata": {
        "username": "",
        "password": "",
        "note": "NASA Earthdata Login. Register at https://urs.earthdata.nasa.gov/. "
                "Or set EARTHDATA_USERNAME + EARTHDATA_PASSWORD env vars. "
                "Or use ~/.netrc with machine urs.earthdata.nasa.gov.",
    },
}


def _ensure_config_dir():
    """Create config directory with restricted permissions."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    os.chmod(CONFIG_DIR, stat.S_IRWXU)


def save_config(config: dict):
    """Save credentials to config file with restricted permissions."""
    _ensure_config_dir()
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)
    os.chmod(CONFIG_FILE, stat.S_IRUSR | stat.S_IWUSR)


def load_config() -> dict:
    """Load credentials from config file, or migrate from ARC, or return defaults."""
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            return json.load(f)
    # Migrate from legacy ~/.arc/config.json if it exists
    if _OLD_ARC_CONFIG.exists():
        with open(_OLD_ARC_CONFIG) as f:
            old_config = json.load(f)
        save_config(old_config)
        print(f"Migrated credentials from {_OLD_ARC_CONFIG} -> {CONFIG_FILE}")
        return old_config
    return DEFAULT_CONFIG.copy()


def create_default_config():
    """Create a default config file if one doesn't exist."""
    if not CONFIG_FILE.exists():
        save_config(DEFAULT_CONFIG)
        print(f"Created default config at {CONFIG_FILE}")
    else:
        print(f"Config already exists at {CONFIG_FILE}")


def get_cdse_credentials() -> dict:
    """
    Get CDSE credentials. Environment variables take priority over config file.

    Returns:
        dict with keys: 's3_access_key', 's3_secret_key', 'username', 'password'
    """
    config = load_config()
    cdse = config.get("cdse", {})

    return {
        "s3_access_key": os.environ.get("CDSE_S3_ACCESS_KEY", "") or cdse.get("s3_access_key", ""),
        "s3_secret_key": os.environ.get("CDSE_S3_SECRET_KEY", "") or cdse.get("s3_secret_key", ""),
        "username": os.environ.get("CDSE_USERNAME", "") or cdse.get("username", ""),
        "password": os.environ.get("CDSE_PASSWORD", "") or cdse.get("password", ""),
    }


def get_earthdata_credentials() -> dict:
    """
    Get NASA Earthdata Login credentials.

    Checks (in order):
    1. Environment variables: EARTHDATA_USERNAME, EARTHDATA_PASSWORD
    2. Config file (~/.eof/config.json)
    3. ~/.netrc file (machine urs.earthdata.nasa.gov)

    Returns:
        dict with keys: 'username', 'password'
    """
    # Env vars
    username = os.environ.get("EARTHDATA_USERNAME", "")
    password = os.environ.get("EARTHDATA_PASSWORD", "")
    if username and password:
        return {"username": username, "password": password}

    # Config file
    config = load_config()
    ed = config.get("earthdata", {})
    username = ed.get("username", "")
    password = ed.get("password", "")
    if username and password:
        return {"username": username, "password": password}

    # .netrc
    try:
        import netrc
        nrc = netrc.netrc()
        auth = nrc.authenticators("urs.earthdata.nasa.gov")
        if auth:
            return {"username": auth[0], "password": auth[2]}
    except Exception:
        pass

    return {"username": "", "password": ""}


def get_data_source_preference() -> list:
    """Return the ordered list of preferred data sources from config."""
    config = load_config()
    return config.get("data_source_preference", DEFAULT_CONFIG["data_source_preference"])


def get_available_sources() -> list:
    """
    Return list of data sources that have working credentials/dependencies.

    Checks each source without importing heavy modules where possible.
    """
    available = []

    # AWS: always available (no auth)
    available.append("aws")

    # Planetary Computer: available if package installed
    try:
        import planetary_computer  # noqa: F401
        available.append("planetary")
    except ImportError:
        pass

    # CDSE: available if credentials configured
    creds = get_cdse_credentials()
    if (creds["s3_access_key"] and creds["s3_secret_key"]) or \
       (creds["username"] and creds["password"]):
        available.append("cdse")

    # Earthdata: available if credentials configured (env, config, or .netrc)
    ed_creds = get_earthdata_credentials()
    if ed_creds["username"] and ed_creds["password"]:
        available.append("earthdata")
    elif os.environ.get("EARTHDATA_TOKEN"):
        available.append("earthdata")

    # GEE: available if ee.Initialize() works
    try:
        import ee
        ee.Initialize()
        available.append("gee")
    except Exception:
        pass

    return available


def probe_download_speed(geojson_path: str, sources: list = None,
                         timeout: float = 30.0) -> dict:
    """
    Probe download speed of each data source by fetching a single band.

    Args:
        geojson_path: Path to GeoJSON field boundary.
        sources: List of sources to probe (default: all available).
        timeout: Max seconds per probe before giving up.

    Returns:
        dict: {source_name: seconds} for successful probes.
    """
    if sources is None:
        sources = get_available_sources()

    with open(geojson_path) as f:
        features = json.load(f)["features"]
    from shapely.geometry import shape
    import shapely as _shapely
    geom = shape(features[0]["geometry"])
    geojson_dict = json.loads(_shapely.to_geojson(geom))

    results = {}

    print("Probing data source speeds...")
    print(f"{'Source':<20} {'Time (s)':<12} {'Status'}")
    print("-" * 50)

    for source in sources:
        try:
            elapsed = _probe_single_source(source, geojson_dict, geojson_path, timeout)
            results[source] = elapsed
            print(f"{source:<20} {elapsed:<12.1f} OK")
        except Exception as e:
            print(f"{source:<20} {'--':<12} FAILED ({e})")

    if results:
        fastest = min(results, key=results.get)
        print(f"\nFastest: {fastest} ({results[fastest]:.1f}s)")

    return results


def _probe_single_source(source: str, geojson_dict: dict,
                         geojson_path: str, timeout: float) -> float:
    """Probe a single source by reading one band. Returns elapsed seconds."""
    from osgeo import gdal
    import pystac_client

    gdal.PushErrorHandler('CPLQuietErrorHandler')

    if source == "aws":
        gdal.SetConfigOption("AWS_NO_SIGN_REQUEST", "YES")
        gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")

        client = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
        search = client.search(
            collections=["sentinel-2-l2a"],
            intersects=geojson_dict,
            max_items=1,
        )
        items = list(search.items())
        if not items:
            raise RuntimeError("No items found")

        href = items[0].assets["blue"].href
        vsi_path = f"/vsicurl/{href}"

    elif source == "cdse":
        from eof._source_configs import _configure_gdal_for_cdse
        overrides = _configure_gdal_for_cdse()
        vsi_prefix = overrides.get("vsi_prefix", "/vsicurl")

        client = pystac_client.Client.open("https://stac.dataspace.copernicus.eu/v1")
        search = client.search(
            collections=["sentinel-2-l2a"],
            intersects=geojson_dict,
            max_items=1,
        )
        items = list(search.items())
        if not items:
            raise RuntimeError("No items found")

        href = items[0].assets["B02_10m"].href
        if vsi_prefix == "/vsis3":
            if href.startswith("s3://"):
                vsi_path = f"/vsis3/{href[5:]}"
            else:
                vsi_path = f"/vsis3{href}"
        else:
            if href.startswith("s3://"):
                path = href[5:]
                vsi_path = f"/vsicurl/https://eodata.dataspace.copernicus.eu/{path}"
            else:
                vsi_path = f"/vsicurl/{href}"

    elif source == "planetary":
        import planetary_computer

        gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
        client = pystac_client.Client.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1"
        )
        search = client.search(
            collections=["sentinel-2-l2a"],
            intersects=geojson_dict,
            max_items=1,
        )
        items = list(search.items())
        if not items:
            raise RuntimeError("No items found")

        href = items[0].assets["B02"].href
        signed_href = planetary_computer.sign_url(href)
        vsi_path = f"/vsicurl/{signed_href}"

    elif source == "gee":
        import ee
        t0 = time.time()
        ee.Initialize()
        img = ee.Image("COPERNICUS/S2_SR_HARMONIZED").first()
        img.getInfo()
        return time.time() - t0

    else:
        raise ValueError(f"Unknown source: {source}")

    # Time the GDAL warp
    t0 = time.time()
    ds = gdal.Warp(
        '', vsi_path,
        format='MEM',
        cutlineDSName=geojson_path,
        cropToCutline=True,
        xRes=10, yRes=10,
        outputType=gdal.GDT_Int16,
    )
    if ds is None:
        raise IOError(f"GDAL failed to read from {source}")
    _ = ds.ReadAsArray()
    ds = None
    return time.time() - t0


def select_data_source(geojson_path: str = None, probe: bool = False) -> str:
    """
    Select the best data source.

    If probe=True and geojson_path is provided, probes download speed and
    picks the fastest. Otherwise, returns the first available source from
    the user's preference list.

    Returns:
        str: Data source name ('aws', 'cdse', 'planetary', or 'gee').
    """
    available = get_available_sources()
    if not available:
        raise RuntimeError(
            "No data sources available. Configure credentials with eof."
        )

    if probe and geojson_path:
        speeds = probe_download_speed(geojson_path, sources=available)
        if speeds:
            return min(speeds, key=speeds.get)

    preference = get_data_source_preference()
    for source in preference:
        if source in available:
            return source

    return available[0]


def benchmark_sources(geojson_path: str = None, sensors: list = None,
                      timeout: float = 30.0) -> dict:
    """
    Time a single-band download from every available sensor x platform combination.

    Args:
        geojson_path: Field boundary GeoJSON. Defaults to eof.TEST_GEOJSON.
        sensors: Sensors to test. None = all available.
        timeout: Max seconds per probe.

    Returns:
        dict: {(sensor, platform): seconds} for successful probes.
        Also prints a formatted table of results.
    """
    from eof._platform_bindings import BINDINGS, get_dataset_info

    if geojson_path is None:
        from pathlib import Path
        geojson_path = str(Path(__file__).parent / "test_data" / "SF_field.geojson")

    available = get_available_sources()

    # Build list of (sensor, platform) combos to test
    combos = []
    for (sensor, platform) in BINDINGS:
        if sensors and sensor not in sensors:
            continue
        if platform not in available:
            continue
        if platform == "gee":
            continue  # GEE timing is server-dominated, not meaningful
        info = get_dataset_info(sensor, platform)
        fmt = info.get("format", "cog")
        if fmt in ("hdf4", "hdf5"):
            continue  # Skip HDF downloads for benchmarking
        combos.append((sensor, platform))

    results = {}

    print(f"\nBenchmarking {len(combos)} sensor x platform combinations...")
    print(f"{'Sensor':<12} {'Platform':<12} {'Time (s)':<10} {'Status'}")
    print("-" * 50)

    for sensor, platform in combos:
        try:
            elapsed = _probe_sensor_source(sensor, platform, geojson_path, timeout)
            results[(sensor, platform)] = elapsed
            print(f"{sensor:<12} {platform:<12} {elapsed:<10.1f} OK")
        except Exception as e:
            print(f"{sensor:<12} {platform:<12} {'--':<10} FAILED ({e})")

    if results:
        fastest = min(results, key=results.get)
        print(f"\nFastest: {fastest[0]}/{fastest[1]} ({results[fastest]:.1f}s)")

    return results


def _probe_sensor_source(sensor: str, platform: str,
                         geojson_path: str, timeout: float) -> float:
    """Probe a single sensor x platform combo by reading one band."""
    from osgeo import gdal
    import pystac_client
    from eof._platform_bindings import get_binding
    from eof._source_configs import get_config

    gdal.PushErrorHandler('CPLQuietErrorHandler')

    binding = get_binding(sensor, platform)
    cfg = get_config(platform)

    # Configure GDAL
    overrides = cfg.configure_gdal()
    vsi_prefix = overrides.get("vsi_prefix", cfg.vsi_prefix)

    # Load geometry for search
    with open(geojson_path) as f:
        features = json.load(f)["features"]
    from shapely.geometry import shape
    import shapely as _shapely
    geom = shape(features[0]["geometry"])
    geojson_dict = json.loads(_shapely.to_geojson(geom))

    # STAC search for one item
    client = pystac_client.Client.open(cfg.stac_url)
    search = client.search(
        collections=[binding.collection],
        intersects=geojson_dict,
        max_items=1,
    )
    items = list(search.items())
    if not items:
        raise RuntimeError("No items found")

    item = items[0]

    # Get the first band's href
    first_band_key = binding.band_asset_keys[0]
    asset = item.assets.get(first_band_key)
    if asset is None:
        raise KeyError(f"Asset '{first_band_key}' not found")

    href = asset.href
    if cfg.transform_href:
        href = cfg.transform_href(href)

    # Build VSI path
    if vsi_prefix == "/vsis3":
        if href.startswith("s3://"):
            vsi_path = f"/vsis3/{href[5:]}"
        else:
            if cfg.s3_endpoint:
                vsi_path = f"/vsis3/{href}"
            else:
                vsi_path = f"/vsis3{href}"
    else:
        if href.startswith("s3://"):
            path = href[5:]
            if cfg.s3_endpoint:
                vsi_path = f"/vsicurl/https://{cfg.s3_endpoint}/{path}"
            else:
                vsi_path = f"/vsicurl/{href}"
        elif href.startswith("http"):
            vsi_path = f"/vsicurl/{href}"
        else:
            vsi_path = f"{vsi_prefix}/{href}"

    # Time the read
    t0 = time.time()
    ds = gdal.Warp(
        '', vsi_path,
        format='MEM',
        cutlineDSName=geojson_path,
        cropToCutline=True,
        xRes=10, yRes=10,
        outputType=gdal.GDT_Int16,
    )
    if ds is None:
        raise IOError(f"GDAL failed to read from {platform}/{sensor}")
    _ = ds.ReadAsArray()
    ds = None
    return time.time() - t0


def print_config_status():
    """Print which credentials are configured."""
    creds = get_cdse_credentials()
    preference = get_data_source_preference()
    available = get_available_sources()

    print("EO Fetch - Data Source Status")
    print("=" * 50)

    # CDSE
    if creds["s3_access_key"] and creds["s3_secret_key"]:
        src = "env" if os.environ.get("CDSE_S3_ACCESS_KEY") else "config"
        print(f"  CDSE (S3 keys):    configured ({src})")
    elif creds["username"] and creds["password"]:
        src = "env" if os.environ.get("CDSE_USERNAME") else "config"
        print(f"  CDSE (user/pass):  configured ({src})")
    else:
        print("  CDSE:              not configured")

    # AWS
    print("  AWS Earth Search:  no credentials needed")

    # Planetary Computer
    if "planetary" in available:
        print("  Planetary Computer: package installed")
    else:
        print("  Planetary Computer: not installed (pip install planetary-computer)")

    # Earthdata
    ed_creds = get_earthdata_credentials()
    if ed_creds["username"] and ed_creds["password"]:
        if os.environ.get("EARTHDATA_USERNAME"):
            print("  NASA Earthdata:    configured (env)")
        else:
            print("  NASA Earthdata:    configured (config/.netrc)")
    elif os.environ.get("EARTHDATA_TOKEN"):
        print("  NASA Earthdata:    configured (token)")
    else:
        print("  NASA Earthdata:    not configured (register at urs.earthdata.nasa.gov)")

    # GEE
    if "gee" in available:
        print("  GEE:               authenticated")
    else:
        print("  GEE:               not authenticated (run: earthengine authenticate)")

    print(f"\n  Preference order:  {preference}")
    print(f"  Available:         {available}")
    print(f"  Config file:       {CONFIG_FILE}")
