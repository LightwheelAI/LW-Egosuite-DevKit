"""lwviz Static File Server Module
Server logic extracted from cli.py to launch a static file server and open the lwviz website.
"""
import tornado.web
import tornado.ioloop
from typing import Optional
from pathlib import Path
from datetime import datetime as dt
import webbrowser
import urllib.parse
import logging
from dataclasses import dataclass
import socket

logger = logging.getLogger(__name__)


def open_url(url: str, open_browser: bool = True) -> None:
    """Open URL, optionally auto-open browser"""
    logger.info(
        "If browser does not open automatically, please open this url: %s", url)
    if open_browser:
        try:
            browser = webbrowser.get()
            browser.open(url)
        except Exception:
            logger.warning("Could not open browser")


def random_ports(port: int, n: int):
    """Generate a list of ports containing n ports. The first 5 ports are consecutive, and the remaining ports are randomly selected from the range [port-2*n, port+2*n].
    """
    import random
    for i in range(min(5, n)):
        yield port + i
    for i in range(n - 5):
        yield max(1, port + random.randint(-2 * n, 2 * n))


class CORSStaticFileHandler(tornado.web.StaticFileHandler):
    """Static file handler supporting CORS"""

    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header("Access-Control-Expose-Headers", "Accept-Ranges")

    async def options(self, path, *args):
        self.path = self.parse_url_path(path)
        self.absolute_path = self.get_absolute_path(self.root, self.path)
        self.modified = self.get_modified_time()
        self.set_status(204)
        self.finish()


class lwvizFileServer:
    """lwviz Static File Server
Starts a static file server to serve MCAP files
and automatically opens the lwviz website.
    """

    def __init__(
        self,
        file_path: Path,
        hostname: str = "0.0.0.0",
        port: int = 12312,
        server_only: bool = False,
        timestamp: Optional[float] = None,
        additional_files: Optional[list[Path]] = None,
    ):
        """
        Args:
            file_path: Path to the main MCAP file
            hostname: Server hostname
            port: Server port
            server_only: If True, do not automatically open the browser
            timestamp: Optional timestamp to navigate to a specific time in lwviz
            additional_files: Optional list of additional MCAP files to serve
        """
        self.file_path = Path(file_path).absolute()
        self.hostname = hostname
        self.port = port
        self.server_only = server_only
        self.timestamp = timestamp
        self.additional_files = additional_files or []

        # Validate main file
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")

        if self.file_path.suffix != ".mcap":
            raise ValueError(
                f"Unsupported file type: {self.file_path.suffix}. "
                "Only .mcap files are supported."
            )

        # Validate additional files
        for additional_file in self.additional_files:
            additional_file = Path(additional_file).absolute()
            if not additional_file.exists():
                raise FileNotFoundError(
                    f"Additional file not found: {additional_file}")
            if additional_file.suffix != ".mcap":
                raise ValueError(
                    f"Unsupported file type for additional file: {additional_file.suffix}. "
                    "Only .mcap files are supported."
                )

    def _get_data_source_type(self) -> str:
        """Returns the data source type (fixed as \"mcap\")"""
        return "mcap-remote-file"

    def _build_lwviz_url(self, actual_hostname: str, actual_port: int) -> str:
        """Build lwviz URL with support for multiple files"""
        ds = self._get_data_source_type()

        # Process hostname
        if actual_hostname in ["0.0.0.0", "localhost"]:
            actual_hostname = "127.0.0.1"

        # Build URLs for all files
        file_urls = []

        # Main file
        main_file_url = f"http://{actual_hostname}:{actual_port}/{self.file_path.name}"
        file_urls.append(main_file_url)

        # Additional files
        for additional_file in self.additional_files:
            additional_file = Path(additional_file).absolute()
            additional_file_url = f"http://{actual_hostname}:{actual_port}/{additional_file.name}"
            file_urls.append(additional_file_url)

        # Join all file URLs with comma separator
        all_file_urls = ",".join(file_urls)
        file_urls_encoded = urllib.parse.quote(all_file_urls, safe='')

        # Build lwviz URL
        url = f"https://foxviz.lightwheel.net/?ds={ds}&ds.url={file_urls_encoded}"

        # Add default layout parameter
        # Use project root relative path for default layout
        project_root = Path(__file__).parent.parent
        default_layout_path = project_root / "assets" / "default_layout.json"
        if default_layout_path.exists():
            layout_url = f"http://{actual_hostname}:{actual_port}/default_layout.json"
            logger.info(f"Using default layout: {layout_url}")
            layout_url_encoded = urllib.parse.quote(layout_url, safe='')
            url += f"&layout={layout_url_encoded}"

        # Add timestamp parameter (if provided)
        if self.timestamp is not None and self.timestamp != -1.0:
            date = dt.fromtimestamp(self.timestamp)
            date_str = date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            date_str_quoted = urllib.parse.quote(date_str)
            url += f"&time={date_str_quoted}"

        return url

    def start(self) -> None:
        """Start the server and open the Lichtblick website"""
        # Create route list for all files
        routes = []

        # Add main file route
        routes.append((
            rf'/({self.file_path.name})$',
            CORSStaticFileHandler,
            {'path': str(self.file_path.parent)}
        ))

        # Add additional files routes
        for additional_file in self.additional_files:
            additional_file = Path(additional_file).absolute()
            routes.append((
                rf'/({additional_file.name})$',
                CORSStaticFileHandler,
                {'path': str(additional_file.parent)}
            ))

        # Add default layout route if exists
        # Use project root relative path for default layout
        project_root = Path(__file__).parent.parent
        default_layout_path = project_root / "assets" / "default_layout.json"
        if default_layout_path.exists():
            routes.append((
                rf'/default_layout.json.mcap$',
                CORSStaticFileHandler,
                {'path': str(default_layout_path.parent)}
            ))
        # Create Tornado Application
        app = tornado.web.Application(routes)

        # Attempting to bind to port (if port is occupied, try another port)
        actual_port = None
        for p in random_ports(self.port, 10):
            try:
                app.listen(p, address=self.hostname)
                actual_port = p
                break
            except socket.error as e:
                logger.info("Port %s in use, trying another.", p)
        else:
            logger.error("No available port found.")
            raise RuntimeError("Could not find an available port")

        # Build URL
        actual_hostname = self.hostname
        if actual_hostname in ["0.0.0.0", "localhost"]:
            actual_hostname = "127.0.0.1"

        # Build URLs for all files
        file_urls = []

        # Main file
        main_file_url = f"http://{actual_hostname}:{actual_port}/{self.file_path.name}"
        file_urls.append(main_file_url)

        # Additional files
        for additional_file in self.additional_files:
            additional_file = Path(additional_file).absolute()
            additional_file_url = f"http://{actual_hostname}:{actual_port}/{additional_file.name}"
            file_urls.append(additional_file_url)

        lwviz_url = self._build_lwviz_url(actual_hostname, actual_port)

        logger.info("%s", "=" * 60)
        logger.info("lwviz web server started")
        for i, file_url in enumerate(file_urls):
            if i == 0:
                logger.info("Main file: %s", file_url)
            else:
                logger.info("Additional file %d: %s", i, file_url)
        logger.info("%s", "=" * 60)
        logger.info("lwviz URL: %s", lwviz_url)

        # Open the browser (if not in server_only mode)
        open_url(lwviz_url, not self.server_only)

        logger.info("Press Ctrl-C to stop the server")

        # Start event loop
        try:
            tornado.ioloop.IOLoop.instance().start()
        except KeyboardInterrupt:
            tornado.ioloop.IOLoop.instance().stop()
            logger.info("Bye!")


def serve_file(
    file_path: Path,
    hostname: str = "0.0.0.0",
    port: int = 12312,
    server_only: bool = False,
    timestamp: Optional[float] = None,
) -> None:
    """Convenience function: Start a file server and open the lwviz website
    Args:
        file_path: Path to the MCAP file
        hostname: Server hostname
        port: Server port
        server_only: If True, do not automatically open the browser
        timestamp: Optional timestamp to navigate to a specific time in lwviz
    """
    server = lwvizFileServer(
        file_path=file_path,
        hostname=hostname,
        port=port,
        server_only=server_only,
        timestamp=timestamp,
    )
    server.start()


@dataclass
class Show:
    """Launch lwviz server to display MCAP file. If the input path is not an .mcap file, the data will be automatically converted to MCAP format.
    """
    in_path: Path = Path("./output/output.mcap")
    """Input path (can be a directory or an .mcap file)"""

    start_convert_time: int = -1
    """Start converting timestamp (19-digit number)"""

    end_convert_time: int = -1
    """End conversion timestamp (19-digit number)"""

    hostname: str = "0.0.0.0"
    """Server hostname"""

    port: int = 12312
    """Server Port"""

    refresh_cache: bool = False
    """Refresh cache?"""

    high_compression: bool = False
    """Whether to use high compression (zstd)"""

    server_only: bool = False
    """Start server only (do not open browser)"""

    timestamp: float = -1.0
    """Timestamp located in lwviz"""

    def __post_init__(self):
        """Initialize post-processing"""
        self.in_path = Path(self.in_path)

        # Convert timestamp to None (if it is the default value)
        if self.timestamp == -1.0:
            self.timestamp = None

    def _validate_time_params(self):
        """Validate time parameters"""
        if self.start_convert_time != -1 and self.end_convert_time != -1:
            logger.info(
                "Both start time and end time given, will use given timestamp ...")
            if (self.start_convert_time < -1 or self.end_convert_time < -1 or
                len(str(self.start_convert_time)) != 19 or
                    len(str(self.end_convert_time)) != 19):
                raise ValueError(
                    "Given start time or end time not correct, please check! "
                    "Expected 19-digit timestamps."
                )
            elif self.start_convert_time > self.end_convert_time:
                raise ValueError(
                    "Given start time is larger than end time, please check!"
                )
        elif self.start_convert_time != -1 and self.end_convert_time == -1:
            logger.info("Start_convert_time set, will convert from given start timestamp, "
                        "please ensure the given timestamp is in this time period.")
        elif self.start_convert_time == -1 and self.end_convert_time != -1:
            logger.info("End_convert_time set, will convert to given end timestamp, "
                        "please ensure the given timestamp is in this time period.")
        elif self.start_convert_time == -1 and self.end_convert_time == -1:
            logger.info(
                "No start_convert_time nor end_convert_time given, will use default timestamp.")

    def _prepare_file_path(self) -> Path:
        """Prepare file path, and convert if needed"""
        in_path_str = str(self.in_path)

        # If it is already an .mcap file, return directly.
        if in_path_str.endswith(".mcap"):
            return self.in_path.absolute()

        else:
            raise ValueError("please first convert the data to mcap format")

    def run(self):
        """Run the show command"""
        # Validate time parameters
        self._validate_time_params()

        # Prepare file path (if conversion is needed)
        file_path = self._prepare_file_path()

        # Auto-generate visualization file path
        additional_files = []
        if file_path.suffix == ".mcap":
            # Generate visualization file path: ./output/{input_filename}_vis.mcap
            input_filename = file_path.stem
            vis_filename = f"{input_filename}_vis.mcap"
            vis_file_path = file_path.parent / "output" / vis_filename

            if vis_file_path.exists():
                additional_files.append(vis_file_path)
                logger.info(f"Found visualization file: {vis_file_path}")
            else:
                logger.warning(
                    f"Visualization file not found: {vis_file_path}")

        # Start server with both files
        server = lwvizFileServer(
            file_path=file_path,
            hostname=self.hostname,
            port=self.port,
            server_only=self.server_only,
            timestamp=self.timestamp,
            additional_files=additional_files,
        )
        server.start()
