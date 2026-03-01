#!/usr/bin/env python3
"""
SponsorBlock Archive File Server

Serves staticData.sqlite3, daily segment databases, and monthly .7z archives
from a NAS mount. Intended to sit behind a PythonAnywhere proxy that hides
this server's IP address from end users.

All endpoints require a Bearer token in the Authorization header.
"""

import os
import re
import json
import logging
import logging.handlers
import mimetypes
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from config import ARCHIVE_PATH, DAILY_FILES_DIR, MONTHLY_ARCHIVES_DIR, PUBLIC_FILES_DIR, AUTH_TOKEN, PORT, HELPER_FUNCTIONS_PATH

CHUNK_SIZE = 64 * 1024  # 64KB

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "file_server.log")

logger = logging.getLogger("file_server")
logger.setLevel(logging.INFO)

_handler = logging.handlers.RotatingFileHandler(
    LOG_FILE,
    maxBytes=10 * 1024 * 1024,  # rotate at 10 MB
    backupCount=5,               # keeps file_server.log.1 … .5
    encoding="utf-8",
)
_handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
))
logger.addHandler(_handler)

# Filename validation patterns (prevents path traversal)
DAILY_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}_segmentData\.sqlite3$')
MONTHLY_PATTERN = re.compile(r'^sponsorTimes_\d{4}-\d{2}\.7z$')


def format_size(size_bytes):
    """Convert bytes to human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def resolve_filepath(filename):
    """
    Validate a filename and return its full path on disk.
    Returns None if the filename doesn't match any allowed pattern.
    """
    # Strip any directory components as a safety measure
    filename = os.path.basename(filename)

    if filename == "staticData.sqlite3":
        return os.path.join(ARCHIVE_PATH, filename)

    if filename == "helper_functions.py":
        return HELPER_FUNCTIONS_PATH

    if DAILY_PATTERN.match(filename):
        return os.path.join(ARCHIVE_PATH, DAILY_FILES_DIR, filename)

    if MONTHLY_PATTERN.match(filename):
        return os.path.join(ARCHIVE_PATH, MONTHLY_ARCHIVES_DIR, filename)

    # Public files: any file directly in the public directory
    public_path = os.path.join(PUBLIC_FILES_DIR, filename)
    if os.path.isfile(public_path):
        return public_path

    return None


def get_file_metadata(filepath, filename, file_type):
    """Build a metadata dict for a single file."""
    stat = os.stat(filepath)
    return {
        "filename": filename,
        "size_bytes": stat.st_size,
        "size_human": format_size(stat.st_size),
        "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec='seconds'),
        "type": file_type,
    }


def get_all_files():
    """Scan the archive and return file metadata grouped by type."""
    result = {"static": [], "daily": [], "monthly": [], "tools": []}

    # Static database
    static_path = os.path.join(ARCHIVE_PATH, "staticData.sqlite3")
    if os.path.isfile(static_path):
        result["static"].append(get_file_metadata(static_path, "staticData.sqlite3", "static"))

    # Helper functions script
    if os.path.isfile(HELPER_FUNCTIONS_PATH):
        result["tools"].append(get_file_metadata(HELPER_FUNCTIONS_PATH, "helper_functions.py", "tools"))

    # Daily files
    daily_dir = os.path.join(ARCHIVE_PATH, DAILY_FILES_DIR)
    if os.path.isdir(daily_dir):
        for entry in sorted(os.listdir(daily_dir), reverse=True):
            if DAILY_PATTERN.match(entry):
                path = os.path.join(daily_dir, entry)
                if os.path.isfile(path):
                    result["daily"].append(get_file_metadata(path, entry, "daily"))

    # Monthly archives
    monthly_dir = os.path.join(ARCHIVE_PATH, MONTHLY_ARCHIVES_DIR)
    if os.path.isdir(monthly_dir):
        for entry in sorted(os.listdir(monthly_dir), reverse=True):
            if MONTHLY_PATTERN.match(entry):
                path = os.path.join(monthly_dir, entry)
                if os.path.isfile(path):
                    result["monthly"].append(get_file_metadata(path, entry, "monthly"))

    return result


def get_public_files():
    """Scan the public files directory and return metadata for all top-level files."""
    files = []
    if not os.path.isdir(PUBLIC_FILES_DIR):
        return files
    for entry in sorted(os.listdir(PUBLIC_FILES_DIR)):
        if entry.startswith("."):
            continue
        path = os.path.join(PUBLIC_FILES_DIR, entry)
        if os.path.isfile(path):
            files.append(get_file_metadata(path, entry, "public"))
    return files


class FileServerHandler(BaseHTTPRequestHandler):

    def check_auth(self):
        """Return True if the request has a valid auth token."""
        auth_header = self.headers.get("Authorization", "")
        return auth_header == AUTH_TOKEN

    def send_json(self, status, data):
        """Send a JSON response."""
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_error_json(self, status, message):
        """Send a JSON error response."""
        self.send_json(status, {"error": message})

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)

        # Auth check on all endpoints
        if not self.check_auth():
            self.send_error_json(401, "Unauthorized")
            return

        # Check NAS is mounted
        if not os.path.isdir(ARCHIVE_PATH):
            logger.error("NAS not mounted - rejecting request from %s", self.address_string())
            self.send_error_json(503, "Archive storage is not available")
            return

        if path == "/api/files":
            self.handle_file_list()
        elif path == "/api/public_files":
            self.handle_public_file_list()
        elif path == "/api/download":
            filename = params.get("file", [None])[0]
            inline = params.get("inline", ["0"])[0] == "1"
            self.handle_download(filename, inline)
        else:
            self.send_error_json(404, "Not found")

    def handle_file_list(self):
        """Return JSON listing of all available files."""
        try:
            files = get_all_files()
            self.send_json(200, files)
        except OSError as e:
            self.send_error_json(500, f"Error scanning files: {e}")

    def handle_public_file_list(self):
        """Return JSON listing of all public files."""
        try:
            files = get_public_files()
            self.send_json(200, files)
        except OSError as e:
            self.send_error_json(500, f"Error scanning public files: {e}")

    def handle_download(self, filename, inline=False):
        """Stream a file download. If inline=True, serve for browser viewing."""
        client_ip = self.headers.get("X-Forwarded-For", self.address_string())
        action = "view" if inline else "download"

        if not filename:
            logger.warning("%s %s FAIL 400 missing_file_parameter", client_ip, action)
            self.send_error_json(400, "Missing 'file' parameter")
            return

        filepath = resolve_filepath(filename)
        if filepath is None:
            logger.warning("%s %s FAIL 400 invalid_filename requested=%s", client_ip, action, filename)
            self.send_error_json(400, "Invalid filename")
            return

        if not os.path.isfile(filepath):
            logger.warning("%s %s FAIL 404 file=%s", client_ip, action, filename)
            self.send_error_json(404, "File not found")
            return

        try:
            file_size = os.path.getsize(filepath)
            safe_filename = os.path.basename(filepath)
            logger.info("%s %s OK file=%s size=%s", client_ip, action, safe_filename, format_size(file_size))

            content_type = "application/octet-stream"
            disposition = "attachment"
            if inline:
                guessed = mimetypes.guess_type(safe_filename)[0]
                if guessed:
                    content_type = guessed
                disposition = "inline"

            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Disposition", f'{disposition}; filename="{safe_filename}"')
            self.send_header("Content-Length", str(file_size))
            self.end_headers()

            with open(filepath, "rb") as f:
                while True:
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    self.wfile.write(chunk)

        except (BrokenPipeError, ConnectionResetError):
            # Client disconnected mid-download, nothing to do
            logger.warning("%s %s INTERRUPTED file=%s", client_ip, action, filename)
        except OSError as e:
            # Can't send error response if headers are already sent
            logger.error("%s %s ERROR file=%s error=%s", client_ip, action, filename, e)
            print(f"Error streaming file {filename}: {e}")

    def log_message(self, format, *args):
        """Route the built-in per-request log to the rotating log file."""
        logger.info("%s - %s", self.address_string(), format % args)


def run():
    server_address = ('', PORT)
    httpd = HTTPServer(server_address, FileServerHandler)
    logger.info("Server starting - port=%d archive=%s", PORT, ARCHIVE_PATH)
    print(f"SponsorBlock Archive File Server")
    print(f"Serving files from: {ARCHIVE_PATH}")
    print(f"Listening on port {PORT}")
    httpd.serve_forever()


if __name__ == '__main__':
    run()