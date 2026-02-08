# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import threading
from ..io import avro_reader, avro_writer, read_avro_schema, avro_schema
from ..url import URL, parse_url
from avro.datafile import DataFileReader
from avro.io import DatumReader
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
import argparse
import io
import os
import json
import logging
import shutil

logger = logging.getLogger(__name__)


class AvroHTTPRequestHandler(BaseHTTPRequestHandler):
    def __init__(self, root_url: URL, *args, **kwargs):
        self.root_url = root_url
        super().__init__(*args, **kwargs)

    def _first_query_param(self, name: str, params: dict[str, list[str]]) -> str | None:
        values = params.get(name)
        return values[0] if values and len(values) > 0 else None

    def _is_json_record(self, mime_type: str | None) -> bool:
        """
        Check if the content type is JSON.
        """
        return mime_type == "application/json"

    def _is_json_lines(self, mime_type: str | None) -> bool:
        """
        Check if the content type is JSON Lines or a variant of JSON Lines.
        """
        return (
            mime_type == "application/x-ndjson"
            or mime_type == "application/jsonl"
            or mime_type == "text/x-jsonl"
        )

    def _is_avro_binary(self, mime_type: str | None) -> bool:
        """
        Check if the content type is Avro binary or a variant of Avro.
        """
        return (
            mime_type == "application/octet-stream" or mime_type == "application/avro"
        )

    def do_PUT(self):
        """
        PUT request handler to create or overwrite an Avro file with the provided schema.
        """
        try:
            url = urlparse(self.path)
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            try:
                # Parse the JSON body to get the schema
                schema = avro_schema(json.loads(body))
                # Open the file for writing with the provided schema, which creates or overwrites it
                data_url = self.root_url.with_path(url.path)
                data_url_exists = data_url.exists()
                with avro_writer(data_url.with_mode("wb"), schema):
                    pass
                self.send_response(200 if data_url_exists else 201)
                self.end_headers()
            except json.JSONDecodeError:
                logger.error("Invalid JSON received")
                self.send_response(400)
                self.end_headers()
        except Exception as e:
            logger.error("Error processing request: %s", e)
            self.send_response(500)
            self.end_headers()

    def do_POST(self):
        """
        POST request handler to append data to an existing Avro file.
        """
        try:
            url = urlparse(self.path)
            try:
                # Check to see if the file exists
                data_url = self.root_url.with_path(url.path).with_mode("a+b")
                if not data_url.exists():
                    self.send_response(404)
                    self.end_headers()
                    return
                # Append the data to the existing Avro file (use its writers schema)
                content_type = self.headers.get("Content-Type")
                if self._is_json_record(content_type):
                    content_length = int(self.headers.get("Content-Length", 0))
                    body = self.rfile.read(content_length)
                    with avro_writer(data_url) as writer:
                        data = json.loads(body)
                        if isinstance(data, list):
                            for record in data:
                                writer.append(record)
                        else:
                            writer.append(data)
                    self.send_response(200)
                    self.end_headers()
                elif self._is_json_lines(content_type):
                    with avro_writer(data_url) as writer:
                        for line in self.rfile:
                            writer.append(json.loads(line))
                    self.send_response(200)
                    self.end_headers()
                elif self._is_avro_binary(content_type):
                    # TODO Don't do this in memory
                    content_length = int(self.headers.get("Content-Length", 0))
                    body = self.rfile.read(content_length)
                    with DataFileReader(
                        io.BytesIO(body), DatumReader()
                    ) as reader, avro_writer(data_url) as writer:
                        for record in reader:
                            writer.append(record)
                    self.send_response(200)
                    self.end_headers()
                else:
                    logger.warning("Unsupported Content-Type header: %s", content_type)
                    self.send_response(406)
                    self.end_headers()
            except json.JSONDecodeError:
                logger.error("Invalid JSON received")
                self.send_response(400)
                self.end_headers()
        except Exception as e:
            logger.error("Error processing request: %s", e)
            self.send_response(500)
            self.end_headers()

    def do_DELETE(self):
        """
        DELETE request handler to delete an Avro file.
        """
        try:
            url = urlparse(self.path)
            data_url = self.root_url.with_path(url.path)
            if data_url.exists():
                data_url.delete()
                self.send_response(204)
                self.end_headers()
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as e:
            logger.error("Error processing request: %s", e)
            self.send_response(500)
            self.end_headers()

    def do_GET(self):
        try:
            url = urlparse(self.path)
            # If root path, list all files
            if url.path == "/":
                urls = self.root_url.expand()
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps([os.path.basename(u.url) for u in urls]).encode("utf-8")
                )
                return
            data_url = self.root_url.with_path(url.path).with_mode("rb")
            params = parse_qs(url.query)
            # Check to see if the file exists
            if not data_url.exists():
                self.send_response(404)
                self.end_headers()
                return
            # Show schema if ?schema=true
            show_schema = self._first_query_param("schema", params) == "true"
            if show_schema:
                schema = read_avro_schema(data_url)
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(schema.to_json(), indent=2).encode("utf-8"))
                return
            # Show the data based on the accept header
            accept = self.headers.get("Accept")
            if self._is_json_record(accept) or self._is_json_lines(accept):
                with avro_reader(data_url) as reader:
                    self.send_response(200)
                    self.send_header("Content-type", "application/jsonl")
                    self.end_headers()
                    for record in reader:
                        self.wfile.write(json.dumps(record).encode("utf-8"))
                        self.wfile.write(b"\n")
            elif self._is_avro_binary(accept):
                with data_url as f:
                    self.send_response(200)
                    self.send_header("Content-type", "application/avro")
                    self.end_headers()
                    shutil.copyfileobj(f, self.wfile)
            else:
                logger.warning("Unsupported Accept header: %s", accept)
                self.send_response(406)
                self.end_headers()
        except Exception as e:
            logger.error("Error processing request: %s", e)
            self.send_response(500)
            self.end_headers()


def handler_factory(root_url: URL):
    def create_handler(*args, **kwargs):
        return AvroHTTPRequestHandler(root_url, *args, **kwargs)

    return create_handler


class HttpServerTool:
    def __init__(self) -> None:
        self.httpd: HTTPServer | None = None
        self.port: int | None = None
        self._started = threading.Event()

    def name(self) -> str:
        return "httpserver"

    def start(self, root_url: URL, port: int) -> None:
        logger.info("Starting server on port %d with root %s", port, root_url)
        self.httpd = HTTPServer(("", port), handler_factory(root_url))
        try:
            self.port = self.httpd.server_port
            self._started.set()
            self.httpd.serve_forever()
        except KeyboardInterrupt:
            logger.info("Stopping server")
        finally:
            self.httpd.server_close()

    def wait_until_started(self, timeout: int | None = None) -> None:
        """
        Wait until the server is started.
        """
        if not self._started.is_set():
            self._started.wait(timeout)

    def stop(self) -> None:
        if self.httpd:
            logger.info("Shutting down server")
            self.httpd.shutdown()

    def configure(self, subparsers: argparse._SubParsersAction) -> None:
        parser = subparsers.add_parser(
            self.name(), help="Start an HTTP server for Avro files."
        )
        parser.add_argument(
            "root_url",
            help="Root directory for Avro data files.",
        )
        parser.add_argument(
            "--port",
            type=int,
            default=8765,
            help="Port for the HTTP server.",
        )

    def run(self, args: argparse.Namespace) -> None:
        self.start(parse_url(args.root_url), args.port)
