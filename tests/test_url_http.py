# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import pytest
import tempfile
import http.server
import socketserver
import threading
import os

from avrokit.url.factory import parse_url


class CustomHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_POST(self):
        # Determine the file path relative to the server root
        file_path = self.translate_path(self.path)
        # Read the content length from headers
        content_length = int(self.headers["Content-Length"])
        # Read the file data from the request body
        file_data = self.rfile.read(content_length)
        # Write the data to the specified file
        with open(file_path, "wb") as f:
            f.write(file_data)
        # Respond to the client
        self.send_response(201)
        self.end_headers()
        self.wfile.write(b"File uploaded successfully")

    def do_DELETE(self):
        # Determine the file path relative to the server root
        file_path = self.translate_path(self.path)
        try:
            # Attempt to delete the file
            os.remove(file_path)
            # Respond with success
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"File deleted successfully")
        except FileNotFoundError:
            # File not found
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"File not found")
        except Exception:
            # Other errors
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"Internal server error")


@pytest.fixture(scope="module")
def http_file_server():
    with tempfile.TemporaryDirectory() as tmp:
        # Create a simple HTTP server
        httpd = socketserver.TCPServer(("localhost", 0), CustomHTTPRequestHandler)
        port = httpd.server_address[1]
        old_cwd = os.getcwd()
        os.chdir(tmp)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        # Yield the URL of the server and the root directory
        yield {"url": f"http://localhost:{port}", "root": tmp}
        # Shut down the server
        httpd.shutdown()
        thread.join()
        os.chdir(old_cwd)


class TestHttpURL:
    def test_crud(self, http_file_server):
        root_url = http_file_server["url"]
        root = http_file_server["root"]
        # Write a file to the server
        url = parse_url(f"{root_url}/test.txt", mode="w")
        assert not url.exists()
        with url as f:
            f.write("Hello, world!")
        assert os.path.exists(os.path.join(root, "test.txt"))
        assert url.exists()
        # Read the file from the server
        with url.with_mode("r") as f:
            content = f.read()
        assert content == "Hello, world!"
        # Read the size of the file
        assert url.size() == len(content)
        # Delete the file from the server
        url.delete()
        assert not url.exists()
