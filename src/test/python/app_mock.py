#!/usr/bin/env python

import sys
import logging
import os
import platform
import time

# Ensure compatibility with Python 2 and 3.
# See https://github.com/JioCloud/python-six/blob/master/six.py for details.
PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY2:
    from SimpleHTTPServer import SimpleHTTPRequestHandler
    from SocketServer import TCPServer as HTTPServer
    from urllib2 import Request, urlopen
else:
    from http.server import SimpleHTTPRequestHandler
    from http.server import HTTPServer
    from urllib.request import Request, urlopen

if PY2:
    byte_type = unicode

    def response_status(response):
        return response.getcode()

else:
    byte_type = bytes

    def response_status(response):
        return response.getcode()


def make_handler(appId, version, url):
    """
    Factory method that creates a handler class.
    """

    class Handler(SimpleHTTPRequestHandler):

        def handle_ping(self):
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()

            marathonId = os.getenv("MARATHON_APP_ID", "NO_MARATHON_APP_ID_SET")
            msg = "Pong {}".format(marathonId)

            self.wfile.write(byte_type(msg, "UTF-8"))
            return

        def check_health(self):
            logging.debug("Query %s for health", url)
            url_req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            response = urlopen(url_req)
            res = response.read()
            status = response_status(response)
            logging.debug("Current health is %s, %s", res, status)

            self.send_response(status)
            self.send_header('Content-type', 'text/html')
            self.end_headers()

            self.wfile.write(res)

            logging.debug("Done processing health request.")
            return

        def do_GET(self):
            try:
                logging.debug("Got GET request")
                if self.path == '/ping':
                    return self.handle_ping()
                else:
                    return self.check_health()
            except:
                logging.exception('Could not handle GET request')
                raise

        def do_POST(self):
            try:
                logging.debug("Got POST request")
                return self.check_health()
            except:
                logging.exception('Could not handle POST request')
                raise

    return Handler


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s: %(message)s',
        level=logging.DEBUG)
    logging.info(platform.python_version())
    logging.debug(sys.argv)

    port = int(sys.argv[1])
    appId = sys.argv[2]
    version = sys.argv[3]
    url = "{}/{}".format(sys.argv[4], port)
    taskId = os.getenv("MESOS_TASK_ID", "<UNKNOWN>")

    HTTPServer.allow_reuse_address = True
    httpd = HTTPServer(("", port), make_handler(appId, version, url))
    msg = "AppMock[%s %s]: %s has taken the stage at port %d. "\
          "Will query %s for health status."
    logging.info(msg, appId, version, taskId, port, url)

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

    logging.info("Shutting down.")
    httpd.shutdown()
    httpd.socket.close()
