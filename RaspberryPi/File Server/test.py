#!/usr/bin/env python3
from http.server import HTTPServer, BaseHTTPRequestHandler

class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/helloworld':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            
            html = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Hello World</title>
                <style>
                    body {                        font-family: Arial, sans-serif;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        height: 100vh;
                        margin: 0;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    }
                    .container {
                        text-align: center;
                        background: white;
                        padding: 50px;
                        border-radius: 10px;
                        box-shadow: 0 10px 25px rgba(0,0,0,0.2);
                    }
                    h1 {
                        color: #333;
                        margin: 0;
                    }
                    p {
                        color: #666;
                        margin-top: 10px;
                    }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>🎉 Hello World! 🎉</h1>
                    <p>Your Raspberry Pi server is working!</p>
                </div>
            </body>
            </html>
            """
            self.wfile.write(html.encode())
        else:
            self.send_response(404)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b'<h1>404 - Not Found</h1><p>Try /helloworld</p>')
    
    def log_message(self, format, *args):
        print(f"{self.address_string()} - {format % args}")

def run(port=8080):
    server_address = ('', port)
    httpd = HTTPServer(server_address, SimpleHandler)
    print(f"Server running on port {port}")
    print(f"Access it at http://<your-ip>:{port}/helloworld")
    httpd.serve_forever()

if __name__ == '__main__':
    run()