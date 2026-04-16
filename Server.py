from flask import Flask, request
import time
import argparse

app = Flask(__name__)

@app.route("/")
def index():
    # simulate small processing
    time.sleep(0.05)
    return "ok\n"

@app.route("/stats")
def stats():
    return "status: ok\n"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    app.run(host=args.host, port=args.port)
