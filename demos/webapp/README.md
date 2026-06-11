# Webapp Demo

`demos/webapp.py` is a small FastAPI debugging surface for sending commands to
the controller over its Unix socket.

Run it from the repo root:

```sh
python demos/webapp.py --socket-path /tmp/hyperloop-controller.sock
```

Open `http://127.0.0.1:8000/`, choose a board and command, and submit. The app
opens a local Unix-socket connection to the controller, sends a v1 `command`, and
renders the controller `response`.

The webapp does not connect directly to boards and does not use UDP.
