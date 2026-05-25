from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse
import json
import socket
import time
import uvicorn

HOSTPORTS = (
    ("127.0.0.1", 6767),
    ("127.0.0.1", 7676),
)

# in practice will be initialized as empty
schemas = {}

app = FastAPI()

last_result: str | None = None
sent_msg: str | None = None


def request_server_schema(addr):
    request = {
        "type": "command",
        "time": time.time(),
        "sequence_no": 0,
        "cmd": "info",
        "args": [],
    }
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_socket.settimeout(0.5)
    print(json.dumps(request).encode('utf-8'))
    client_socket.sendto(json.dumps(request).encode("utf-8"), addr)
    try:
        data, addr = client_socket.recvfrom(1024)
        res = json.loads(data.decode())
        print(res)
    except socket.timeout:
        res = {}
        print("No response")
    return res


@app.get("/", response_class=HTMLResponse)
def index():
    global last_result
    global sent_msg
    query = request_server_schema(("127.0.0.1", 6767))
    hostname = query.get("hostname", None)
    if hostname:
        schemas[hostname] = query
    html = "<h1>Schema</h1>"
    html += f"{query}"
    html += "<h1>Debugger</h1>"
    if last_result is not None:
        html += f"<p>{sent_msg}</p>"
        html += f"<p><strong>Result of call:</strong> {last_result}</p>"

    for subsystem, commands in schemas.items():
        html += f"<h2>-- {subsystem} --</h2>"
        html += '<div style="display: flex; flex-direction: row; gap: 12px;">'
        for name in commands:
            html += (
                f'<form action="/run/{subsystem}/{name}" method="post">'
                f'<button type="submit">{name}</button>'
                "</form>"
            )
        html += "</div>"

    return html


@app.post("/run/{subsystem}/{entry}")
def run(subsystem: str, entry: str):
    global last_result, sent_msg
    webAppClientSocket.sendto(entry, HOSTPORTS[subsystem])
    sent_msg = f"*sent command {entry} to hostname {subsystem}*"
    dataRec = webAppClientSocket.recvfrom(1024)
    last_result = dataRec
    return RedirectResponse("/", status_code=303)


if __name__ == "__main__":
    for entry in HOSTPORTS:
        dataRec = request_server_schema(entry)
        print("commands recieved from ", entry[0], ": ", dataRec)
    uvicorn.run(app, host="0.0.0.0", port=8000)
