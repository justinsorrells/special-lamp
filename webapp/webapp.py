from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse
import uvicorn
import socket

HOSTPORTS = {
    'fake subsystem': ('hostname', 6767), 
    'fake subsystem 2': ('hostname', 7676)
}

#in practice will be initialized as empty
schemas = {
    "fake subsystem":{
        "info": {
            "function": None,
            "args": [],
        },
        "hello": {
            "function": None,
            "args": ["str"],
        },
    },
    "fake subsystem 2":{
        "info": {
            "function": None,
            "args": [],
        },
        "hello": {
            "function": None,
            "args": ["str"],
        },
    }
}

app = FastAPI()

last_result: str | None = None
sent_msg: str | None = None

@app.get("/", response_class=HTMLResponse)
def index():
    global last_result
    global sent_msg

    html = "<h1>Debugger</h1>"

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
def run(subsystem:str, entry: str):
    global last_result, sent_msg
    webAppClientSocket.sendto(entry, HOSTPORTS[subsystem])
    sent_msg = f"*sent command {entry} to hostname {subsystem}*"
    dataRec = webAppClientSocket.recvfrom(1024)
    last_result = dataRec
    return RedirectResponse("/", status_code=303)

if __name__ == "__main__":
    webAppClientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for entry in HOSTPORTS:
        webAppClientSocket.sendto(b"gimme schema pls", (HOSTPORTS[entry]))
        dataRec = webAppClientSocket.recvfrom(1024)
        print("commands recieved from ", entry[0], ": ", dataRec)
        newSchemaEntry = {entry: dataRec}
        schemas.append(newSchemaEntry)

    uvicorn.run(app, host="0.0.0.0", port=8000)
