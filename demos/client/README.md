# Unix Socket Demo Client

`demos/client/client.py` is a persistent local client for the controller Unix
socket. It sends v1 `command` or `estop_reset` messages and continuously reads
newline-JSON `response` and unsolicited `event` messages from the same
full-duplex connection.

Run a single command:

```sh
python demos/client/client.py --socket-path /tmp/hyperloop-controller.sock \
  --target motor --command status
```

Run a command with arguments:

```sh
python demos/client/client.py --socket-path /tmp/hyperloop-controller.sock \
  --target motor --command move rpm=1200
```

Reset software e-stop after the electrical condition is cleared:

```sh
python demos/client/client.py --socket-path /tmp/hyperloop-controller.sock --estop-reset
```

Watch all inbound messages:

```sh
python demos/client/client.py --socket-path /tmp/hyperloop-controller.sock --watch
```

This client never talks directly to a board. Board communication remains owned
by the controller.
