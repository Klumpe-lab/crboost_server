# CryoBoost Server

```
I think the best would be something like that and you start the flask server on the headnode and interact with it throgh the browser of your labtop or whatever.
https://flask.palletsprojects.com/en/stable/quickstart/#a-minimal-application
The only remaining problem would be the container for the applicaion if needed.
And of course we can still seperate the interactive from the server process.

Florian
```

A fastapi-based implementation of a cryoboost scheduler process.

The idea is to have cryoboost dispatcher work via a stock server like Flask or FastAPI running directly on the headnode and interacting only with a the cluster's filesystem while exposing the UI via a single port on the headnode's parent network s.t. it is accessible via a simple browser window anywhere on the network.

The proof of concept works already. To see the UI running on the cluster's subnet from anywhere on the general institution network -- open ssh tunnel with a port forwarding:

```
ssh -L ${LOCAL_PORT}:localhost:${HEADNODE_PORT} ${USERNAME}@${HEADNODE_URL}
```
or background:
```
ssh -f -N -L ${LOCAL_PORT}:localhost:${HEADNODE_PORT} ${USERNAME}@${HEADNODE_URL}
```
(kill when done:  `pkill -f "ssh.*${LOCAL_PORT}:localhost:${HEADNODE_PORT}"`)

- `LOCAL_PORT` is any free port of choosing on your computer
- `HEADNODE_PORT` is the port on which this software (crboost_server) is running on the headnode
- `USERNAME` and `HEADNODE_URL` are the credentials for your local cluster setup

This, of course, assumes that `USERNAME` has previously added their public key to the cluster's ssh folder (usually done for you by Slurm's admins).

You may also want to save this configuration to your local sshconfig (example):

```
Host cryoboost-tunnel
    HostName clip-login-0.cbe.vbc.ac.at
    User artem.kushner
    LocalForward 8080 localhost:1717
    LocalForward 8081 localhost:1718
    LocalForward 8082 localhost:1719
```

Then, `ssh cryoboost-tunnel` suffices on local.




```
Your Laptop          SSH Tunnel               Head Node
┌─────────────┐     ┌─────────────────┐     ┌──────────────┐
│  Browser    │────►│ Port 8080       │────►│ Port 1717    │
│ localhost:  │     │       ↓         │     │ CryoBoost    │
│   8080      │     │ SSH Connection  │     │ Server       │
└─────────────┘     └─────────────────┘     └──────────────┘
```