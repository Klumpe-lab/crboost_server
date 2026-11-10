[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Klumpe-lab/crboost_server)

# CryoBoost Server

A cryo-ET pipeline manager in your browser: point it at your movies and mdocs, build a pipeline, and it runs
the processing (RELION, Warp/AreTomo, PyTOM, cryoCARE, IsoNet, easymode, …) as SLURM jobs on the cluster.

## Start CryoBoost

Your server runs on the cluster's headnode under your account; your browser reaches it through an SSH tunnel.

**1. On the headnode**, start it from the folder CryoBoost is installed in (Klumpe lab:
`/groups/klumpe/software/crboost_server`; elsewhere, ask whoever installed it):

```bash
cd /groups/klumpe/software/crboost_server
./crboost
```

It prints a tunnel line and a link for you (your numbers differ):

```
ssh -f -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -L 24305:127.0.0.1:24305 $USER@headnode01
http://localhost:24305/?token=Xq3…
```

**2. On your own computer**, paste the `ssh -f -N …` line into a terminal. If your username there differs from
the cluster's, replace `$USER` with your cluster username.

**3. Open the link** in your browser and bookmark it: the port and token stay the same next time.

Closing the headnode terminal (or `Ctrl-C`) stops the server; jobs already submitted keep running.

<details>
<summary><b>Something doesn't work</b></summary>

| What you see | What to do |
|---|---|
| `ssh` says `Address already in use` | An old tunnel is still open on your computer: run `pkill -f 'ssh.*-L 24305:'` (your port), then paste the tunnel line again. |
| `ssh` can't find the headnode | Replace the name after `@` with the one you log in to the cluster with; connect to the VPN first if the cluster needs it. |
| The page loads forever | The tunnel line went to the headnode instead of your computer (`./crboost` lists such leftovers when it starts), or the tunnel died while your laptop slept: paste it again. |
| The page says `401` | Open the full link with `?token=…` once. |
| The page stopped responding | The server stopped: start it again with `./crboost`. |
| `./crboost` prints the links but doesn't start | Your server is already running: just use the links. |

</details>

<details>
<summary><b>Port, token, tunnel and logs</b></summary>

- **Port:** one of 24300–24331, the same each time it is free. `./crboost --port N` picks one yourself.
- **Token:** kept in `~/.crboost/token`, and in your browser after the first visit. Treat the link like a
  password. For a new token, delete that file and restart; every browser then needs the new link once.
- **Logs:** `~/.crboost/logs/crboost-<port>.log`; `./crboost --debug` logs more.
- **Inside the cluster network** no tunnel is needed: replace `localhost` in the link with the headnode's name.
- **A tunnel you start by name:** put this in `~/.ssh/config` on your computer, then run `ssh -f -N cryoboost`.

  ```
  Host cryoboost
      HostName <headnode>
      User <your-cluster-username>
      LocalForward 24305 127.0.0.1:24305
      ExitOnForwardFailure yes
      ServerAliveInterval 30
  ```

</details>

## Installing and updating

Done once per cluster by whoever maintains the install. Full guide: **[docs/installation.md](docs/installation.md)**.

<details>
<summary>In short</summary>

1. Clone the repository onto the shared filesystem and run `./install.sh`: it builds, inside the folder, the Python
   environment the server runs on.
2. Build the tool containers from `container_defs/` (`warp_aretomo` and `relion` are required).
3. Run `./crboost preflight`, then fill in `config/conf.yaml` (project folder, partitions, container paths).
4. Run `./crboost preflight` until it is clean, then `./crboost test -m cluster`.

To update: `git pull`, then `./crboost`; it builds a new Python environment first when the dependencies changed.
Restart running servers to move them onto it.

</details>

## Documentation

[Installation](docs/installation.md) · [architecture](docs/architecture.md) · [protocols](docs/protocols.md) ·
[particle data flow](docs/particle-data-flow.md) · [binning and resolution](docs/binning-and-resolution.md) ·
[known bugs](docs/known_bugs.md)
