# Instructions for running DGL Distributed exeriments on slurm
#### <small> All of these steps are better done in conda </small>
## Step 1. Buld Pytorch from source files
### 1. Clone the PyTorch repo (without the --recursive flag)
```
git clone https://github.com/pytorch/pytorch
```
### 2. Re-route the gloo submodule url in the .gitmodules file to <a> https://github.com/rustambaku13/gloo</a>
### 3. git update sync and build according to pytorch documentation  

```
cd pytorch  
git submodule sync  
git submodule update --init --recursive --jobs 0  
```

## Step 2. Build the DGL library from source files
### 1. Follow the instruction on the DGL homepage
### 2. locate dgl/distributed/rpc_client.py and replace this function
```
def get_local_usable_addr(probe_addr):
    """Get local usable IP and port

    Returns
    -------
    str
        IP address, e.g., '192.168.8.12:50051'
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # should get the address on the same subnet as probe_addr's
        sock.connect((probe_addr, 1))
        ip_addr = sock.getsockname()[0]
    except ValueError:
        ip_addr = '127.0.0.1'
    finally:
        sock.close()

    port = None
    if os.getenv("GLOO_PORT_RANGE") is not None:
        min_port, max_port = map(int, os.getenv("GLOO_PORT_RANGE").split(":"))
        for i in range(min_port, max_port + 1):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.bind(("", i))
                sock.listen(1)
                port = i
                sock.close()
                break
            except socket.error:
                pass
    if port is None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("", 0))
        sock.listen(1)
        port = sock.getsockname()[1]
        sock.close()

    return ip_addr + ':' + str(port)
```

## Configure <strong>GLOO_PORT_RANGE</strong> env variable as such: "100:1000"
## Now the gloo and DGL will only use this port range for communication
