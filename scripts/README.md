# Prerequisite?

please install `golang`, `cargo(rust)`, `nodejs`, `python>=3.10` 

### dependencies for NFT-markeplace
```bash
    # first install nvm
    nvm install 22
    node -v # v22.x.x
    npm install -g npm  # this may require sudo privilege

    # install truffle to deploy smart contracts to AuditChain
    npm install -g truffle # this may require sudo privilege
    
    # install node-packages for client
    cd client
    npm install

    # install node-packages for backend
    cd ../backend
    npm install
```


### dependencies for sui folder
```bash
sudo apt-get update
sudo apt-get -y upgrade
sudo apt-get -y autoremove

# The following dependencies prevent the linking error.
sudo apt-get -y install build-essential
sudo apt-get -y install cmake

# Install rust (non-interactive).
sudo apt-get -y install curl
curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env
rustup update
rustup default stable

# This is missing from the Rocksdb installer (needed for Rocksdb).
sudo apt-get install -y clang
sudo apt-get install pkg-config
sudo apt-get install libssl-dev

# Install protobuf.
sudo apt-get install -y protobuf-compiler
```

# How to run AuditChain for demo

### 1) run AuditChain validators


```bash
fab local-demo
```

```bash
tmux kill-server
```



### 2) run AuditChain gateway (geth client)
The `run_gateway` script (1) removes geth-produced data, (2) builds geth binary, and (3) run geth.  
Note that user transactions will be forwarded to auditchain via GRPC call.
You MUST set `VALIDATOR_URL` for the GRPC to forward user transactions correctly.  
  - `VALIDATOR_URL` can be one of the urls of listening servers on auditor worker's mempool (i.e., narwhal worker mempool)
  - Please, modify the `VALIDATOR_URL` correctly in the `env` file.  

```bash
 # geth run in console-mode
 # if some ports are already in use, modify corresponding ports in `eth-data/geth-config.toml`
 ./run_gateway 
```


### 3) deploy contracts
```bash
./migrate_contracts
```

### 4) run Client & Backend
```bash
./run_web_page  # if 3000 port is alrealy being used, the client process will not be started
```
```bash
./run_web_backend  # if 3333 port is alrealy being used, the client process will not be started
```

### 5) run Chrome browser & enter http://localhost:3000
You MUST install metamask in Chrome browser.  
You MUST create a new wallet with the secret key written in `NFT-Marketplace/truffle-config.js` because its address are set to have the maximum amount of balance in genesis.json.

# Trouble Shooting

### 1) connection b/w geth and validators
  geth connects to validtors with validator enode urls written into `eth-data/geth-config.toml`::BootStrapNode.  
  Please check carefully check `sui/crate/sslab-benchmark/logs/primary-*.log` to see each validator's enode url, and check it is correctly mapped into the BootStrapNode.

  Enode urls are already generated correctly. If you set `reuse_config=False` in `sui/crate/sslab-benchmark/fabfile.py::115`, enode urls are created and printed as a'boot_nodes.json' file in `sui/crate/sslab-benchmark`. In this case, you must modify `eth-data/geth-config.toml`::BootStrapNode accordingly.

### 2) check logs
  - geth log   
  : geth is running in console-mode so that logs are printed on the terminal in real-time
  - AuditChain validator log  
  : check `sui/crate/sslab-benchmark/logs` folder. Logs are stored in the folder. 