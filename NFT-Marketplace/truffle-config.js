const HDWalletProvider = require("@truffle/hdwallet-provider");

const privateKey = "436f56d07ec9b5aac5f76e47f53ac40fe3187141bf8d93ac6eda36176c82a187"

module.exports = {
  contracts_build_directory: './client/src/contracts',

  networks: {
    auditchain: {
      host: "localhost",
      port: 40000,
      provider: () => new HDWalletProvider(privateKey, "http://localhost:40000"),
      from: "0x11Ce04fB4A94727987D4F5750E271e63A84F8418",
      network_id: "*",
      gas: 100000000,
      gasPrice: 1,
    },
  },

  compilers: {
    solc: {
      version: "0.8.19",
    }
  },

  mocha: {
    timeout: 100000
  }
};
