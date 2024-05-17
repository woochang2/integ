import { HardhatUserConfig } from "hardhat/config";
import "@nomicfoundation/hardhat-toolbox";

const config: HardhatUserConfig = {
  networks: {
  localhost: {
    url: "http://localhost:40000",     // Localhost (default: none)
    chainId: 423423,            // Standard Ethereum port (default: none)
  },
  },
  solidity: "0.8.24",
};

export default config;
