# What is PIAX?

PIAX is a middleware for P2P distributed computing.
By using PIAX core transport mechanism, you can write programs that utilize discovery features of overlays.
PIAX also provides a mobile agent-based programming framework. You can write distributed programs that interact each other by using remote procedure calls among mobile agents.

# Structure of the software

The structure of PIAX is as follows:

* 'piax-gtrans' - Core modules of GTrans.
* 'piax-agent' - PIAX Agent modules.
* 'piax-dht' - A DHT implementation on PIAX.
* 'piax-gtrans-dtn' - Modules related to DTN, AdHoc and Bluetooth (experimental).
* 'piax-samples' - Some simple samples of GTrans and PIAX Agents.
* 'piax-shell' - A command-line application to demonstrate PIAX Agents.

# Build from sources

cd into cloned source directory and `mvn -Dmaven.test.skip=true clean package`. 
Of course you can do tests by eliminating `-Dmaven.test.skip=true` (it takes a while).
