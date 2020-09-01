# decentralized-transactions
This is a short project to implement a decentralized distributed system that handles transactions between users and maintains a total ordering among all processes. In this project, the following concepts were implemented:
* Reliable multicast
* Total ordering using the ISIS algorithm 
* Message timeouts to handle node failure

## To run
To run: `python3 -u generator.py <frequency> | python3 node.py <number of nodes> <port>`
