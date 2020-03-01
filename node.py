# 1. Create a connection to the other nodes
# 2. Start listening on the stdin for input
# 3. Broadcast the messages according (according to ISIS algorithm)
# 4. Handle transaction messages accordingly
from collections import defaultdict
import threading
import socket
import sys

num_nodes_in_system = int(sys.argv[1])
port = int(sys.argv[2])

class Node:

    def __init__(self):
        self.accounts  = defaultdict(int)
        self.in_conns  = []
        self.out_socks = []

        
        self.lock = threading.Lock()
        self._make_server()
        self._start_server()

    def _make_server(self):
        # Create server and start listening
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        host = socket.gethostname()
        
        self.sock.bind((host, port))
        self.sock.listen()
        
    def _start_server(self):
        # Connect to the others (note: potentially dangerous)
        valid_addresses = [ f'sp20-cs425-g36-0{x}.cs.illinois.edu' for x in range(num_nodes_in_system) ]
        
        for addr in valid_addresses:
            threading.Thread(target=self.__connect_to_node, args=((addr), )).start()

        while len(self.out_socks) < num_nodes_in_system:
            conn, address = s.accept()
            peer_thread = threading.Thread(target=__handle_peer)
            self.in_conns.append(peer_thread)
            peer_thread.start()


        # Wait for a few seconds
        for conn in self.out_cons:
            print(conn)
        
        # Start listening on stdin
        pass
        
    def __connect_to_node(self, addr):
        sock = socket.create_connection((addr, host))

        self.lock.acquire()
        self.out_socks.append(sock)
        self.lock.release()
        
    def _connect_to_others(self):
        # Problem: the other nodes won't have started just yet
        # Loop until connected to enough of the
        pass
        
