# 1. Create a connection to the other nodes
# 2. Start listening on the stdin for input
# 3. Broadcast the messages according (according to ISIS algorithm)
# 4. Handle transaction messages accordingly
from collections import defaultdict
import threading
import socket
import sys
import time

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

    def __handle_peer(self, conn, addr):
        print(addr)

        while True:
            data = conn.recv(1024)
            if not data:
                break

            msg = data.decode('utf-8')
            print(msg)

    def __listen_for_connections(self):
        while True:
            conn, address = self.sock.accept()
            peer_thread = threading.Thread(target=self.__handle_peer, args=(conn, address))
            self.in_conns.append(peer_thread)
            peer_thread.start()
        
    def _start_server(self):
        self._connect_to_others()
        
        conn_thread = threading.Thread(target=self.__listen_for_connections, daemon=True)
        conn_thread.start()

        # Connect to all other nodes
        while len(self.out_socks) < len(valid_addresses):
            pass

        # Wait for a few seconds
        time.sleep(2)

        # Start listening on stdin
        while True:
            for line in sys.stdin:
                self.multicast(line)

    def multicast(self, msg):
        # Naive implementation for now
        for out in self.out_socks:
            out.sendall(str.encode(msg))
        
    def __connect_to_node(self, addr):
        connected = False
        
        while not connected:
            try:
                sock = socket.create_connection((addr, port))
                connected = True
            except Exception:
                connected = False

        self.lock.acquire()
        self.out_socks.append(sock)
        self.lock.release()
        
    def _connect_to_others(self):
        # Connect to the others (note: hardcoded so potentially dangerous)
        valid_addresses = [ f'sp20-cs425-g36-0{x}.cs.illinois.edu' for x in range(1, num_nodes_in_system+1) ]
        valid_addresses = [ x for x in valid_addresses if x != socket.gethostname() ]
        print(valid_addresses)
        
        for addr in valid_addresses:
            threading.Thread(target=self.__connect_to_node, args=((addr), )).start()
        

if __name__ == '__main__':
    node = Node()
