# 1. Create a connection to the other nodes
# 2. Start listening on the stdin for input
# 3. Broadcast the messages according (according to ISIS algorithm)
# 4. Handle transaction messages accordingly
from collections import defaultdict
from queue import Queue
import threading
import socket
import sys
import time

num_nodes_in_system = int(sys.argv[1])
port = int(sys.argv[2])

class Node:

    def __init__(self):
        self.accounts  = defaultdict(lambda: -1)
        self.in_conns  = []
        self.out_socks = []

        self.msg_queue = Queue()

        self.lock = threading.Lock()
        self.acc_lock = threading.Lock()
        
        self._make_server()
        self._start_server()

    ##################################
    ## Transaction handling
    ##################################
    def handle_transactions(self):
        while True:
            msg = self.msg_queue.get()

            if "DEPOSIT" in msg:
                # handle deposit
                params = msg.split()
                self.deposit(params[1], float(params[2]))
            else:
                # handle transfer
                params = msg.split()
                self.withdraw(params[1], params[3], float(params[4]))

    def deposit(self, account, amt):
        self.acc_lock.acquire()
        if self.accounts[account] < 0:
            self.accounts[account] = 0
        self.accounts[account] += amt
        self.acc_lock.release()
        
    def withdraw(self, sender, recipient, amt):
        self.acc_lock.acquire()
        if self.accounts[sender] >= amt:
            self.accounts[sender] -= amt

            if self.accounts[recipient] < 0:
                self.accounts[recipient] = 0
            self.accounts[recipient] += amt
        self.acc_lock.release()
            
    def output_accounts(self):
        while True:
            self.acc_lock.acquire()
            balances = " ".join([ f"{acc}:{amt}" for acc, amt in self.accounts.items() if amt > 0 ])
            self.acc_lock.release()
        
            print(f"BALANCES {balances}")
            time.sleep(5)
            
    ##################################
    ## Multicasting
    #################################
    def multicast(self, msg):
        # Naive implementation for now
        self.deliver(msg)
        
        for out in self.out_socks:
            out.sendall(str.encode(msg))

    def deliver(self, msg):
        self.msg_queue.put(msg)

    def __handle_peer(self, conn, addr):
        # Naive implementation for now
        while True:
            data = conn.recv(1024)
            if not data:
                break

            msg = data.decode('utf-8')
            self.deliver(msg)
    
    #####################################
    ## Server connection
    #####################################
    def _make_server(self):
        # Create server and start listening
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        host = socket.gethostname()
        
        self.sock.bind((host, port))
        self.sock.listen()

    def __listen_for_connections(self):
        while True:
            conn, address = self.sock.accept()
            peer_thread = threading.Thread(target=self.__handle_peer, args=(conn, address))
            self.in_conns.append(peer_thread)
            peer_thread.start()
        
    def _start_server(self):
        # Connect to the others (note: hardcoded so potentially dangerous)
        valid_addresses = [ f'sp20-cs425-g36-0{x}.cs.illinois.edu' for x in range(1, num_nodes_in_system+1) ]
        valid_addresses = [ x for x in valid_addresses if x != socket.gethostname() ]
        print(valid_addresses)
        for addr in valid_addresses:
            threading.Thread(target=self.__connect_to_node, args=((addr), )).start()
        
        conn_thread = threading.Thread(target=self.__listen_for_connections, daemon=True)
        conn_thread.start()

        # Connect to all other nodes
        while len(self.out_socks) < len(valid_addresses):
            pass

        # Wait for a few seconds
        time.sleep(2)
        
        transaction_thread = threading.Thread(target=self.handle_transactions, daemon=True)
        transaction_thread.start()

        status_thread = threading.Thread(target=self.output_accounts, daemon=True)
        status_thread.start()
        
        # Start listening on stdin
        while True:
            for line in sys.stdin:
                self.multicast(line)


        
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
        

if __name__ == '__main__':
    node = Node()
