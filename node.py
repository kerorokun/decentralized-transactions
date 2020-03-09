from collections import defaultdict
from queue import Queue
import threading
import socket
import sys
import time
import heapq
import uuid
import struct
import os

num_nodes_in_system = int(sys.argv[1])
port = int(sys.argv[2])

class Node:

    def __init__(self):
        self.MSG_THRESHOLD = 2
        self.accounts  = defaultdict(lambda: -1)

        self.isis_queue = []
        
        self.proposed_times = defaultdict(lambda: -1)
        self.num_response = 0
        
        self.in_conns  = []
        self.out_socks = []
        self.out_socks_map = {}
        self.sequence_num = 0

        self.received_messages = defaultdict(lambda: False)

        self.msg_queue = Queue()
        self.bandwidth_queue = Queue()
        self.msg_time_queue = Queue()

        self.process_failed = False

        self.lock          = threading.Lock()
        self.acc_lock      = threading.Lock()
        self.seq_lock      = threading.Lock()
        self.proposed_lock = threading.Lock()
        self.TO_lock       = threading.Lock()
        self.msg_lock      = threading.Lock()
        self.fail_lock     = threading.Lock()
        
        self._make_server()
        self._start_server()
        self._main_loop()

    def _main_loop(self):
        transaction_thread = threading.Thread(target=self.handle_transactions, daemon=True)
        transaction_thread.start()

        status_thread = threading.Thread(target=self.output_accounts, daemon=True)
        status_thread.start()

        bandwidth_thread = threading.Thread(target=self.calculate_bandwidth, daemon=True)
        bandwidth_thread.start()

        msg_time_thread = threading.Thread(target=self.calculate_msg_times, daemon=True)
        msg_time_thread.start()
        
        # Start listening on stdin
        try:
            while True:
                # flush_time = time.time() + 2.0
                # for _ in sys.stdin:
                #     if time.time() > flush_time:
                #         break
                    
                for line in sys.stdin:
                    self.multicast_TO(line)
        except KeyboardInterrupt:
            pass

    ##################################
    ## Transaction handling
    ##################################
    def handle_transactions(self):
        while True:
            msg = self.msg_queue.get()

            print (msg, file=sys.stderr)
            
            msg = msg.lower()

            if 'deposit' in msg:
                # handle deposit
                params = msg.split()
                self.deposit(params[1], float(params[2]))
            elif 'transfer' in msg:
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
            balances = sorted(self.accounts.items(), key=lambda t: t[0])
            balances = ' '.join([ f'{acc}:{amt}' for (acc, amt) in balances if amt >= 0 ])
            self.acc_lock.release()
        
            print(f'BALANCES {balances}')
            time.sleep(5)

    ##################################
    ## Multicasting
    #################################

    def generate_unique_id(self):
        return f'{socket.gethostname()}-{time.time()}'
    
    def multicast_TO(self, msg):
        # multicast to everyone
        self.num_response = 0
        id = self.generate_unique_id()#uuid.uuid4()

        self.multicast(f'ISIS-TO-INIT {id} {time.time()} {msg}')
        
        # wait to hear back from everyone
        while self.num_response < len(self.in_conns):
            pass
        
        # decide on final time
        self.proposed_lock.acquire()
        final_time = -1
        for k, v in self.proposed_times.items():
            final_time = max(final_time, v)
        self.proposed_lock.release()
        
        # tell everyone else
        self.multicast(f'ISIS-TO-FINAL {final_time} {id}')


    def deliver_TO(self, addr, msg):
        msg = msg.lower()
        if 'isis-to-final' in msg:

            _, final_time, id = msg.split()

            self.TO_lock.acquire()
            self.isis_queue.sort(key=lambda x: x[0])

            curr_time = time.time()
            self.isis_queue = [m for m in self.isis_queue if curr_time - m[1] < self.MSG_THRESHOLD]
            
            i = 0
            for i, queued_msg in enumerate(self.isis_queue):
                seq_time, start_time, content, msg_id, deliverable = queued_msg

                if id == msg_id:
                    deliverable = True
                    self.isis_queue[i] = (seq_time, start_time, content, msg_id, True)

                if not deliverable:
                    break

                self.msg_time_queue.put((id, f'{time.time() - start_time}'))
                self.deliver(content)
                
            if i + 1 >= len(self.isis_queue):
                self.isis_queue = []
            else:
                self.isis_queue = self.isis_queue[i+1:]
            self.TO_lock.release()

        elif 'init' in msg:
            _, id, start_time, content = msg.split(' ', 3)
            #id = split[1]
            #content = ' '.join(split[2:])

            self.TO_lock.acquire()
            self.isis_queue.append((self.sequence_num, float(start_time), content, id, False))
            self.TO_lock.release()

            self.seq_lock.acquire()
            if addr != socket.gethostname():
                self.r_unicast(self.out_socks_map[addr],
                               f'ISIS-TO-PROPOSE {self.sequence_num}')
            self.sequence_num += 1
            self.seq_lock.release()
                
        elif 'propose' in msg:
            self.proposed_lock.acquire()
            self.proposed_times[addr] = int(msg.split()[1])
            self.num_response += 1
            self.proposed_lock.release()
    
    def multicast(self, msg):
        self.r_multicast(msg)

    def r_multicast(self, msg):
        id = self.generate_unique_id()
        msg = f'{id} {msg}'
        self.r_deliver((socket.gethostname(), ''), msg)
        self.b_multicast(msg)

    def b_multicast(self, msg):
        for out in self.out_socks:
            self.unicast(out, msg) #out.sendall(str.encode(msg))

    def r_unicast(self, sock, msg):
        id = self.generate_unique_id()
        msg = f'{id} {msg}'
        self.unicast(sock, msg)
            
    def unicast(self, sock, msg):
        msg = str.encode(msg)
        sock.send(struct.pack('i', len(msg)) + msg)
            
    def deliver(self, msg):
        self.msg_queue.put(msg)

    def b_deliver(self, addr, msg):
        self.r_deliver(addr, msg)

    def r_deliver(self, addr, msg):
        # Check to see if message has been recieved
        msg_id, content = msg.split(' ', 1)
        new_message = False
        
        self.msg_lock.acquire()

        if not self.received_messages[msg_id]: 
            new_message = True
            self.received_messages[msg_id] = True
        self.msg_lock.release()

        if not new_message:
            return
        
        # Handle the message
        #if 'ISIS-TO' in content:
        self.b_multicast(msg)

        self.deliver_TO(addr[0], content)
        #else:
        #    self.deliver(content)

    def __handle_peer(self, conn, addr):
        # Naive implementation for now
        while True:
            try:
                msg_size = struct.unpack('i', conn.recv(struct.calcsize('i')))[0]
                msg = ''
                #msg = conn.recv(1024)
                #if not msg:
                #    break
                while len(msg) < msg_size:
                    submsg = conn.recv(msg_size - len(msg))
                    if not submsg:
                        break
                    msg += submsg.decode('utf-8')

                self.bandwidth_queue.put((time.time(), msg_size))
                self.b_deliver(addr, msg)
            except OSError as e:
                print('Caught failure', file=sys.stderr)
                return False
            except struct.error as e:
                print('Caught failure', file=sys.stderr)

                self.in_conns.remove(conn)
                self.out_socks.remove(self.out_socks_map[addr[0]])
                self.out_socks_map[addr[0]] = None
                
                return False
    
    #####################################
    ## Server connection
    #####################################
    def _make_server(self):
        # Create server and start listening
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        host = socket.gethostname()
        
        self.sock.bind((host, port))
        self.sock.listen()

    def __listen_for_connections(self):
        while True:
            conn, address = self.sock.accept()
            peer_thread = threading.Thread(target=self.__handle_peer, args=(conn, address), daemon=True)
            self.in_conns.append(conn)
            peer_thread.start()
            
    def __connect_to_node(self, addr):
        connected = False
        addr = socket.gethostbyname(addr)
        
        while not connected:
            try:
                sock = socket.create_connection((addr, port))
                connected = True
            except Exception:
                connected = False

        self.lock.acquire()
        self.out_socks.append(sock)
        self.out_socks_map[addr] = sock
        self.lock.release()
        
    def _start_server(self):
        # Connect to the others (note: hardcoded so potentially dangerous)
        valid_addresses = [ f'sp20-cs425-g36-0{x}.cs.illinois.edu' for x in range(1, num_nodes_in_system+1) ]
        valid_addresses = [ x for x in valid_addresses if x != socket.gethostname() ]
        print(f'Connecting to {valid_addresses}', file=sys.stderr)
        for addr in valid_addresses:
            threading.Thread(target=self.__connect_to_node, args=((addr), )).start()
        
        conn_thread = threading.Thread(target=self.__listen_for_connections, daemon=True)
        conn_thread.start()

        # Connect to all other nodes
        while len(self.out_socks) < len(valid_addresses):
            pass

        # Wait for a few seconds
        time.sleep(1)

    ########################################
    ### Metric collection
    #######################################
    def calculate_bandwidth(self):
        curr_dir = os.path.dirname(os.path.abspath(__file__))
        log_file = os.path.join(curr_dir, 'bandwidths.txt')
        curr_time = -10
        leftover_event = None

        with open(log_file, 'w') as fp:
            while True:
                curr_time += 1

                total_size = 0

                # Check if leftover event is applicable
                if leftover_event and curr_time > 0:
                    receive_time, size = leftover_event
                    if receive_time > curr_time + 1:
                        fp.write('0\n')
                    else:
                        total_size += size
                    leftover_event = None

                try:
                    while True:
                        receive_time, size = self.bandwidth_queue.get()

                        if curr_time <= 0:
                            curr_time = receive_time

                        if receive_time > curr_time + 1:
                            leftover_event = (receive_time, size)
                            break
                        total_size += size

                    fp.write(f'{total_size}\n')
                    fp.flush()
                except Exception as e:
                    pass

    def calculate_msg_times(self):
        curr_dir = os.path.dirname(os.path.abspath(__file__))
        time_file = os.path.join(curr_dir, 'times.txt')

        with open(time_file, 'w') as fp:

            while True:
                id, delivery_time = self.msg_time_queue.get()

                fp.write(f'{id} {delivery_time}\n')
                fp.flush()
            
        
if __name__ == '__main__':
    node = Node()
