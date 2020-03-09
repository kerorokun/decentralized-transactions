import sys
import os
import matplotlib.pyplot as plt
import paramiko
import socket
from collections import defaultdict

num_nodes_in_system = int(sys.argv[1])
valid_addresses = [ f'sp20-cs425-g36-0{x}.cs.illinois.edu' for x in range(1, num_nodes_in_system+1) ]
valid_addresses = [ x for x in valid_addresses if x != socket.gethostname() ]

USERNAME = sys.argv[2]
PASSWORD = sys.argv[3]

def generate_remote_node_bandwidths():
    print('------Generating bandwidths-----')
    for addr in valid_addresses:
        print(f'Generating bandwidths for {addr}')
        
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=addr,
                           username=USERNAME,
                           password=PASSWORD)
        sftp_client = ssh_client.open_sftp()
        sftp_client.chdir('decentralized-transactions')
        remote_file = sftp_client.open('bandwidths.txt')

        bandwidths = []
        for line in remote_file:
            bandwidths.append(int(line))
            
        sftp_client.close()
        ssh_client.close()
        
        plt.plot(bandwidths, label='Bandwidth (bytes)')
        plt.xlabel('Time (s)')
        plt.ylabel('Bandwidth (bytes)')
        plt.title(f'Bandwidth Per Second For {addr}')
        plt.savefig(f'bandwidth-{addr}.png', dpi=600, bbox_inches='tight')
        plt.clf()
    print('------Generated bandwidths-----\n')

def generate_message_times():
    print('------Generating message times-----')
    
    max_dict = defaultdict(int)
    min_dict = defaultdict(lambda: float("inf"))

    curr_dir = os.path.dirname(os.path.abspath(__file__))
    times_file = os.path.join(curr_dir, 'times.txt')
    with open(times_file, 'r') as fp:
        for line in fp:
            id, send_time = line.split()
            max_dict[id] = max(max_dict[id], float(send_time))
            min_dict[id] = min(min_dict[id], float(send_time))
    
    for addr in valid_addresses:
        print(f'Gathering times from {addr}')
        
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=addr,
                           username=USERNAME,
                           password=PASSWORD)
        sftp_client = ssh_client.open_sftp()
        sftp_client.chdir('decentralized-transactions')
        remote_file = sftp_client.open('times.txt')

        for line in remote_file:
            id, send_time = line.split()
            max_dict[id] = max(max_dict[id], float(send_time))
            min_dict[id] = min(min_dict[id], float(send_time))
            
        sftp_client.close()
        ssh_client.close()

    mins = []
    maxes = []
    for id in max_dict:
        mins.append(min_dict[id])
        maxes.append(max_dict[id])

    plt.plot(mins, label='Min time')
    plt.plot(maxes, label='Max time')
    plt.xlabel('Msg')
    plt.ylabel('Time (s)')
    plt.title('Message Processing Time')
    plt.savefig('msg_send_time.png', dpi=600, bbox_inches='tight')
    plt.clf()
    
    print('------Generated message times-----\n')


def generate_local_bandwidth():
    print('------Generating local bandwidth-----')
    print(f'Generating for {socket.gethostname()}')
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    bw_file = os.path.join(curr_dir, 'bandwidths.txt')
    
    bandwidths = []
    with open(bw_file, 'r') as fp:
        for line in fp:
            bandwidths.append(int(line))
                
    plt.plot(bandwidths, label='Bandwidth (bytes)')
    plt.xlabel('Time (s)')
    plt.ylabel('Bandwidth (bytes)')
    plt.title(f'Bandwidth Per Second For {socket.gethostname()}')
    plt.savefig(f'bandwidth-{socket.gethostname()}.png', dpi=600, bbox_inches='tight')
    plt.clf()
    print('------Generated local bandwidth-----\n')

        
if __name__ == '__main__':
    generate_local_bandwidth()
    generate_remote_node_bandwidths()
    generate_message_times()
