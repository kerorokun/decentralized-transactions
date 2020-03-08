import os
import matplotlib.pyplot as plt

def generate_graphs():
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    bw_file = os.path.join(curr_dir, 'bandwidths.txt')
    
    bandwidths = []
    with open(bw_file, 'r') as fp:
        for line in fp:
            bandwidths.append(int(line))
                
    plt.plot(bandwidths, label='Bandwidth (bytes)')
    plt.xlabel('Time (s)')
    plt.ylabel('Bandwidth (bytes)')
    plt.title('Bandwidth Per Second')
    plt.savefig('bandwidth.png', dpi=600, bbox_inches='tight')
    plt.clf()


if __name__ == '__main__':
    generate_graphs()
