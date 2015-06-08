import sys
import re
import os
import numpy as np
import matplotlib.pyplot as plt
#import scipy.stats as stats


if __name__ == "__main__":
    #if len(sys.argv) != 6:
    #    print("Invalid argument")
    #    print("Usage: machine_count process_count thread_count frequency level")
    #    exit(1)
    data_points_dict = {}
    
    filename_pattern = r'\d+-\d+-\d+-\d+-\w+'
    for filename in os.listdir("."):
        if re.search(filename_pattern, filename) is not None:
            with open(filename, 'r') as f:
                data_points = []
                lines = f.read().split("\n")
                for line in lines:
                    if len(line) == 0:
                        continue
                    else:
                        data_points.append(float(line))
            args = filename.split("-")
            machine_count = int(args[0])
            process_count = int(args[1])
            thread_count = int(args[2])
            frequency = int(args[3])
            connection_count = machine_count * process_count * thread_count
            key_name = (connection_count, frequency)
            if key_name in data_points_dict:
                old_list = data_points_dict[key_name] + data_points
                data_points_dict[key_name] = old_list
            else:
                data_points_dict[key_name] = data_points
    data_count = len(data_points_dict)
    fig, axs = plt.subplots(data_count // 2,2)
    count = 0
    sorted_key_list = sorted(data_points_dict, key=lambda x: (x[0], x[1]))
    for key in sorted_key_list:
        ax = axs[count // 2][count % 2]
        if count >= data_count - 2: ax.set_xlabel("Latency (s)")
        connection_count, frequency = key
        ax.set_title("Connection: {0:d} Frequency: {1:d} Hz".format(connection_count, frequency))
        count += 1
        data_points = data_points_dict[key]
        weights = np.ones_like(data_points) / float(len(data_points))
        ax.hist(data_points, histtype='bar', bins=100, weights=weights)

    plt.show()
    #fig.savefig("benchmark.pdf", format = "pdf")
    
    # filter the unnecessary list
    #for key in data_points_list:
    #    data_points = data_points_list[key]
    #    data_points_list[key] = [x for x in data_points if x < 15]
    fig2, ax = plt.subplots(1, 1)
    box_data_points = [[x for x in data_points_dict[key] if x < 15] for key in sorted_key_list]
    ax.boxplot(box_data_points)
    xtickNames = plt.setp(ax, xticklabels=["Connection: " + str(key[0]) + " " + str(key[1]) + " Hz" for key in sorted_key_list])
    plt.setp(xtickNames, rotation=0)
    plt.show()
