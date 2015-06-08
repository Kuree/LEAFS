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
    for file in os.listdir("."):
        if re.search(filename_pattern, file) is not None:
            with open(file, 'r') as f:
                data_points = []
                lines = f.read().split("\n")
                for line in lines:
                    if len(line) == 0:
                        continue
                    else:
                        data_points.append(float(line))
            key_name = "-".join(file.split("-")[:-1])
            if key_name in data_points_dict:
                old_list = data_points_dict[key_name] + data_points
                data_points_dict[key_name] = old_list
            else:
                data_points_dict[key_name] = data_points
    data_count = len(data_points_dict)
    fig, axs = plt.subplots(data_count,2)
    count = 0
    for key in data_points_dict:
        ax0 = axs[count][0]
        if count == data_count - 1: ax0.set_xlabel("Latency (s)")
        args = key.split("-")
        machine_count = int(args[0])
        process_count = int(args[1])
        thread_count = int(args[2])
        frequency = int(args[3])
        connection_count = machine_count * process_count * thread_count 
        ax0.set_title("Connection: {0:d} Frequency: {1:d} Hz".format(connection_count, frequency))
        ax1 = axs[count][1]
        count += 1
        data_points = data_points_dict[key]
        weights = np.ones_like(data_points) / float(len(data_points))
        ax0.hist(data_points, histtype='bar', bins=100, weights=weights)
        ax1.boxplot(data_points)

    plt.show()
    fig.savefig("benchmark.pdf", format = "pdf")
