import sys
import fnmatch
import os
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as stats


if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Invalid argument")
        print("Usage: machine_count process_count thread_count frequency level")
        exit(1)

    data_points = []

    for file in os.listdir("."):
        filename = "{0}-{1}-{2}-{3}-{4}-*".format(*sys.argv[1:])
        if fnmatch.fnmatch(file, filename):
            with open(file, 'r') as f:
                lines = f.read().split("\n")
                for line in lines:
                    if len(line) == 0:
                        continue
                    else:
                        data_points.append(float(line))
    data_points.sort()
    
    fig, axs = plt.subplots(1,2)
    ax, ax2 = axs
    hmean = np.mean(data_points)
    hstd = np.std(data_points)
    pdf = stats.norm.pdf(data_points, hmean, hstd)
    ax.plot(data_points, pdf)
    ax.hist(data_points, histtype='stepfilled', normed=True, bins=200, alpha = 0.5)
    ax2.boxplot(data_points)
    plt.show()
