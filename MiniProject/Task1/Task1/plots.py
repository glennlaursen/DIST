import matplotlib.pyplot as plt
import numpy as np

# Open file
f = open("../results/raid1_getfile_4k_100kB.csv", "r")
file = f.readlines()
f.close()

# Create lists for data
elements = []

# Split file into lists

for line in file:
    #print(line)
    number = float(line.split(',')[-1].strip('\n'))
    elements.append(number)


# Create np arrays
elements_np = np.array(elements)

print(len(elements_np))


# Get data in ms
#x = elements_np*1000

x = elements_np


# Get 5% and 9% percentiles
q5, q95 = np.percentile(x, [5, 95])

# Remove outliers outside percentiles
x_clipped = x[(x < q95) & (x > q5)]

# Get average and median
avg = np.average(x_clipped)
median = np.median(x_clipped)

# Something i found online to determine number of bins to use in histogram
# I don't use it, since the result doesn't make sense to use
q25, q75 = np.percentile(x_clipped, [25, 75])
bin_width = 2 * (q75 - q25) * len(x_clipped) ** (-1/3)
bins = round((x_clipped.max() - x_clipped.min()) / bin_width)
print("Freedman–Diaconis number of bins:", bins)

# Create histogram
plt.hist(x_clipped, density=False, bins=20)  # density=False would make counts

# Create lines for average and median
min_ylim, max_ylim = plt.ylim()
plt.axvline(avg, color='k', linestyle='dashed', linewidth=1)
plt.text(avg*1.005, max_ylim*0.9, 'Average: {:.4f}'.format(avg))
plt.axvline(median, color='k', linestyle='dashed', linewidth=1)
plt.text(median*1.005, max_ylim*0.8, 'Median: {:.4f}'.format(median))

# Insert labels etc.
plt.ylabel('Count')
plt.xlabel('Time (ms)')
plt.title("Raid1 Get File Time: file_size=100kB, k=4")
plt.savefig("./plots/Raid1 Get File Time file_size=100kB k=4")
plt.show()