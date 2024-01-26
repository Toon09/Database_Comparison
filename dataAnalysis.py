# make graphs
# make folders for data with compressor alg and without
# run another piece of code (folder_name) instead of changing the code for MongoDB 

import matplotlib.pyplot as plt
import pandas as pd

# Paths to your CSV files
files = ['Mongo/data_zlib/insert.csv', 'Mongo/data_zstd/insert.csv', 'Arango/data/insert.csv']

fig, ax = plt.subplots()

for i, file in enumerate(files):
    # Load the data from the CSV file
    data = pd.read_csv(file, header=None)

    # Calculate the open, high, low, and close values
    open_val = data.iloc[0][0]
    high_val = data.max()[0]
    low_val = data.min()[0]
    close_val = data.iloc[-1][0]

    # Plot the candlestick
    ax.plot([i+1]*2, [low_val, high_val], color='black')  # The wick
    ax.plot([i+1], [open_val], marker='o', markersize=5, color='black')  # The open value
    ax.plot([i+1], [close_val], marker='o', markersize=5, color='red' if close_val < open_val else 'green')  # The close value

# Set the x-ticks to be the names of the experiments
ax.set_xticks(range(1, len(files) + 1))
ax.set_xticklabels(['Mongo zlib', 'Mongo zstd', 'Arango'])

plt.show()
