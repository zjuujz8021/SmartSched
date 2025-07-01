import matplotlib.pyplot as plt
import pandas as pd
from collections import defaultdict
from utils import *
from matplotlib.ticker import MaxNLocator

schemes = comparable_schemes
shown_schemes_set = comparable_schemes_set

columns = ["block", "txs"]
columns.extend(schemes)
log_prefix = "./results/logs/history_"
x_labels = {"threads": "Number of threads", "accounts": "Number of accounts", "block_size": "Block size"}

plt.rc('font', family="Times New Roman")
plt.rcParams['figure.figsize'] = (8.0, 3.5)
plt.rcParams['figure.dpi'] = 300
plt.rcParams['image.cmap'] = 'gray'

def calculate_history_tps_speedups(df):
    exec_costs = defaultdict(float)
    txs = 0
    for i in range(len(df)):
        txs += df.iloc[i]["txs"]
        for scheme in schemes:
            exec_costs[scheme] += df.iloc[i][scheme]
    tps = defaultdict(float)
    speedups = defaultdict(float)
    for scheme in schemes:
            speedups[scheme] = exec_costs["serial"] / exec_costs[scheme]
            tps[scheme] = txs/exec_costs[scheme]*1000
    return tps, speedups

def threads():
    print("##################### plot threads #####################")
    threads = [4, 8, 12, 16, 20, 24, 28, 32]
    cursors = {} # name => ((threads), (speedups))
    
    for thread in threads:
        df = pd.read_csv(f"{log_prefix}{thread}.csv", names=columns)
        tps, speedups = calculate_history_tps_speedups(df)
        print(thread)
        print(tps)
        print(speedups)
        for scheme in speedups:
            if scheme not in shown_schemes_set:
                continue
            if scheme not in cursors:
                cursors[scheme] = ([], [])
            cursors[scheme][0].append(thread)
            cursors[scheme][1].append(tps[scheme])
    plt.figure()
    ax = plt.subplot(1,1,1)
    ax.set_title("History",fontsize=18)
    for scheme in cursors:
        ax.plot(cursors[scheme][0], cursors[scheme][1], label=legends[scheme], linewidth=3, marker=markers[scheme], color=colors[scheme], markersize=7)
    ax.set_xlabel(x_labels["threads"], fontsize=18, labelpad=2)
    ax.set_ylabel("Throughput (TPS)", fontsize=18, labelpad=10)
    # ax.set_yticks([1, 2, 3, 4, 5])
    ax.xaxis.set_tick_params(labelsize=18)
    ax.yaxis.set_tick_params(labelsize=18)
    ax.ticklabel_format(style='sci', scilimits=(3,3), axis='y', useMathText=True)
    ax.yaxis.get_offset_text().set(size=18)
    ax.yaxis.set_major_locator(MaxNLocator(6))
    ax.legend(fontsize=12, columnspacing=0.9, labelspacing=0.1, framealpha=0.4)
    plt.savefig(f"./results/threads_history.pdf", bbox_inches="tight")
    plt.cla()

def blocksize():
    print("##################### plot block size #####################")
    interval = 100
    df = pd.read_csv(f"{log_prefix}32.csv", names=columns)
    totalTxs = defaultdict(float)
    exec_costs = {}
    speedups = {}
    tps = {}
    binCnt = defaultdict(int)
    for i in range(len(df)):
        txs = df.iloc[i]["txs"]
        if txs >= 1000:
            continue
        bin = int(txs) // interval
        binCnt[bin] += 1
        totalTxs[bin] += df.iloc[i]["txs"]
        if bin not in exec_costs:
            exec_costs[bin] = defaultdict(float)
            speedups[bin] = defaultdict(float)
            tps[bin] = defaultdict(float)
        for scheme in schemes:
            exec_costs[bin][scheme] += df.iloc[i][scheme]

    for bin in exec_costs:
        if binCnt[bin] < 10:
            continue
        for scheme in schemes:
            speedups[bin][scheme] = exec_costs[bin]["serial"] / exec_costs[bin][scheme]
            tps[bin][scheme] = totalTxs[bin]/exec_costs[bin][scheme]*1000

    tps = sorted(tps.items(), key=lambda x: x[0])
    cursors = {} # name => ((block_size), (speedups))
    for bin in tps:
        x_value = (bin[0] + 1) * interval
        print(x_value)
        print(bin[1])
        print(speedups[bin[0]])
        for scheme in bin[1]:
            if scheme not in shown_schemes_set:
                continue
            if scheme not in cursors:
                cursors[scheme] = ([], [])
            cursors[scheme][0].append(x_value)
            cursors[scheme][1].append(bin[1][scheme])
    plt.figure()
    ax = plt.subplot(1,1,1)     
    ax.set_title("History",fontsize=18)
    for scheme in cursors:
        ax.plot(cursors[scheme][0], cursors[scheme][1], label=legends[scheme], linewidth=3, marker=markers[scheme], color=colors[scheme], markersize=7)
    ax.set_xlabel(x_labels["block_size"], fontsize=18, labelpad=2)
    ax.set_ylabel("Throughput (TPS)", fontsize=18, labelpad=10)
    ax.ticklabel_format(style='sci', scilimits=(3,3), axis='y', useMathText=True)
    ax.yaxis.get_offset_text().set(size=18)
    ax.yaxis.set_major_locator(MaxNLocator(6))
    # ax.set_yticks([1, 2, 3, 4, 5, 6, 7])
    ax.xaxis.set_tick_params(labelsize=18)
    ax.yaxis.set_tick_params(labelsize=18)
    ax.legend(fontsize=12, columnspacing=0.9, labelspacing=0.1)
    plt.savefig(f"./results/blocksize_history.pdf", bbox_inches="tight")
    plt.cla()

def cdf():
    print("##################### plot cdf #####################")
    interval = 0.1
    df = pd.read_csv(f"{log_prefix}32.csv", names=columns)
    speedups = {}
    for i in range(len(df)):
        for scheme in schemes:
            if scheme == "serial":
                continue
            if scheme not in shown_schemes_set:
                continue
            if scheme not in speedups:
                speedups[scheme] = []
            speedups[scheme].append(df.iloc[i]["serial"] / df.iloc[i][scheme])

    plt.figure()
    ax = plt.subplot(1,1,1)     
    # ax.set_title("History",fontsize=18)
    cursors = {} # name => ((speedup), (cdf))
    for scheme in speedups:
        cursors[scheme] = ([], [])
        speedups[scheme].sort()
        total = len(speedups[scheme])
        lastX = 0.0
        for i in range(total):
            if speedups[scheme][i] - lastX < interval:
                continue
            if i < total - 1 and speedups[scheme][i] == speedups[scheme][i+1]:
                continue
            lastX = speedups[scheme][i]
            cdf = (i + 1) / total
            if cdf >= 0.99:
                break
            cursors[scheme][0].append(lastX)
            cursors[scheme][1].append(cdf)
        print(scheme)
        print_cursor(cursors[scheme][0], cursors[scheme][1])
        ax.plot(cursors[scheme][0], cursors[scheme][1], linewidth=3, color=colors[scheme])
        ax.plot(cursors[scheme][0][-1], cursors[scheme][1][-1], linewidth=3, label=legends[scheme], marker=markers[scheme], color=colors[scheme], markersize=7)
    ax.set_xlabel("Speedup", fontsize=18, labelpad=2)
    ax.set_ylabel("CDF", fontsize=18, labelpad=-1)
    # ax.set_yticks([1, 2, 3, 4, 5, 6, 7])
    ax.xaxis.set_tick_params(labelsize=18)
    ax.yaxis.set_tick_params(labelsize=18)
    ax.legend(fontsize=12, columnspacing=0.9, labelspacing=0.1)
    plt.savefig(f"./results/cdf_history.pdf", bbox_inches="tight")
    plt.cla()

def effectiveness():
    print("plot effectiveness")
    threads = [8, 16, 24, 32]
    data = {scheme: [] for scheme in sched_schemes}
    for thread in threads:
        df = pd.read_csv(f"{log_prefix}{thread}.csv", names=columns)
        tps, speedups = calculate_history_tps_speedups(df)
        for scheme in data:
            data[scheme].append(speedups[scheme])
    plot_effectiveness("history", data, x_labels["threads"], threads)

def print_cursor(x, y):
    for i in range(len(x)):
        print(f"{round(x[i], 4)}:{round(y[i], 4)}", end=", ")
    print()
    print()

if __name__ == "__main__":
    threads()
    blocksize()
    cdf()
    # effectiveness()