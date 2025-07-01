import matplotlib.pyplot as plt
import pandas as pd
from collections import defaultdict
from utils import *
from matplotlib.ticker import MaxNLocator

schemes = all_schemes
shown_schemes_set = comparable_schemes_set

columns = ["name", "threads", "accounts", "blocksize", "cost", "speedup"]
x_labels = {"threads": "Number of threads", "accounts": "Number of accounts", "blocksize": "Block size"}


plt.rc('font', family="Times New Roman")
plt.rcParams['figure.figsize'] = (8.0, 3.5)
plt.rcParams['figure.dpi'] = 300
plt.rcParams['image.cmap'] = 'gray'

def draw_speedups(variable):
    print(f"---------- draw {variable} ----------")
    cursors = {} # name => ((threads), (speedups))
    df = pd.read_csv(f"./results/logs/erc20_{variable}_vs_speedups.csv", names=columns)
    for i in range(len(df)):
        scheme = df.iloc[i]["name"]
        x = df.iloc[i][variable]
        speedup = df.iloc[i]["speedup"]
        tps = df.iloc[i]["blocksize"]/df.iloc[i]["cost"]*1000
        if scheme not in cursors:
            cursors[scheme] = ([], [], [])
        cursors[scheme][0].append(x)
        cursors[scheme][1].append(tps)
        cursors[scheme][2].append(speedup)
    plt.figure()
    ax = plt.subplot(1,1,1)
    if variable != "accounts":
        ax.set_title("ERC-20",fontsize=18)

    for scheme in schemes:
        if scheme not in shown_schemes_set:
            continue
        if scheme not in cursors:
            continue
        print(scheme)
        print(cursors[scheme][0])
        print(cursors[scheme][1])
        print(cursors[scheme][2])
        ax.plot(cursors[scheme][0], cursors[scheme][1], label=legends[scheme], linewidth=3, marker=markers[scheme], color=colors[scheme], markersize=7)

    ax.set_xlabel(x_labels[variable], fontsize=18, labelpad=2)
    ax.set_ylabel("Throughput (TPS)", fontsize=18, labelpad=1)
    # ax.set_yticks([2, 4, 6, 8, 10, 12])
    ax.xaxis.set_tick_params(labelsize=18)
    ax.yaxis.set_tick_params(labelsize=18)
    ax.ticklabel_format(style='sci', scilimits=(3,3), axis='y', useMathText=True)
    ax.yaxis.get_offset_text().set(size=18)
    ax.yaxis.set_major_locator(MaxNLocator(6))
    if variable == "threads":
        ax.legend(fontsize=12, columnspacing=0.9, labelspacing=0.1)
    elif variable == "accounts":
        ax.legend(fontsize=12, ncol=2, loc='right', bbox_to_anchor=(1,0.3), columnspacing=0.9, labelspacing=0.1)
    else:
        # ax.legend(fontsize=12, ncol=2, loc='right', bbox_to_anchor=(1,0.67), columnspacing=0.9, labelspacing=0.1)
        ax.legend(fontsize=12, ncol=2, columnspacing=0.9, labelspacing=0.1, framealpha=0.4)
    plt.savefig(f"./results/{variable}_erc20.pdf", bbox_inches="tight")
    plt.cla()

def draw_effectiveness():
    print("draw effectiveness")
    data = {scheme: [] for scheme in sched_schemes}
    df = pd.read_csv(f"./results/logs/erc20_effectiveness.csv", names=columns)
    labels = []
    for i in range(len(df)):
        scheme = df.iloc[i]["name"]
        if scheme not in sched_schemes_set:
            continue
        if scheme == "serial":
            labels.append(f"{df.iloc[i]['threads']}-{df.iloc[i]['accounts']}")
        data[scheme].append(df.iloc[i]["speedup"])
    plot_effectiveness("erc20", data, "Threads-Accounts", labels)

def draw_erc20_effectiveness():
    print("draw effectiveness")
    threads = [8, 32]
    for thread in threads:
        data = {}
        for s in sched_schemes:
            if s != "serial":
                data[s] = []
        df = pd.read_csv(f"./results/logs/erc20_effectiveness_{thread}.csv", names=columns)
        labels = []
        for i in range(len(df)):
            scheme = df.iloc[i]["name"]
            if scheme not in sched_schemes_set:
                continue
            if scheme == "serial":
                labels.append(f"{df.iloc[i]['accounts']}")
                continue
            data[scheme].append(df.iloc[i]["speedup"])
        plot_erc20_effectiveness(thread, data, labels)

if __name__ == "__main__":
    draw_speedups("threads")
    draw_speedups("accounts")
    draw_speedups("blocksize")
    draw_erc20_effectiveness()