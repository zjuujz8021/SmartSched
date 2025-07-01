import numpy as np
import matplotlib.pyplot as plt

all_schemes =        ["serial", "occ", "2pl", "dmvcc", "spectrum", "stm", "parevm", "smartsched_onlyPreCommit", "smartsched_onlySched", "smartsched"]
comparable_schemes = ["serial", "occ", "2pl", "dmvcc", "spectrum", "stm", "parevm", "smartsched"]
sched_schemes =      ["serial", "smartsched_onlySched", "smartsched_onlyPreCommit", "smartsched"]

all_schemes_set = set(all_schemes)
comparable_schemes_set = set(comparable_schemes)
sched_schemes_set = set(sched_schemes)

legends = {"serial": "Serial", "occ": "OCC", "2pl": "2PL", "stm": "BlockSTM", "spectrum": "Spectrum", "dmvcc": "DMVCC", "parevm": "ParallelEVM", "smartsched": "SmartSched", "smartsched_onlyPreCommit": r"SmartSched$^*$", "smartsched_onlySched": r"SmartSched$^\dagger$"}

colors = {
    "serial": "black",
    "occ": '#1f77b4', 
    "2pl": "#ff7f0e", 
    "stm": "#2ca02c", 
    "spectrum": "#d62728", 
    "dmvcc": "#9467bd", 
    "parevm": "#8c564b", 
    "smartsched": "#e377c2", 
    "smartsched_onlyPreCommit": "#7f7f7f",
    "smartsched_onlySched": "#bcbd22",
}

markers = {
    "serial": "+",
    "occ": "s", 
    "2pl": "x", 
    "stm": "o", 
    "spectrum": "D", 
    "dmvcc": "v", 
    "parevm": "p", 
    "smartsched": "^",
    "smartsched_onlyPreCommit": ">",
    "smartsched_onlySched": "<",
}

# data: name: str => values: list[float]
# labels: list[str]
def plot_effectiveness(file_name, data, x_axis ,x_labels):
    print(x_axis, x_labels)
    print(data)
    width = 0.2
    x = len(data["serial"])
    if x != len(x_labels):
        raise ValueError(f"Labels length mismatch: {len(x_labels)} != {x}")
    groups = np.arange(len(x_labels))
    plt.figure()
    ax = plt.subplot(1,1,1)
    # ax.set_title("ERC-20" if file_name == "erc20" else "History", fontsize=18)
    for i, (scheme, values) in enumerate(data.items()):
        if len(values) != x:
            raise ValueError(f"Data length mismatch: {len(values)} != {x}")
        ax.bar(groups + i * width, values, width, label=legends[scheme], color=colors[scheme])
    ax.set_xlabel(x_axis, fontsize=18, labelpad=2)
    if file_name == "erc20":
        ax.set_ylabel("Speedup", fontsize=18, labelpad=0)
    else:
        ax.set_ylabel("Speedup", fontsize=18, labelpad=9)
    ax.set_xticks(groups + width * (len(data) - 1) / 2)
    ax.set_xticklabels(x_labels, fontsize=18)
    if file_name == "erc20":
        ax.set_yticks([2, 4, 6, 8, 10])
    else:
        ax.set_yticks([1, 2, 3, 4])
        ax.set_ylim(0.5, 4.2)
    ax.yaxis.set_tick_params(labelsize=18)
    # ax.legend(fontsize=14, loc="upper center", bbox_to_anchor=(0.5, 1.13), ncol=4, columnspacing=0.9, labelspacing=0.2)
    if file_name == "erc20":
        ax.legend(fontsize=12, columnspacing=0.9, labelspacing=0.1)
    else:
        ax.legend(fontsize=12, loc='upper center', columnspacing=0.9, labelspacing=0.1, ncol=4)
    plt.savefig(f"./results/effectiveness_{file_name}.pdf", bbox_inches="tight")
    plt.cla()

def plot_erc20_effectiveness(thread, data, accounts):
    print(thread, accounts)
    for scheme in data:
        print(scheme, data[scheme])
    width = 0.2
    x = len(data["smartsched"])
    if x != len(accounts):
        raise ValueError(f"Labels length mismatch: {len(accounts)} != {x}")
    groups = np.arange(len(accounts))
    plt.figure()
    ax = plt.subplot(1,1,1)
    ax.set_title(f"{thread} threads", fontsize=18)
    for i, (scheme, values) in enumerate(data.items()):
        if len(values) != x:
            raise ValueError(f"Data length mismatch: {len(values)} != {x}")
        ax.bar(groups + i * width, values, width, label=legends[scheme], color=colors[scheme])
    ratiosPrecommit = []
    ratiosSched = []
    for i in range(x):
        # ratios.append(min(data["smartsched"][i]/data["smartsched_onlyPreCommit"][i], data["smartsched"][i]/data["smartsched_onlySched"][i]))
        ratiosPrecommit.append(data["smartsched"][i]/data["smartsched_onlyPreCommit"][i])
        ratiosSched.append(data["smartsched"][i]/data["smartsched_onlySched"][i])
    print(f"ratiosSched: {ratiosSched}")
    print(f"ratiosPrecommit: {ratiosPrecommit}")
    ax.plot(groups + width * (len(data) - 1), ratiosSched, label=r"R$^\dagger$", linewidth=3, marker="D", markersize=7, color=colors["smartsched_onlySched"])
    ax.plot(groups + width * (len(data) - 1), ratiosPrecommit, label=r"R$^*$", linewidth=3, marker="^", markersize=7, color=colors["smartsched_onlyPreCommit"])
    ax.set_xlabel("Number of accounts", fontsize=18, labelpad=2)
    ax.set_xticks(groups + width * (len(data) - 1) / 2)
    ax.set_xticklabels(accounts, fontsize=18)
    if thread > 10:
        ax.set_yticks([1, 2, 4, 6, 8, 10, 12])
        ax.set_ylabel("Speedup", fontsize=18, labelpad=0)
    else:
        ax.set_ylabel("Speedup", fontsize=18, labelpad=9)
    ax.yaxis.set_tick_params(labelsize=18)
    # ax.legend(fontsize=14, loc="upper center", bbox_to_anchor=(0.5, 1.13), ncol=4, columnspacing=0.9, labelspacing=0.2)
    ax.legend(fontsize=12, columnspacing=0.9, labelspacing=0.1)
    plt.savefig(f"./results/effectiveness_erc20_{thread}.pdf", bbox_inches="tight")
    plt.cla()