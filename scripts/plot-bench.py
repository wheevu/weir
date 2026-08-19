#!/usr/bin/env python3
"""Plot the backend campaign results from results/bench/*.json.

Output: results/bench/graphs/{throughput,latency,rejection,memory}.png
One line per backend, one point per connection level (mean of samples).
"""

import glob
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "..", "results", "bench")
OUT = os.path.join(HERE, "..", "results", "bench", "graphs")

BACKENDS = ["epoll", "coroutine", "io_uring"]
LEVELS = [1000, 5000, 10000, 20000]
COLORS = {"epoll": "#1f77b4", "coroutine": "#ff7f0e", "io_uring": "#2ca02c"}
MARKERS = {"epoll": "o", "coroutine": "s", "io_uring": "^"}


def load():
    """Return {backend: {level: {field: mean_over_samples}}}."""
    data = {b: {lvl: {} for lvl in LEVELS} for b in BACKENDS}
    for path in glob.glob(os.path.join(DATA, "*.json")):
        name = os.path.basename(path)[:-len(".json")]
        backend, level, _sample = name.rsplit("-", 2)
        level = int(level)
        with open(path) as f:
            sample = json.load(f)
        for key, value in sample.items():
            if key in ("backend", "connections"):
                continue
            acc = data[backend][level].setdefault(key, [])
            acc.append(value)
    for backend in BACKENDS:
        for level in LEVELS:
            for key in list(data[backend][level]):
                values = data[backend][level][key]
                data[backend][level][key] = sum(values) / len(values)
    return data


def plot(ax, data, field, ylabel, yscale="linear", title=None):
    for backend in BACKENDS:
        xs = LEVELS
        ys = [data[backend][lvl][field] for lvl in xs]
        ax.plot(xs, ys, label=backend, color=COLORS[backend],
                marker=MARKERS[backend], linewidth=1.5)
    ax.set_xscale("log")
    ax.set_yscale(yscale)
    ax.set_xticks(LEVELS)
    ax.set_xticklabels([f"{lvl:,}" for lvl in LEVELS])
    ax.set_xlabel("concurrent connections")
    ax.set_ylabel(ylabel)
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()
    if title:
        ax.set_title(title)


def main():
    data = load()
    os.makedirs(OUT, exist_ok=True)

    fig, ax = plt.subplots(figsize=(6, 4))
    plot(ax, data, "throughput_ops_s", "accepted events / s",
         title="Throughput (mean of 3 x 30 s samples)")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "throughput.png"), dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4))
    plot(ax, data, "p50_us", "p50 latency (us)", yscale="log",
         title="Median round-trip latency")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "latency-p50.png"), dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4))
    plot(ax, data, "p999_us", "p999 latency (us)", yscale="log",
         title="p999 round-trip latency")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "latency-p999.png"), dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4))
    for backend in BACKENDS:
        xs = LEVELS
        ys = [data[backend][lvl]["err_replies"]
              / (data[backend][lvl]["ops"] + data[backend][lvl]["err_replies"])
              * 100 for lvl in xs]
        ax.plot(xs, ys, label=backend, color=COLORS[backend],
                marker=MARKERS[backend], linewidth=1.5)
    ax.set_xscale("log")
    ax.set_xticks(LEVELS)
    ax.set_xticklabels([f"{lvl:,}" for lvl in LEVELS])
    ax.set_xlabel("concurrent connections")
    ax.set_ylabel("in-band rejections (% of replies)")
    ax.set_ylim(0, 100)
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()
    ax.set_title("Admission: share of replies rejected with ERR queue")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "rejection.png"), dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4))
    for backend in BACKENDS:
        xs = LEVELS
        ys = [data[backend][lvl]["rss_kb"] * 1024 / lvl for lvl in xs]
        ax.plot(xs, ys, label=backend, color=COLORS[backend],
                marker=MARKERS[backend], linewidth=1.5)
    ax.set_xscale("log")
    ax.set_xticks(LEVELS)
    ax.set_xticklabels([f"{lvl:,}" for lvl in LEVELS])
    ax.set_xlabel("concurrent connections")
    ax.set_ylabel("server RSS per connection (bytes)")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()
    ax.set_title("Memory per connection (max RSS / connections)")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "memory.png"), dpi=150)
    plt.close(fig)

    print("wrote", ", ".join(sorted(os.listdir(OUT))))


if __name__ == "__main__":
    main()