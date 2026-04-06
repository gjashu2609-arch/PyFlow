import time
import random
import matplotlib.pyplot as plt
from utils import timing_decorator


def benchmark_list_vs_set(size: int = 1_000_000) -> dict[str, float]:

    print(f"Generating {size:,} location IDs...")
    location_ids = [random.randint(1, 265) for _ in range(size)]
    search_ids = [random.randint(1, 265) for _ in range(1000)]


    location_list = list(location_ids)
    location_set = set(location_ids)

    start = time.perf_counter()
    for id_ in search_ids:
        _ = id_ in location_list 
    list_time = time.perf_counter() - start

    start = time.perf_counter()
    for id_ in search_ids:
        _ = id_ in location_set   
    set_time = time.perf_counter() - start

    print(f"List lookup time: {list_time:.6f} seconds")
    print(f"Set lookup time:  {set_time:.6f} seconds")
    print(f"Set is {list_time / set_time:.1f}x faster than list")

    return {"list_time": list_time, "set_time": set_time}


def plot_benchmark(results: dict[str, float]) -> None:
    """Plots list vs set benchmark results as a bar chart."""
    labels = ["List (O(n))", "Set (O(1))"]
    times = [results["list_time"], results["set_time"]]
    colors = ["red", "green"]

    plt.figure(figsize=(8, 5))
    bars = plt.bar(labels, times, color=colors, width=0.4)
    plt.title("List vs Set — membership check (1M items, 1000 searches)")
    plt.ylabel("Time (seconds)")

    for bar, t in zip(bars, times):
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.0001,
            f"{t:.6f}s",
            ha="center", va="bottom"
        )

    plt.tight_layout()
    plt.savefig("PyFlow/data/benchmark_plot.png")
    print("Plot saved to data/benchmark_plot.png")
    plt.show()


if __name__ == "__main__":
    results = benchmark_list_vs_set()
    plot_benchmark(results)