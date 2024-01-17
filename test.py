import sys

import lightgbm as lgb
import polars as pl
import psutil

CURRENT_PROCESS = psutil.Process()


def log_memory_consumption():
    current_memory = CURRENT_PROCESS.memory_info().rss / (2**30)
    peak_memory = _get_peak_memory_gib()
    print(
        f"Currently using {current_memory:.2f} GiB memory (max {peak_memory:.2f} GiB)",
    )


def _get_peak_memory_gib() -> float | None:
    match sys.platform:
        case "win32":
            # Return value is bytes
            return CURRENT_PROCESS.memory_info().peak_wset / (2**30)
        case "linux":
            import resource  # NOTE: This package is not available on Windows

            # Return value is kilobytes
            return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (2**20)
        case "darwin":
            import resource  # NOTE: This package is not available on Windows

            # Return value is bytes
            return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (2**30)
        case _:
            return None


ldf = pl.scan_parquet("df.parquet")

for _ in range(10):
    log_memory_consumption()
    ds = lgb.Dataset(ldf.collect().to_arrow())
    ds.construct()
    del ds
log_memory_consumption()
