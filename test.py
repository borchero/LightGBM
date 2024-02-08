import sys

import lightgbm as lgb
import polars as pl
import psutil
from pyarrow.cffi import ffi as arrow_cffi

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


# df = pl.read_parquet("df.parquet", use_pyarrow=True, memory_map=False)
# ldf = pl.concat([pl.scan_parquet("df.parquet") for _ in range(10)])
ldf = pl.scan_parquet("df.parquet")

for _ in range(10):
    log_memory_consumption()
    # ds = pl.read_parquet("df.parquet")
    # ds = ldf.collect().to_arrow()
    # ds = lgb.Dataset(df.to_arrow())
    df = ldf.collect()
    arr = df.to_arrow()

    # export_objects = arr.to_batches()
    # chunks = arrow_cffi.new("struct ArrowArray[]", len(export_objects))
    # schema = arrow_cffi.new("struct ArrowSchema*")
    # for i, obj in enumerate(export_objects):
    #     chunk_ptr = int(arrow_cffi.cast("uintptr_t", arrow_cffi.addressof(chunks[i])))
    #     if i == 0:
    #         schema_ptr = int(arrow_cffi.cast("uintptr_t", schema))
    #         obj._export_to_c(chunk_ptr, schema_ptr)
    #     else:
    #         obj._export_to_c(chunk_ptr)

    ds = lgb.Dataset(arr)
    ds.construct()
    del df
    del ds
    del arr
log_memory_consumption()
