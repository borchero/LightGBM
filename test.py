from time import sleep

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
    import resource  # NOTE: This package is not available on Windows

    # Return value is bytes
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (2**30)


def read_vs_scan_parquet():
    # read
    log_memory_consumption()
    df1 = pl.read_parquet("*.parquet")
    sleep(1)
    log_memory_consumption()
    arr1 = df1.to_arrow()
    sleep(1)
    log_memory_consumption()
    del df1
    del arr1
    log_memory_consumption()
    # scan
    ldf1 = pl.scan_parquet("*.parquet")
    sleep(1)
    log_memory_consumption()
    df2 = ldf1.collect()
    sleep(1)
    log_memory_consumption()
    arr2 = df2.to_arrow()
    sleep(1)
    log_memory_consumption()


# df = pl.read_parquet("*.parquet")
# ldf = pl.scan_parquet("df.parquet")
# df = ldf.collect()

# print(df.estimated_size(unit="gb"))

# for _ in range(1):
#     log_memory_consumption()
#     # ds = pl.read_parquet("df.parquet")
#     # ds = ldf.collect().to_arrow()
#     # ds = lgb.Dataset(df.to_arrow())
#     df = ldf.collect()
#     arr = df.to_arrow()

#     # export_objects = arr.to_batches()
#     # chunks = arrow_cffi.new("struct ArrowArray[]", len(export_objects))
#     # schema = arrow_cffi.new("struct ArrowSchema*")
#     # for i, obj in enumerate(export_objects):
#     #     chunk_ptr = int(arrow_cffi.cast("uintptr_t", arrow_cffi.addressof(chunks[i])))
#     #     if i == 0:
#     #         schema_ptr = int(arrow_cffi.cast("uintptr_t", schema))
#     #         obj._export_to_c(chunk_ptr, schema_ptr)
#     #     else:
#     #         obj._export_to_c(chunk_ptr)

#     ds = lgb.Dataset(arr)
#     ds.construct()
# log_memory_consumption()

if __name__ == "__main__":
    read_vs_scan_parquet()
