"""DDS-8530 Assignment 6: big data integration on the Alaska benchmark."""
try:  # keep text columns as Python objects; Dask's automatic Arrow-string conversion used ~6 GB on this data
    import dask
    dask.config.set({"dataframe.convert-string": False})
except ImportError:
    pass
