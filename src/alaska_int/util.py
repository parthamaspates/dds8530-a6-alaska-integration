import json, logging, os, sys, time
from contextlib import contextmanager
from . import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout),
                              logging.FileHandler(config.OUT_DIR / "pipeline.log")])
log = logging.getLogger("alaska")

TIMINGS = {}

@contextmanager
def timer(name):
    """Time a block and keep the result in TIMINGS (seconds)."""
    t0 = time.perf_counter()
    yield
    TIMINGS[name] = round(time.perf_counter() - t0, 3)
    log.info("%s: %.2f s", name, TIMINGS[name])

def save_json(obj, name):
    path = config.OUT_DIR / name
    path.write_text(json.dumps(obj, indent=2, default=str))
    return path

def spark_session(app="alaska", kafka=False):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.appName(app).master(config.SPARK_MASTER)
         .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
         .config("spark.sql.execution.arrow.pyspark.enabled", os.getenv("SPARK_ARROW", "true"))
         .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
         .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.ui.showConsoleProgress", "false"))
    if config.SPARK_MASTER.startswith("spark://"):
        import socket                       # executors on the worker nodes must reach the driver in this container
        b = (b.config("spark.driver.host", socket.gethostbyname(socket.gethostname()))
              .config("spark.driver.bindAddress", "0.0.0.0")
              .config("spark.executor.memory", "1g").config("spark.executor.cores", "2")
              .config("spark.cores.max", "4"))
    if kafka:
        b = b.config("spark.jars.packages", config.SPARK_KAFKA_PACKAGE)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def cluster_info(spark):
    """Master URL, executors (one per worker node) and total cores, to document where Spark ran."""
    sc = spark.sparkContext
    status = lambda: [f"{e.host()}:{e.port()}" for e in sc._jsc.sc().statusTracker().getExecutorInfos()]
    execs, deadline = status(), time.time() + 60
    while sc.master.startswith("spark://") and len(execs) < 3 and time.time() < deadline:   # driver + 2 executors
        time.sleep(2)
        execs = status()
    if sc.master.startswith("spark://") and len(execs) < 2:
        raise RuntimeError("No Spark executors registered with the master within 60 s; check the spark-worker containers.")
    return {"master": sc.master, "block_managers": sorted(execs),
            "executors": max(0, len(execs) - 1) if sc.master.startswith("spark://") else 0,
            "default_parallelism": sc.defaultParallelism}
