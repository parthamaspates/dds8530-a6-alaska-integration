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
    if kafka:
        b = b.config("spark.jars.packages", config.SPARK_KAFKA_PACKAGE)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
