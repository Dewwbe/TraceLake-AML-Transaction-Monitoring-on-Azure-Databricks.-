import shutil

import pytest

if shutil.which("java") is None:  # Spark tests need a JVM
    collect_ignore_glob = []
    SPARK_AVAILABLE = False
else:
    SPARK_AVAILABLE = True


@pytest.fixture(scope="session")
def spark():
    if not SPARK_AVAILABLE:
        pytest.skip("Java not available; Spark tests skipped")
    from aml.common.spark import get_spark

    session = get_spark("aml-tests", local=True)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
