import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: marks tests requiring docker-compose")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--run-integration", default=False):
        skip = pytest.mark.skip(reason="Pass --run-integration to run")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)


def pytest_addoption(parser):
    parser.addoption("--run-integration", action="store_true", default=False)
