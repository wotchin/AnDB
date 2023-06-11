import pytest


def setup():
    pass


def cleanup():
    pass


@pytest.fixture(autouse=True)
def run_before_and_after_test_case():
    setup()

    yield

    cleanup()
