"""Both pages render without raising when the whole stack is unreachable."""

from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

PLAYGROUND = Path(__file__).resolve().parents[1]
DOWN = "http://127.0.0.1:9"


@pytest.fixture(autouse=True)
def stack_is_down(monkeypatch):
    for name in (
        "GATEWAY_URL",
        "NOTIFICATION_URL",
        "ROUTING_URL",
        "CONFIGURATION_URL",
        "EMAIL_URL",
        "TELEGRAM_URL",
    ):
        monkeypatch.setenv(name, DOWN)


def test_the_console_renders_errors_instead_of_raising():
    at = AppTest.from_file(str(PLAYGROUND / "app.py"), default_timeout=120).run()
    assert not at.exception
    assert "unreachable" in " ".join(m.value for m in at.sidebar.markdown)


def test_the_load_test_page_renders():
    at = AppTest.from_file(str(PLAYGROUND / "pages" / "2_Load_test.py"), default_timeout=120).run()
    assert not at.exception
    assert at.title[0].value == "Load test"
