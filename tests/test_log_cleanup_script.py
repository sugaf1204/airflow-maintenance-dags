from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from log_cleanup import build_cleanup_bash_script


def test_build_script_includes_default_value():
    script = build_cleanup_bash_script(
        default_max_log_age_in_days=42,
        enable_delete=True,
        lock_file_path="/tmp/test.lock",
    )

    assert "Using Default '42'" in script
    assert "MAX_LOG_AGE_IN_DAYS='42'" in script


def test_build_script_can_disable_delete():
    script = build_cleanup_bash_script(
        default_max_log_age_in_days=30,
        enable_delete=False,
        lock_file_path="/tmp/test.lock",
    )

    assert 'ENABLE_DELETE="false"' in script


def test_build_script_embeds_lock_file_path():
    lock_file = "/tmp/custom.lock"
    script = build_cleanup_bash_script(
        default_max_log_age_in_days=30,
        enable_delete=True,
        lock_file_path=lock_file,
    )

    assert script.count(lock_file) >= 3
    assert "-exec rm -f {} \\;" in script
