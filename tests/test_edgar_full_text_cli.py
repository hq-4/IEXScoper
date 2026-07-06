from pathlib import Path

from utils.search_edgar_full_text import prepare_log_path


def test_prepare_log_path_truncates_stale_log_by_default(tmp_path: Path) -> None:
    output_root = tmp_path / "out"
    output_root.mkdir()
    log_path = output_root / "edgar_full_text_search.jsonl"
    log_path.write_text('{"level":"ERROR"}\n')

    assert prepare_log_path(output_root, append_log=False) == log_path

    assert not log_path.exists()


def test_prepare_log_path_preserves_log_when_append_requested(tmp_path: Path) -> None:
    output_root = tmp_path / "out"
    output_root.mkdir()
    log_path = output_root / "edgar_full_text_search.jsonl"
    log_path.write_text('{"level":"ERROR"}\n')

    assert prepare_log_path(output_root, append_log=True) == log_path

    assert log_path.read_text() == '{"level":"ERROR"}\n'
