"""Unit tests for the batched-insert builder used by normalize's _append_* paths.

These exercise the pure SQL/param construction (no DB): chunking, placeholder
naming, param mapping, and the Postgres bind-param limit guard. The actual
INSERT ... ON CONFLICT DO NOTHING RETURNING behaviour is covered by the live
pipeline; here we lock down the statement shape that the count semantics rely on.
"""
from bountygate.transforms.normalize import _BATCH_ROWS, _build_bulk_insert


def test_single_row_builds_one_statement():
    out = list(_build_bulk_insert("t", ["a", "b"], ["a"], [{"a": 1, "b": 2}]))
    assert len(out) == 1
    sql, params = out[0]
    assert "INSERT INTO t (a, b) VALUES (:a_0, :b_0)" in sql
    assert sql.strip().endswith("ON CONFLICT (a) DO NOTHING RETURNING 1")
    assert params == {"a_0": 1, "b_0": 2}


def test_multi_column_multi_row_placeholders_and_params():
    rows = [{"x": 10, "y": 20}, {"x": 30, "y": 40}]
    out = list(_build_bulk_insert("tbl", ["x", "y"], ["x", "y"], rows, batch_rows=10))
    assert len(out) == 1
    sql, params = out[0]
    assert "VALUES (:x_0, :y_0), (:x_1, :y_1)" in sql
    assert "ON CONFLICT (x, y) DO NOTHING" in sql
    assert params == {"x_0": 10, "y_0": 20, "x_1": 30, "y_1": 40}


def test_chunks_respect_batch_size_and_reindex_per_chunk():
    rows = [{"a": i} for i in range(5)]
    out = list(_build_bulk_insert("t", ["a"], ["a"], rows, batch_rows=2))
    assert len(out) == 3  # 2 + 2 + 1
    assert out[0][1] == {"a_0": 0, "a_1": 1}
    assert "VALUES (:a_0), (:a_1)" in out[0][0]
    assert out[1][1] == {"a_0": 2, "a_1": 3}
    assert out[2][1] == {"a_0": 4}
    assert "VALUES (:a_0)" in out[2][0]


def test_empty_rows_yields_no_statements():
    assert list(_build_bulk_insert("t", ["a"], ["a"], [])) == []


def test_default_batch_stays_under_pg_bind_param_limit():
    # The widest real row (player props) has 8 columns. The default batch size
    # must keep params per statement under Postgres' 65535 bind-param ceiling.
    cols = [f"c{i}" for i in range(8)]
    rows = [{c: 1 for c in cols} for _ in range(_BATCH_ROWS)]
    for _sql, params in _build_bulk_insert("t", cols, cols, rows):
        assert len(params) <= 65535
