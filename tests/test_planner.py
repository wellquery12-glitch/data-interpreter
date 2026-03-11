from app.planner import CodePlanner, DatasetMeta


def test_driver_top_query_generates_value_counts() -> None:
    planner = CodePlanner()
    meta = DatasetMeta(columns=["driver", "weight"], dtypes={"driver": "object", "weight": "float64"})

    code = planner.generate("哪个司机运单最多？", meta)

    assert "value_counts" in code
    assert "driver" in code
