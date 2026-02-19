from scripts import probe_anomaly_path as probe_mod


def test_evaluate_probe_passes_with_required_conditions():
    ok, failures = probe_mod._evaluate_probe(
        alert_seen=True,
        anomaly_delta=1,
        summary_request_seen=True,
        summary_db_updated=True,
        summary_dlq_seen=False,
        check_summaries=True,
        require_summary_db_update=True,
        require_no_summary_dlq=True,
    )

    assert ok is True
    assert failures == []


def test_evaluate_probe_fails_when_summary_lands_in_dlq():
    ok, failures = probe_mod._evaluate_probe(
        alert_seen=True,
        anomaly_delta=1,
        summary_request_seen=True,
        summary_db_updated=True,
        summary_dlq_seen=True,
        check_summaries=True,
        require_summary_db_update=False,
        require_no_summary_dlq=True,
    )

    assert ok is False
    assert "probe summary request observed in summaries-deadletter" in failures


def test_evaluate_probe_fails_when_summary_db_update_required_and_missing():
    ok, failures = probe_mod._evaluate_probe(
        alert_seen=True,
        anomaly_delta=1,
        summary_request_seen=True,
        summary_db_updated=False,
        summary_dlq_seen=False,
        check_summaries=True,
        require_summary_db_update=True,
        require_no_summary_dlq=False,
    )

    assert ok is False
    assert "summary DB update not observed for probe symbol" in failures


def test_evaluate_probe_does_not_require_summary_when_flag_disabled():
    ok, failures = probe_mod._evaluate_probe(
        alert_seen=True,
        anomaly_delta=1,
        summary_request_seen=False,
        summary_db_updated=False,
        summary_dlq_seen=False,
        check_summaries=False,
        require_summary_db_update=False,
        require_no_summary_dlq=False,
    )

    assert ok is True
    assert failures == []
