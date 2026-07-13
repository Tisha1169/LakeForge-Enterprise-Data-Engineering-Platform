from datetime import date

import pytest
from pipelines.silver.runner import _JOBS, run_silver_job


def test_run_silver_job_raises_on_unknown_job_name():
    with pytest.raises(ValueError, match="Unknown Silver job 'not_a_real_job'"):
        run_silver_job("not_a_real_job", date(2024, 1, 1))


def test_run_silver_job_dispatches_to_the_right_module(monkeypatch):
    """Verifies dispatch logic only — each job's own run()/clean() behavior
    is already covered by tests/silver/test_*_silver.py, so this stubs the
    target module's run() rather than re-exercising Spark."""
    calls = []
    monkeypatch.setattr(_JOBS["customers"], "run", lambda batch_date: calls.append(batch_date))

    run_silver_job("customers", date(2024, 1, 1))

    assert calls == [date(2024, 1, 1)]


def test_all_registered_jobs_have_a_callable_run_function():
    for job_name, module in _JOBS.items():
        assert callable(module.run), f"{job_name} module has no run() entrypoint"
