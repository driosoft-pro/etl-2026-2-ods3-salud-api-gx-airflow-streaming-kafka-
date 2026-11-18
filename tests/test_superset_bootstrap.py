"""Regressions for idempotent Superset setup without a live Superset server."""

import pytest

from scripts import superset_bootstrap as bootstrap


def test_existing_dashboard_is_reused_by_slug(monkeypatch):
    calls = []

    def request(method, path, *args, **kwargs):
        calls.append((method, path))
        return 200, {'count': 2, 'result': [
            {'id': 1, 'slug': None}, {'id': 9, 'slug': 'ods3-salud-colombia'},
        ]}

    monkeypatch.setattr(bootstrap, 'request_json', request)
    assert bootstrap.ensure_dashboard('token', 'Health', 'ods3-salud-colombia', 'csrf') == 9
    assert calls == [('GET', '/api/v1/dashboard/?q=(page:0,page_size:100)')]


def test_existing_dataset_matches_database_and_schema(monkeypatch):
    def request(method, path, *args, **kwargs):
        assert method == 'GET'
        return 200, {'count': 3, 'result': [
            {'id': 1, 'table_name': 'dim_facility', 'database': {'id': 5}, 'schema': 'public'},
            {'id': 2, 'table_name': 'dim_facility', 'database': {'id': 7}, 'schema': 'other'},
            {'id': 8, 'table_name': 'dim_facility', 'database': {'id': 7}, 'schema': 'public'},
        ]}

    monkeypatch.setattr(bootstrap, 'request_json', request)
    assert bootstrap.ensure_dataset('token', 7, 'dim_facility', csrf='csrf') == 8


@pytest.mark.parametrize('status,response', [
    (500, {'message': 'service unavailable'}), (200, {'raw': '<html>login</html>'}),
])
def test_failed_lookup_does_not_create_objects(monkeypatch, status, response):
    calls = []

    def request(method, path, *args, **kwargs):
        calls.append(method)
        return status, response

    monkeypatch.setattr(bootstrap, 'request_json', request)
    with pytest.raises(RuntimeError):
        bootstrap.ensure_dashboard('token', 'Health', 'ods3-salud-colombia', 'csrf')
    assert calls == ['GET']


def test_failed_dataset_creation_is_not_reported_as_success(monkeypatch):
    def request(method, path, *args, **kwargs):
        if method == 'GET':
            return 200, {'count': 0, 'result': []}
        return 400, {'message': 'relation missing'}

    monkeypatch.setattr(bootstrap, 'request_json', request)
    with pytest.raises(RuntimeError, match='relation missing'):
        bootstrap.ensure_dataset('token', 1, 'dim_facility', csrf='csrf')


def test_database_errors_redact_passwords(monkeypatch):
    uri = 'postgresql+psycopg2://etl_user:private_password@postgres:5432/salud'
    monkeypatch.setattr(bootstrap, 'DW_URI', uri)
    assert 'private_password' not in bootstrap.safe_error({'message': uri})
    assert 'another_password' not in bootstrap.safe_error(
        'postgresql://etl_user:another_password@postgres/salud'
    )
