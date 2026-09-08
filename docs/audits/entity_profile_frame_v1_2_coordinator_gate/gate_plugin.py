"""Record collection/results/fixture flags and restrict real HTTP transports."""
import json
import os
from pathlib import Path
import threading
from urllib.parse import urlsplit

import pytest

OUT = Path(os.environ['COORDINATOR_OUTPUT'])
NAME = os.environ['COORDINATOR_COMPONENT']
DATA = {'collection': [], 'results': [], 'flags': {}, 'network': [], 'deselected': []}
LOCK = threading.RLock()


def permitted(method, url):
    parsed = urlsplit(str(url))
    return (os.environ['COORDINATOR_NETWORK'] == 'secop_reads'
            and parsed.hostname in ('www.datos.gov.co', 'datos.gov.co')
            and (method in ('GET', 'HEAD') or
                 (method == 'POST' and parsed.path.startswith('/api/v3/views/')
                  and parsed.path.endswith('/query.json'))))


def guard(original):
    def send(self, request, *args, **kwargs):
        url = str(request.url)
        parsed = urlsplit(url)
        event = {'method': request.method, 'host': parsed.hostname, 'path': parsed.path,
                 'allowed': permitted(request.method, url),
                 'nodeid': os.environ.get('PYTEST_CURRENT_TEST', 'collection')}
        with LOCK:
            DATA['network'].append(event)
            if not event['allowed']:
                raise AssertionError(f'GATE_UNAUTHORIZED_NETWORK: {event}')
            return original(self, request, *args, **kwargs)
    return send


# Installed at plugin import, before test collection; synthetic transports remain intact.
import requests.adapters  # noqa: E402
import httpx  # noqa: E402
requests.adapters.HTTPAdapter.send = guard(requests.adapters.HTTPAdapter.send)
httpx.HTTPTransport.handle_request = guard(httpx.HTTPTransport.handle_request)


def pytest_collection_finish(session):
    DATA['collection'] = [item.nodeid for item in session.items]
    DATA['modules'] = {item.nodeid: str(item.path) for item in session.items}


def pytest_deselected(items):
    DATA['deselected'].extend(item.nodeid for item in items)


def pytest_runtest_call(item):
    from app.config import entity_profile_frame_enabled
    DATA['flags'][item.nodeid] = {'before_call': entity_profile_frame_enabled()}


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    entry = {'nodeid': item.nodeid, 'when': report.when, 'outcome': report.outcome,
             'duration': report.duration, 'xfail': getattr(report, 'wasxfail', None)}
    if report.failed or report.skipped:
        entry['detail'] = str(report.longrepr)
    DATA['results'].append(entry)
    if report.when == 'call':
        from app.config import entity_profile_frame_enabled
        DATA['flags'][item.nodeid]['after_call'] = entity_profile_frame_enabled()


def pytest_sessionfinish(session, exitstatus):
    DATA['exitstatus'] = int(exitstatus)
    (OUT / f'{NAME}.json').write_text(json.dumps(DATA, indent=2) + '\n')
