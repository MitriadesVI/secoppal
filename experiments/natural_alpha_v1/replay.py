"""Request-exact replay. Missing, duplicated, changed or unused calls fail."""
import json

from app.core.natural_alpha.provider import ProviderResult


class ReplayError(ValueError):
    pass


class RecordedProvider:
    def __init__(self, records: list[dict]):
        self.records, self.used = {}, set()
        for record in records:
            key = record.get("call_key")
            if type(key) is not str or key in self.records:
                raise ReplayError("MISSING_OR_DUPLICATE_CALL_KEY")
            self.records[key] = record

    def observe(self, call_key: str, request: dict) -> ProviderResult:
        record = self.records.get(call_key)
        if record is None:
            raise ReplayError(f"MISSING_CALL_KEY:{call_key}")
        if call_key in self.used:
            raise ReplayError(f"REPEATED_CALL_KEY:{call_key}")
        if record.get("request") != request:
            raise ReplayError(f"REQUEST_MISMATCH:{call_key}")
        self.used.add(call_key)
        return ProviderResult(record.get("payload"), record.get("provider_error", ""), 0)

    def assert_consumed(self):
        if set(self.records) != self.used:
            raise ReplayError("UNUSED_RECORDS")


def load(path):
    document = json.loads(path.read_text())
    records = document.get("provider_records")
    if type(records) is not list:
        raise ReplayError("PROVIDER_RECORDS_REQUIRED")
    return RecordedProvider(records)
