"""One bounded Responses call; injected clients and no semantic/client retries."""
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class ProviderResult:
    payload: str | None = None
    error: str = ""
    provider_calls: int = 0


class Provider(Protocol):
    def observe(self, call_key: str, request: dict) -> ProviderResult: ...


class DeepSeekResponsesProvider:
    def __init__(self, *, api_key: str | None, timeout_seconds: float, client=None):
        self._api_key = api_key
        self._timeout = timeout_seconds
        self._client = client

    def observe(self, call_key: str, request: dict) -> ProviderResult:
        client = self._client
        if client is None:
            if not self._api_key:
                return ProviderResult(error="MISSING_API_KEY")
            from openai import OpenAI

            client = OpenAI(
                api_key=self._api_key, base_url="https://api.deepseek.com",
                timeout=self._timeout, max_retries=0,
            )
        try:
            # Per-call timeout also applies to injected SDK clients.
            response = client.responses.create(**request, timeout=self._timeout)
        except Exception as exc:
            status = getattr(exc, "status_code", None)
            error = "PROVIDER_CONTRACT_INCOMPATIBLE" if status == 400 else "PROVIDER_FAILURE"
            if isinstance(exc, TimeoutError) or type(exc).__name__ == "APITimeoutError":
                error = "PROVIDER_TIMEOUT"
            return ProviderResult(error=error, provider_calls=1)
        payload = getattr(response, "output_text", None)
        if type(payload) is not str:
            return ProviderResult(error="MISSING_OUTPUT_TEXT", provider_calls=1)
        return ProviderResult(payload=payload, provider_calls=1)
