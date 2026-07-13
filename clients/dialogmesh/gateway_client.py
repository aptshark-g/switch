#!/usr/bin/env python3
"""
DialogMesh Gateway Client
Replaces the embedded LLM Provider layer with HTTP calls to the Gateway Runtime.

Usage:
    from clients.dialogmesh.gateway_client import GatewayClient
    client = GatewayClient(base_url="http://localhost:8080")
    response = client.chat(messages=[{"role":"user","content":"Hello"}])
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, AsyncGenerator, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


@dataclass
class GatewayConfig:
    """Configuration for a Gateway client connection."""
    base_url: str = "http://localhost:8080"
    provider: str = ""          # provider name to route to (empty = auto)
    timeout: float = 60.0       # HTTP timeout in seconds
    max_retries: int = 2
    retry_delay: float = 1.0


@dataclass
class ChatResponse:
    """Normalised chat completion response."""
    id: str = ""
    model: str = ""
    content: str = ""
    finish_reason: str = "stop"
    usage: Dict[str, int] = field(default_factory=dict)
    raw: Dict[str, Any] = field(default_factory=dict)


class GatewayError(Exception):
    """Raised when the Gateway returns an error."""
    pass


class GatewayClient:
    """
    HTTP client for the Gateway Runtime.

    Designed as a drop-in replacement for DialogMesh v3.0's
    ``LLMProvider_v3.generate_async()`` — same request/response shape,
    different transport.
    """

    def __init__(self, config: Optional[GatewayConfig] = None) -> None:
        self.config = config or GatewayConfig()
        self._client = httpx.Client(
            base_url=self.config.base_url,
            timeout=self.config.timeout,
        )

    # ------------------------------------------------------------------
    # Synchronous API
    # ------------------------------------------------------------------

    def chat(
        self,
        messages: List[Dict[str, Any]],
        model: str = "",
        provider: str = "",
        temperature: float = 0.7,
        max_tokens: int = 0,
        tools: Optional[List[Dict[str, Any]]] = None,
    ) -> ChatResponse:
        """Send a synchronous chat completion request."""
        provider_name = provider or self.config.provider
        url = "/v1/chat/completions"
        if provider_name:
            url += f"?provider={provider_name}"

        body: Dict[str, Any] = {
            "messages": messages,
            "temperature": temperature,
        }
        if model:
            body["model"] = model
        if max_tokens > 0:
            body["max_tokens"] = max_tokens
        if tools:
            body["tools"] = tools
        body["stream"] = False

        resp = self._request_with_retry("POST", url, json=body)
        return self._parse_response(resp)

    # ------------------------------------------------------------------
    # Streaming API
    # ------------------------------------------------------------------

    def chat_stream(
        self,
        messages: List[Dict[str, Any]],
        model: str = "",
        provider: str = "",
        temperature: float = 0.7,
        max_tokens: int = 0,
    ):
        """Yield streaming chat completion chunks."""
        provider_name = provider or self.config.provider
        url = "/v1/chat/completions"
        if provider_name:
            url += f"?provider={provider_name}"

        body: Dict[str, Any] = {
            "messages": messages,
            "temperature": temperature,
            "stream": True,
        }
        if model:
            body["model"] = model
        if max_tokens > 0:
            body["max_tokens"] = max_tokens

        with self._client.stream("POST", url, json=body, timeout=self.config.timeout) as resp:
            if resp.status_code != 200:
                raise GatewayError(f"HTTP {resp.status_code}: {resp.text}")
            for line in resp.iter_lines():
                if line.startswith("data: "):
                    data = line[6:]
                    if data == "[DONE]":
                        break
                    try:
                        yield json.loads(data)
                    except json.JSONDecodeError:
                        continue

    # ------------------------------------------------------------------
    # Health / Admin
    # ------------------------------------------------------------------

    def health(self) -> bool:
        """Check if the Gateway is reachable and healthy."""
        try:
            resp = self._client.get("/v1/health")
            return resp.status_code == 200
        except Exception:
            return False

    def list_providers(self) -> List[Dict[str, Any]]:
        """Return the list of registered providers."""
        resp = self._client.get("/v1/providers")
        if resp.status_code != 200:
            raise GatewayError(f"HTTP {resp.status_code}: {resp.text}")
        return resp.json().get("providers", [])

    def usage(self) -> Dict[str, Any]:
        """Return aggregated usage statistics."""
        resp = self._client.get("/v1/usage")
        if resp.status_code != 200:
            raise GatewayError(f"HTTP {resp.status_code}: {resp.text}")
        return resp.json()

    # ------------------------------------------------------------------
    # Async variant (for DialogMesh v3.0 integration)
    # ------------------------------------------------------------------

    async def chat_async(
        self,
        messages: List[Dict[str, Any]],
        model: str = "",
        provider: str = "",
        temperature: float = 0.7,
        max_tokens: int = 0,
    ) -> ChatResponse:
        """Async wrapper for chat. Suitable for asyncio event loops."""
        async with httpx.AsyncClient(
            base_url=self.config.base_url,
            timeout=self.config.timeout,
        ) as client:
            provider_name = provider or self.config.provider
            url = "/v1/chat/completions"
            if provider_name:
                url += f"?provider={provider_name}"
            body: Dict[str, Any] = {
                "messages": messages,
                "temperature": temperature,
                "stream": False,
            }
            if model:
                body["model"] = model
            if max_tokens > 0:
                body["max_tokens"] = max_tokens
            resp = await client.post(url, json=body)
            if resp.status_code != 200:
                raise GatewayError(f"HTTP {resp.status_code}: {resp.text}")
            return self._parse_response(resp.json())

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _request_with_retry(self, method: str, url: str, **kwargs) -> httpx.Response:
        last_err = None
        for attempt in range(self.config.max_retries + 1):
            try:
                resp = self._client.request(method, url, **kwargs)
                if resp.status_code < 500:
                    return resp
                last_err = GatewayError(f"HTTP {resp.status_code}: {resp.text}")
            except httpx.RequestError as e:
                last_err = e
            if attempt < self.config.max_retries:
                time.sleep(self.config.retry_delay * (2 ** attempt))
        raise last_err or GatewayError("request failed after retries")

    def _parse_response(self, data: Dict[str, Any]) -> ChatResponse:
        resp = ChatResponse(
            id=data.get("id", ""),
            model=data.get("model", ""),
            raw=data,
        )
        choices = data.get("choices", [])
        if choices:
            msg = choices[0].get("message", {})
            resp.content = msg.get("content", "")
            resp.finish_reason = choices[0].get("finish_reason", "stop")
        usage = data.get("usage", {})
        if usage:
            resp.usage = {
                "prompt_tokens": usage.get("prompt_tokens", 0),
                "completion_tokens": usage.get("completion_tokens", 0),
                "total_tokens": usage.get("total_tokens", 0),
            }
        return resp

    def __repr__(self) -> str:
        return f"GatewayClient(base_url={self.config.base_url!r})"
