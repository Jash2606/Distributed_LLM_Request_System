"""
Integration tests — requires docker-compose to be running.

Run with:
    pytest tests/integration/ -v
"""

import os
import pytest
import httpx

BASE_URL = os.getenv("API_URL", "http://localhost:8000")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_health_ok():
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        r = await client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] in ("healthy", "degraded")
    assert "components" in data


@pytest.mark.integration
@pytest.mark.asyncio
async def test_process_returns_completed():
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=40) as client:
        r = await client.post("/process", json={
            "user_id": "u_test",
            "prompt_id": "p_e2e_001",
            "text": "What is 2+2?",
            "priority": "high",
        })
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "completed"
    assert data["response"] is not None
    assert data["processing_time_ms"] is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_idempotency_same_returns_same():
    payload = {"user_id": "u_idem", "prompt_id": "p_idem_001", "text": "idempotency test"}
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=40) as client:
        r1 = await client.post("/process", json=payload)
        r2 = await client.post("/process", json=payload)
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json()["response"] == r2.json()["response"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_idempotency_conflict_returns_409():
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=40) as client:
        await client.post("/process", json={
            "user_id": "u1", "prompt_id": "p_conflict_001", "text": "original text"
        })
        r = await client.post("/process", json={
            "user_id": "u1", "prompt_id": "p_conflict_001", "text": "different text"
        })
    assert r.status_code == 409


@pytest.mark.integration
@pytest.mark.asyncio
async def test_second_request_is_cached():
    text = "Explain photosynthesis in simple terms"
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=40) as client:
        r1 = await client.post("/process", json={
            "user_id": "u_cache", "prompt_id": "p_cache_001", "text": text
        })
        r2 = await client.post("/process", json={
            "user_id": "u_cache", "prompt_id": "p_cache_002", "text": text
        })
    assert r1.json()["status"] == "completed"
    assert r2.json()["cached"] is True
    assert r1.json()["response"] == r2.json()["response"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_result_endpoint():
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=40) as client:
        await client.post("/process", json={
            "user_id": "u_result", "prompt_id": "p_result_001", "text": "result endpoint test"
        })
        r = await client.get("/result/p_result_001")
    assert r.status_code == 200
    assert r.json()["prompt_id"] == "p_result_001"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_result_404_unknown():
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        r = await client.get("/result/nonexistent_prompt_xyz")
    assert r.status_code == 404


@pytest.mark.integration
@pytest.mark.asyncio
async def test_invalid_priority_rejected():
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        r = await client.post("/process", json={
            "user_id": "u1", "prompt_id": "p_bad", "text": "ok", "priority": "urgent"
        })
    assert r.status_code == 422
