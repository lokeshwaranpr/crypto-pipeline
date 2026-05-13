from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from crypto_pipeline.api.app import app
from crypto_pipeline.db.queries import insert_alert, insert_tick

client = TestClient(app)
NOW = datetime.now(timezone.utc)


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.usefixtures("clean_db")
def test_latest_prices_empty(pg):
    response = client.get("/prices/latest")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.usefixtures("clean_db")
def test_latest_prices_with_data(pg):
    insert_tick("bitcoin", 50000.0, NOW)
    insert_tick("ethereum", 3000.0, NOW)
    response = client.get("/prices/latest")
    assert response.status_code == 200
    coins = {r["coin"] for r in response.json()}
    assert "bitcoin" in coins
    assert "ethereum" in coins


@pytest.mark.usefixtures("clean_db")
def test_price_history(pg):
    insert_tick("solana", 100.0, NOW)
    response = client.get("/prices/solana/history?limit=10")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["price_usd"] == 100.0


@pytest.mark.usefixtures("clean_db")
def test_alerts_empty(pg):
    response = client.get("/alerts")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.usefixtures("clean_db")
def test_alerts_with_data(pg):
    insert_alert("bitcoin", 51000.0, 50000.0, 2.0, "UP", NOW)
    response = client.get("/alerts?coin=bitcoin")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["direction"] == "UP"
