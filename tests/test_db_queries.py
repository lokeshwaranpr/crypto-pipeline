from datetime import datetime, timezone, timedelta

import pytest

from crypto_pipeline.db.queries import (
    get_alerts,
    get_latest_prices,
    get_price_history,
    insert_alert,
    insert_tick,
)

NOW = datetime.now(timezone.utc)


@pytest.mark.usefixtures("clean_db")
def test_insert_and_retrieve_tick(pg):
    insert_tick("bitcoin", 50000.0, NOW)
    history = get_price_history("bitcoin", limit=10)
    assert len(history) == 1
    assert history[0]["coin"] == "bitcoin"
    assert history[0]["price_usd"] == 50000.0


@pytest.mark.usefixtures("clean_db")
def test_get_latest_prices_returns_one_per_coin(pg):
    insert_tick("bitcoin", 50000.0, NOW)
    insert_tick("bitcoin", 51000.0, NOW + timedelta(seconds=1))  # later timestamp
    insert_tick("ethereum", 3000.0, NOW)
    latest = get_latest_prices()
    coins = {r["coin"] for r in latest}
    assert "bitcoin" in coins
    assert "ethereum" in coins
    btc = next(r for r in latest if r["coin"] == "bitcoin")
    assert btc["price_usd"] == 51000.0  # most recent wins


@pytest.mark.usefixtures("clean_db")
def test_price_history_limit(pg):
    for i in range(10):
        insert_tick("solana", float(100 + i), NOW)
    history = get_price_history("solana", limit=5)
    assert len(history) == 5


@pytest.mark.usefixtures("clean_db")
def test_insert_and_retrieve_alert(pg):
    insert_alert("bitcoin", 51000.0, 50000.0, 2.0, "UP", NOW)
    alerts = get_alerts()
    assert len(alerts) == 1
    assert alerts[0]["direction"] == "UP"
    assert alerts[0]["change_pct"] == 2.0


@pytest.mark.usefixtures("clean_db")
def test_get_alerts_filtered_by_coin(pg):
    insert_alert("bitcoin", 51000.0, 50000.0, 2.0, "UP", NOW)
    insert_alert("ethereum", 3100.0, 3000.0, 3.3, "UP", NOW)
    btc_alerts = get_alerts(coin="bitcoin")
    assert len(btc_alerts) == 1
    assert btc_alerts[0]["coin"] == "bitcoin"
