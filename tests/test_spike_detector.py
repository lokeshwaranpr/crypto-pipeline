from crypto_pipeline.smart_consumer import SpikeDetector


def test_returns_none_with_fewer_than_3_points():
    detector = SpikeDetector(window_size=10, threshold_pct=0.5)
    assert detector.check("btc", 100.0) is None
    assert detector.check("btc", 101.0) is None


def test_no_spike_within_threshold():
    detector = SpikeDetector(window_size=10, threshold_pct=0.5)
    for price in [100.0, 100.0, 100.0]:
        detector.check("btc", price)
    result = detector.check("btc", 100.1)  # 0.1% — below 0.5% threshold
    assert result is None


def test_spike_up():
    detector = SpikeDetector(window_size=10, threshold_pct=0.5)
    for price in [100.0, 100.0, 100.0]:
        detector.check("btc", price)
    result = detector.check("btc", 102.0)  # 2% above avg
    assert result is not None
    assert result["direction"] == "UP"
    assert result["change_pct"] > 0


def test_spike_down():
    detector = SpikeDetector(window_size=10, threshold_pct=0.5)
    for price in [100.0, 100.0, 100.0]:
        detector.check("btc", price)
    result = detector.check("btc", 98.0)  # 2% below avg
    assert result is not None
    assert result["direction"] == "DOWN"
    assert result["change_pct"] < 0


def test_independent_windows_per_coin():
    detector = SpikeDetector(window_size=10, threshold_pct=0.5)
    for price in [100.0, 100.0, 100.0]:
        detector.check("btc", price)
    # eth has empty window — should not spike
    assert detector.check("eth", 999.0) is None


def test_rolling_window_evicts_old_prices():
    detector = SpikeDetector(window_size=3, threshold_pct=0.5)
    # Fill window with high prices
    for price in [200.0, 200.0, 200.0]:
        detector.check("btc", price)
    # Add 3 low prices — old highs evicted
    for price in [100.0, 100.0, 100.0]:
        detector.check("btc", price)
    # Now avg should be ~100, so 100.1 is no spike
    result = detector.check("btc", 100.1)
    assert result is None
