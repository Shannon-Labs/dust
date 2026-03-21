-- Dust finance example: market data analysis
-- Run with: dust query -f examples/finance.sql

CREATE TABLE IF NOT EXISTS ohlcv (
    id INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open_cents INTEGER NOT NULL,
    high_cents INTEGER NOT NULL,
    low_cents INTEGER NOT NULL,
    close_cents INTEGER NOT NULL,
    volume INTEGER NOT NULL
);

-- Sample VIX data (prices in cents)
INSERT INTO ohlcv (id, symbol, trade_date, open_cents, high_cents, low_cents, close_cents, volume) VALUES
    (1,  'VIX', '2026-03-02', 2100, 2200, 2050, 2144, 0),
    (2,  'VIX', '2026-03-03', 2144, 2400, 2100, 2356, 0),
    (3,  'VIX', '2026-03-04', 2356, 2400, 2050, 2114, 0),
    (4,  'VIX', '2026-03-05', 2114, 2500, 2100, 2375, 0),
    (5,  'VIX', '2026-03-06', 2375, 2600, 2200, 2550, 0),
    (6,  'SPY', '2026-03-02', 65000, 65500, 64500, 65200, 85000000),
    (7,  'SPY', '2026-03-03', 65200, 65800, 64800, 64900, 92000000),
    (8,  'SPY', '2026-03-04', 64900, 65100, 64000, 64500, 78000000),
    (9,  'SPY', '2026-03-05', 64500, 65000, 63500, 64800, 105000000),
    (10, 'SPY', '2026-03-06', 64800, 66000, 64700, 65900, 88000000);

-- VIX spike days (above 22)
SELECT trade_date, close_cents FROM ohlcv WHERE symbol = 'VIX' AND close_cents > 2200;

-- SPY high-volume days
SELECT trade_date, close_cents, volume FROM ohlcv WHERE symbol = 'SPY' AND volume > 90000000;

-- All data sorted by date
SELECT symbol, trade_date, close_cents FROM ohlcv
