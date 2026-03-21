#!/usr/bin/env python3
"""
End-to-end test: pull financial data from yfinance, load into dust, query it.
"""

import subprocess
import sys
import os
import yfinance as yf

DUST = os.path.join(os.path.dirname(__file__), "..", "target", "debug", "dust")
TEST_DIR = "/tmp/dust_finance_test"

def run_dust(sql):
    """Run a dust query and return (stdout, stderr, returncode)."""
    result = subprocess.run(
        [DUST, "query", sql],
        capture_output=True, text=True, cwd=TEST_DIR
    )
    return result.stdout.strip(), result.stderr.strip(), result.returncode

def run_dust_ok(sql, label=""):
    """Run a dust query, assert success, return stdout."""
    out, err, rc = run_dust(sql)
    if rc != 0:
        print(f"FAIL [{label}]: {err}")
        sys.exit(1)
    return out

def main():
    # Setup
    os.makedirs(TEST_DIR, exist_ok=True)

    # Init project
    subprocess.run([DUST, "init", TEST_DIR, "--force"], capture_output=True)

    # Pull data for several tickers
    tickers = ["SPY", "QQQ", "AAPL", "MSFT", "NVDA"]
    print(f"Pulling data for {tickers}...")

    all_rows = []
    for symbol in tickers:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="3mo")
            for date, row in hist.iterrows():
                date_str = date.strftime("%Y-%m-%d")
                all_rows.append((
                    symbol,
                    date_str,
                    round(row["Open"], 2),
                    round(row["High"], 2),
                    round(row["Low"], 2),
                    round(row["Close"], 2),
                    int(row["Volume"]),
                ))
        except Exception as e:
            print(f"  Warning: could not fetch {symbol}: {e}")

    print(f"Got {len(all_rows)} total rows across {len(tickers)} tickers")

    # Build SQL
    # Create table + insert in one batch (dust is stateless between invocations for now)
    create_sql = """
    CREATE TABLE ohlcv (
        id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        trade_date TEXT NOT NULL,
        open_price INTEGER NOT NULL,
        high_price INTEGER NOT NULL,
        low_price INTEGER NOT NULL,
        close_price INTEGER NOT NULL,
        volume INTEGER NOT NULL
    )
    """

    # We store prices as cents (integer) since dust doesn't have float yet
    insert_chunks = []
    for i, (sym, dt, o, h, l, c, v) in enumerate(all_rows):
        # Convert prices to cents for integer storage
        o_cents = int(o * 100)
        h_cents = int(h * 100)
        l_cents = int(l * 100)
        c_cents = int(c * 100)
        insert_chunks.append(
            f"({i+1}, '{sym}', '{dt}', {o_cents}, {h_cents}, {l_cents}, {c_cents}, {v})"
        )

    # Batch inserts (dust handles multi-value INSERT)
    batch_size = 50
    batches = [insert_chunks[i:i+batch_size] for i in range(0, len(insert_chunks), batch_size)]

    # First batch includes CREATE TABLE
    first_batch_sql = create_sql + "; INSERT INTO ohlcv (id, symbol, trade_date, open_price, high_price, low_price, close_price, volume) VALUES " + ", ".join(batches[0])

    print("\n--- TEST 1: Create table + insert first batch ---")
    out = run_dust_ok(first_batch_sql, "create+insert")
    print(f"  Result: {out}")

    # Insert remaining batches
    for batch_idx, batch in enumerate(batches[1:], 2):
        sql = "INSERT INTO ohlcv (id, symbol, trade_date, open_price, high_price, low_price, close_price, volume) VALUES " + ", ".join(batch)
        # Need to include CREATE TABLE since dust is stateless between calls
        full_sql = create_sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS") + "; " + sql
        out = run_dust_ok(full_sql, f"insert batch {batch_idx}")

    # For the query tests, we need to include everything in one batch
    # Build a mega SQL that creates, inserts all, then queries
    all_inserts = "; INSERT INTO ohlcv (id, symbol, trade_date, open_price, high_price, low_price, close_price, volume) VALUES ".join(
        [", ".join(batch) for batch in batches]
    )
    all_inserts = "INSERT INTO ohlcv (id, symbol, trade_date, open_price, high_price, low_price, close_price, volume) VALUES " + all_inserts

    base_sql = create_sql + "; " + all_inserts

    print(f"\n--- TEST 2: Count all rows (select all) ---")
    query_sql = base_sql + "; SELECT * FROM ohlcv"
    out = run_dust_ok(query_sql, "select all")
    row_count = len(out.strip().split("\n")) - 1  # minus header
    print(f"  Rows returned: {row_count}")
    assert row_count == len(all_rows), f"Expected {len(all_rows)}, got {row_count}"

    print(f"\n--- TEST 3: Filter by symbol (WHERE) ---")
    query_sql = base_sql + "; SELECT id, symbol, trade_date, close_price FROM ohlcv WHERE symbol = 'AAPL'"
    out = run_dust_ok(query_sql, "filter by symbol")
    aapl_rows = len(out.strip().split("\n")) - 1
    print(f"  AAPL rows: {aapl_rows}")
    assert aapl_rows > 0, "Expected some AAPL rows"

    print(f"\n--- TEST 4: Filter by price (WHERE with comparison) ---")
    # Find rows where close > $500 (50000 cents)
    query_sql = base_sql + "; SELECT symbol, trade_date, close_price FROM ohlcv WHERE close_price > 50000"
    out = run_dust_ok(query_sql, "filter by price")
    expensive_rows = len(out.strip().split("\n")) - 1
    print(f"  Rows with close > $500: {expensive_rows}")

    print(f"\n--- TEST 5: Compound WHERE (AND) ---")
    query_sql = base_sql + "; SELECT symbol, trade_date, close_price FROM ohlcv WHERE symbol = 'NVDA' AND close_price > 10000"
    out = run_dust_ok(query_sql, "compound where")
    nvda_rows = len(out.strip().split("\n")) - 1
    print(f"  NVDA rows with close > $100: {nvda_rows}")

    print(f"\n--- TEST 6: UPDATE (adjust prices) ---")
    query_sql = base_sql + "; UPDATE ohlcv SET close_price = 99999 WHERE symbol = 'SPY' AND id = 1; SELECT close_price FROM ohlcv WHERE id = 1"
    out = run_dust_ok(query_sql, "update")
    print(f"  After update: {out}")
    assert "99999" in out, f"Expected 99999 in output, got: {out}"

    print(f"\n--- TEST 7: DELETE (remove a symbol) ---")
    query_sql = base_sql + "; DELETE FROM ohlcv WHERE symbol = 'QQQ'; SELECT * FROM ohlcv WHERE symbol = 'QQQ'"
    out = run_dust_ok(query_sql, "delete")
    remaining = len(out.strip().split("\n")) - 1 if out.strip() else 0
    print(f"  QQQ rows after delete: {remaining}")
    # After delete, header line is still there but no data rows
    assert remaining == 0, f"Expected 0 QQQ rows after delete, got {remaining}"

    print(f"\n--- TEST 8: OR condition ---")
    query_sql = base_sql + "; SELECT symbol, trade_date FROM ohlcv WHERE symbol = 'AAPL' OR symbol = 'MSFT'"
    out = run_dust_ok(query_sql, "or condition")
    or_rows = len(out.strip().split("\n")) - 1
    print(f"  AAPL + MSFT rows: {or_rows}")

    print(f"\n--- TEST 9: DROP TABLE ---")
    query_sql = base_sql + "; DROP TABLE ohlcv; SELECT 1"
    out = run_dust_ok(query_sql, "drop table")
    print(f"  After drop: {out}")

    print(f"\n{'='*50}")
    print(f"ALL TESTS PASSED")
    print(f"  {len(all_rows)} financial data rows loaded and queried")
    print(f"  Tickers: {', '.join(tickers)}")
    print(f"  Operations tested: CREATE, INSERT, SELECT, WHERE, AND, OR, UPDATE, DELETE, DROP")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
