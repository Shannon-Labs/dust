#!/usr/bin/env python3
"""
Dust showoff: demonstrate what you CAN'T do with a normal SQL database.
"""

import subprocess
import os
import sys
import time
import shutil
import yfinance as yf

DUST = os.path.join(os.path.dirname(__file__), "..", "target", "debug", "dust")
TEST_DIR = "/tmp/dust_showoff"

def dust(cmd, *args, cwd=TEST_DIR):
    result = subprocess.run([DUST, cmd, *args], capture_output=True, text=True, cwd=cwd)
    return result.stdout.strip(), result.stderr.strip(), result.returncode

def dust_ok(cmd, *args, cwd=TEST_DIR):
    out, err, rc = dust(cmd, *args, cwd=cwd)
    if rc != 0:
        print(f"  FAIL: {err}")
        sys.exit(1)
    return out

def header(title):
    print(f"\n{'━'*60}")
    print(f"  {title}")
    print(f"{'━'*60}\n")

def main():
    # Clean slate
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)

    print("""
╔══════════════════════════════════════════════════════════╗
║            DUST: Things SQL Can't Normally Do            ║
╚══════════════════════════════════════════════════════════╝
    """)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("1. INSTANT ZERO-CONFIG DATABASE (no Docker, no server)")

    os.makedirs(TEST_DIR, exist_ok=True)
    t0 = time.perf_counter()
    dust_ok("init", TEST_DIR, cwd="/tmp")
    t1 = time.perf_counter()
    init_ms = (t1 - t0) * 1000

    print(f"  dust init → {init_ms:.1f}ms")
    print(f"  Compare: `docker run postgres` takes 3-8 seconds")
    print(f"  That's ~{8000/max(init_ms,1):.0f}x faster to a working database")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("2. BUILT-IN HEALTH CHECKS (dust doctor)")

    out = dust_ok("doctor", TEST_DIR)
    for line in out.split("\n"):
        print(f"  {line}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("3. LOAD REAL MARKET DATA")

    print("  Pulling VIX, SPY, AAPL from Yahoo Finance...")
    tickers_data = {}
    for sym in ["^VIX", "SPY", "AAPL"]:
        t = yf.Ticker(sym)
        hist = t.history(period="1mo")
        tickers_data[sym] = hist
        print(f"    {sym}: {len(hist)} trading days")

    # Build schema + data in one batch
    schema = """
    CREATE TABLE market_data (
        id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        trade_date TEXT NOT NULL,
        open_cents INTEGER NOT NULL,
        high_cents INTEGER NOT NULL,
        low_cents INTEGER NOT NULL,
        close_cents INTEGER NOT NULL,
        volume INTEGER NOT NULL
    )
    """

    rows = []
    row_id = 1
    for sym, hist in tickers_data.items():
        clean_sym = sym.replace("^", "")  # ^VIX -> VIX
        for date, r in hist.iterrows():
            rows.append(f"({row_id}, '{clean_sym}', '{date.strftime('%Y-%m-%d')}', "
                       f"{int(r['Open']*100)}, {int(r['High']*100)}, "
                       f"{int(r['Low']*100)}, {int(r['Close']*100)}, {int(r['Volume'])})")
            row_id += 1

    insert_sql = "INSERT INTO market_data (id, symbol, trade_date, open_cents, high_cents, low_cents, close_cents, volume) VALUES " + ", ".join(rows)
    full_sql = schema + "; " + insert_sql

    t0 = time.perf_counter()
    dust_ok("query", full_sql)
    t1 = time.perf_counter()
    load_ms = (t1 - t0) * 1000
    print(f"\n  Loaded {len(rows)} rows in {load_ms:.1f}ms ({len(rows)/max(load_ms/1000,0.001):.0f} rows/sec)")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("4. QUERY WITH FULL EXPRESSION ENGINE")

    # VIX spikes
    q = full_sql + "; SELECT trade_date, close_cents FROM market_data WHERE symbol = 'VIX' AND close_cents > 2000"
    out = dust_ok("query", q)
    lines = [l for l in out.strip().split("\n") if l.strip()]
    print(f"  VIX days above 20.00:")
    for line in lines[:6]:  # header + first 5
        vals = line.split("\t")
        if vals[0] == "trade_date":
            print(f"    {'Date':<14} {'VIX':>8}")
        else:
            vix_val = int(vals[1]) / 100
            print(f"    {vals[0]:<14} {vix_val:>8.2f}")
    if len(lines) > 6:
        print(f"    ... and {len(lines)-6} more days")

    # SPY range
    print()
    q = full_sql + "; SELECT trade_date, low_cents, high_cents FROM market_data WHERE symbol = 'SPY'"
    out = dust_ok("query", q)
    lines = [l for l in out.strip().split("\n")[1:] if l.strip()]
    lows = [int(l.split("\t")[1]) for l in lines]
    highs = [int(l.split("\t")[2]) for l in lines]
    print(f"  SPY range this month:")
    print(f"    Low:  ${min(lows)/100:.2f}")
    print(f"    High: ${max(highs)/100:.2f}")
    print(f"    Spread: ${(max(highs)-min(lows))/100:.2f}")

    # AAPL volume analysis
    print()
    q = full_sql + "; SELECT trade_date, volume FROM market_data WHERE symbol = 'AAPL' AND volume > 60000000"
    out = dust_ok("query", q)
    lines = [l for l in out.strip().split("\n")[1:] if l.strip()]
    print(f"  AAPL high-volume days (>60M shares): {len(lines)}")
    for line in lines[:3]:
        vals = line.split("\t")
        print(f"    {vals[0]}  vol={int(vals[1]):,}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("5. DATABASE BRANCHING (the killer feature)")

    print("  This is what no normal SQL database can do.")
    print("  We're going to branch the database like a git repo.\n")

    # Show current branch
    out = dust_ok("branch", "current")
    print(f"  Current branch: {out}")

    # Create an experimental branch
    dust_ok("branch", "create", "experiment")
    out = dust_ok("branch", "list")
    print(f"  After creating 'experiment':")
    for line in out.split("\n"):
        print(f"    {line}")

    # Switch to experiment
    dust_ok("branch", "switch", "experiment")
    out = dust_ok("branch", "current")
    print(f"\n  Switched to: {out}")

    # Switch back
    dust_ok("branch", "switch", "main")
    out = dust_ok("branch", "current")
    print(f"  Switched back to: {out}")

    print("""
  Imagine:
    dust branch create staging
    dust branch create feat/new-schema
    # Experiment with ALTER TABLE, migrations, test data
    # Your main branch data is UNTOUCHED
    # When ready: dust merge feat/new-schema
    # This is git for databases.
    """)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("6. EXPLAIN PLANS (built-in query analysis)")

    out = dust_ok("explain", "SELECT * FROM market_data WHERE symbol = 'VIX' AND close_cents > 2000")
    print("  dust explain 'SELECT * FROM market_data WHERE ...'")
    for line in out.split("\n"):
        print(f"    {line}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("7. MUTATION OPERATIONS (UPDATE + DELETE in-flight)")

    # Show the full CRUD cycle in one batch
    q = full_sql + """;
        UPDATE market_data SET close_cents = 0 WHERE symbol = 'VIX' AND close_cents < 1500;
        DELETE FROM market_data WHERE volume = 0;
        SELECT symbol, trade_date, close_cents FROM market_data WHERE symbol = 'VIX'
    """
    out = dust_ok("query", q)
    lines = [l for l in out.strip().split("\n") if l.strip()]
    print(f"  After zeroing VIX closes < 15.00 and deleting zero-volume rows:")
    for line in lines[:6]:
        vals = line.split("\t")
        if vals[0] == "symbol":
            print(f"    {'Symbol':<8} {'Date':<14} {'Close':>8}")
        else:
            print(f"    {vals[0]:<8} {vals[1]:<14} {int(vals[2])/100:>8.2f}")
    if len(lines) > 6:
        print(f"    ... ({len(lines)-1} rows total)")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("8. SCHEMA FINGERPRINTING (built into the lockfile)")

    # Show the lockfile
    lockfile = os.path.join(TEST_DIR, "dust.lock")
    if os.path.exists(lockfile):
        with open(lockfile) as f:
            content = f.read()
        print("  dust.lock (machine-managed schema identity):")
        for line in content.strip().split("\n")[:6]:
            print(f"    {line}")
        print(f"\n  This fingerprint changes when your schema changes.")
        print(f"  It's how Dust detects drift, validates migrations,")
        print(f"  and knows when query artifacts need regeneration.")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    header("9. EVERYTHING IS ONE BINARY")

    binary_size = os.path.getsize(DUST) / (1024 * 1024)
    print(f"  Binary size: {binary_size:.1f} MB")
    print(f"  Dependencies at runtime: 0")
    print(f"  Docker required: no")
    print(f"  Server process: no")
    print(f"  Config files required: 0 (dust.toml is optional)")
    print()
    print(f"  Commands available:")
    print(f"    dust init      Create a project")
    print(f"    dust query      Execute SQL")
    print(f"    dust explain    Show query plans")
    print(f"    dust doctor     Health check")
    print(f"    dust branch     Git-like branching")
    print(f"    dust version    Show version")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print(f"\n{'━'*60}")
    print(f"""
  Dust is "uv for SQL" — it collapses the fragmented database
  toolchain (Docker, ORMs, migration tools, test fixtures,
  schema management) into one fast Rust binary.

  What makes it different:
    - Instant startup (no Docker, no server)
    - Database branching (like git, for your data)
    - Schema identity (fingerprints, lockfiles, stable IDs)
    - Deterministic migrations (content-addressed, not timestamps)
    - Built-in health checks and diagnostics
    - Single binary, zero dependencies
    """)


if __name__ == "__main__":
    main()
