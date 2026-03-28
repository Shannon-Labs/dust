CREATE TABLE accounts (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE ledger_entries (
    id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL,
    memo TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    booked_on TEXT NOT NULL
);
