CREATE TABLE IF NOT EXISTS possessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    acquired_date TEXT,
    amount REAL,
    amount_unit TEXT,
    price REAL,
    purchase_price REAL
);

CREATE TABLE IF NOT EXISTS possessions_containers (
    id TEXT PRIMARY KEY,
    label TEXT,
    tare_weight_g REAL
);

CREATE TABLE IF NOT EXISTS possessions_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    timestamp TEXT,
    amount REAL,
    amount_unit TEXT,
    method TEXT,
    start_date TEXT,
    end_date TEXT
);