CREATE TABLE IF NOT EXISTS gold_daily_stock_metrics (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    price_date DATE NOT NULL,
    average_price NUMERIC(18, 6) NOT NULL,
    highest_price NUMERIC(18, 6) NOT NULL,
    lowest_price NUMERIC(18, 6) NOT NULL,
    daily_change NUMERIC(18, 6),
    daily_change_pct NUMERIC(18, 6),
    total_volume BIGINT NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_gold_stock_symbol_date UNIQUE (symbol, price_date)
);
