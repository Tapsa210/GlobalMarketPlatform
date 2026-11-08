
CREATE TABLE IF NOT EXISTS bronze_stock_prices (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    symbol VARCHAR(20) NOT NULL,
    price_date DATE NOT NULL,

    open_price NUMERIC(18, 6) NOT NULL,
    high_price NUMERIC(18, 6) NOT NULL,
    low_price NUMERIC(18, 6) NOT NULL,
    close_price NUMERIC(18, 6) NOT NULL,

    volume BIGINT NOT NULL,

    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_bronze_stock_symbol_date
        UNIQUE (symbol, price_date),

    CONSTRAINT chk_bronze_stock_prices_positive
        CHECK (
            open_price > 0
            AND high_price > 0
            AND low_price > 0
            AND close_price > 0
        ),

    CONSTRAINT chk_bronze_stock_high_low
        CHECK (high_price >= low_price),

    CONSTRAINT chk_bronze_stock_volume
        CHECK (volume >= 0)
);
