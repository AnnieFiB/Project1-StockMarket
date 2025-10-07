-- Schema (optional; keep "public" if you prefer)
CREATE SCHEMA IF NOT EXISTS public;

-- Main table  Spark job writes to
CREATE TABLE IF NOT EXISTS public.stock_stream (
    symbol       text,
    bar_time     timestamp with time zone,
    open         double precision,
    high         double precision,
    low          double precision,
    close        double precision,
    volume       double precision,
    src          text,
    ingested_at  timestamp with time zone
);

-- Recommended uniqueness (if you want to avoid duplicates)
CREATE UNIQUE INDEX IF NOT EXISTS ux_stock_stream_fullrow
ON public.stock_stream(symbol, bar_time);

-- Another table if required events_stream


-- Permissions 
-- GRANT USAGE ON SCHEMA public TO readonly;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;
