-- sigmaelectronica.net, in the same product_avnet database as the Avnet,
-- Automation24 and stex24 tables, hence the prefixed names. Nothing is shared
-- with them: this shop is keyed on its own WooCommerce product id.
--
-- Two tables with two different jobs:
--   sigma_products - the catalogue as it is NOW. A product that leaves the shop
--                    leaves this table too (only after a complete run).
--   sigma_history  - every version of every product, appended, never updated.


CREATE TABLE IF NOT EXISTS sigma_products (
    -- WooCommerce post id: the Store API is keyed on it
    -- (/wp-json/wc/store/v1/products/{id}), so it is the key, not the sku.
    product_id        integer PRIMARY KEY,

    -- Deliberately NOT UNIQUE: the shop does not enforce it, and one clash
    -- would roll back a whole page.
    sku               text NOT NULL,
    -- On this shop name repeats the sku; the readable title is the first line
    -- of short_description.
    name              text,
    url               text NOT NULL,
    -- The pa_fabricante attribute; about a third of the products have one.
    manufacturer      text,

    -- Full paths ("Tarjetas de desarrollo > LattePanda > ..."): the leaf alone
    -- is ambiguous across branches, and a product sits in several of them.
    categories        text[] NOT NULL DEFAULT '{}',
    images            text[] NOT NULL DEFAULT '{}',

    -- Whole Colombian pesos, 19% IVA included (the page's JSON-LD shows the
    -- same prices without it). sale_price is NULL unless on_sale.
    price             numeric(14, 2),
    regular_price     numeric(14, 2),
    sale_price        numeric(14, 2),
    currency          text,
    on_sale           boolean NOT NULL DEFAULT false,

    in_stock          boolean NOT NULL DEFAULT false,
    -- Units on hand from "267 disponibles": 0 when out of stock, NULL when the
    -- shop prints no number.
    stock_qty         integer,

    short_description text,
    description       text,

    -- Fingerprint of exactly the fields above. updated_at moves, and a history
    -- row is written, only when it changes - not on every run.
    data_hash         bytea NOT NULL,

    first_seen_at     timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    -- Rewritten on every run, changed or not.
    last_seen_at      timestamptz NOT NULL DEFAULT now()
);

-- last_seen_at is rewritten for every row on every run, so leave room on the
-- pages for HOT updates instead of letting the table sprawl each time.
ALTER TABLE sigma_products SET (fillfactor = 70);

CREATE INDEX IF NOT EXISTS idx_sigma_products_sku          ON sigma_products (sku);
CREATE INDEX IF NOT EXISTS idx_sigma_products_manufacturer ON sigma_products (manufacturer);
CREATE INDEX IF NOT EXISTS idx_sigma_products_in_stock     ON sigma_products (in_stock);
CREATE INDEX IF NOT EXISTS idx_sigma_products_updated_at   ON sigma_products (updated_at DESC);
-- GIN, because the question asked of an array is "which products are in X".
CREATE INDEX IF NOT EXISTS idx_sigma_products_categories   ON sigma_products USING gin (categories);


CREATE TABLE IF NOT EXISTS sigma_history (
    history_id    bigserial PRIMARY KEY,

    -- Deliberately WITHOUT a foreign key to sigma_products: the history has to
    -- outlive a product's removal from the active table.
    product_id    integer NOT NULL,

    --   new      - first time the product is seen;
    --   changed  - any stored field differs from the previous version;
    --   returned - back in the catalogue after having been removed;
    --   gone     - no longer in the catalogue; the row holds its last state.
    event         text NOT NULL CHECK (event IN ('new', 'changed', 'returned', 'gone')),

    -- The fields a history is usually asked about, as real columns; the rest
    -- is in the snapshot.
    sku           text,
    price         numeric(14, 2),
    regular_price numeric(14, 2),
    sale_price    numeric(14, 2),
    on_sale       boolean,
    in_stock      boolean,
    stock_qty     integer,

    -- The whole row at that moment: sigma_products will keep changing shape,
    -- and history cannot be rewritten after the fact.
    snapshot      jsonb NOT NULL,

    -- NOT unique per product, unlike car_history: stock goes 5 -> 4 -> 5 all the
    -- time, and the return to an earlier state is a version like any other.
    data_hash     bytea NOT NULL,
    recorded_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sigma_history_product  ON sigma_history (product_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_sigma_history_recorded ON sigma_history (recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_sigma_history_event    ON sigma_history (event);
