-- ============================================================
-- PPC Dashboard Schema  –  single migration file
-- ============================================================

-- 1. ppc_raw  – one row per report line
CREATE TABLE IF NOT EXISTS ppc_raw (
    id              BIGSERIAL PRIMARY KEY,

    -- calendar
    date            DATE        NOT NULL,
    week            INT,
    month           INT,
    year            INT,
    hour            INT,

    -- account / entity
    budget_currency           TEXT,
    advertiser_account_name   TEXT,
    advertiser_account_id     TEXT NOT NULL,
    ad_product                TEXT,
    portfolio_id              BIGINT,
    portfolio_name            TEXT,
    campaign_id               BIGINT,
    campaign_name             TEXT,
    campaign_budget_amount    NUMERIC,
    campaign_bid_strategy     TEXT,
    campaign_rule_amount      NUMERIC,
    campaign_cost_type        TEXT,
    campaign_delivery_status  TEXT,
    ad_group_id               BIGINT,
    ad_group_name             TEXT,
    ad_group_delivery_status  TEXT,
    advertised_product_id     TEXT,
    advertised_product_sku    TEXT,

    -- placement / targeting
    placement_name            TEXT,
    placement_size            TEXT,
    site_or_app               TEXT,
    placement_classification  TEXT,
    target_value              TEXT,
    target_match_type         TEXT,
    search_term               TEXT,
    matched_target            TEXT,

    -- performance
    impressions               BIGINT  DEFAULT 0,
    invalid_impression_rate   NUMERIC,
    clicks                    BIGINT  DEFAULT 0,
    invalid_clicks            BIGINT  DEFAULT 0,
    viewable_ctr_vctr         NUMERIC,
    ctr                       NUMERIC,
    cpc                       NUMERIC,
    viewable_rate             NUMERIC,
    viewable_impressions      BIGINT,
    total_cost                NUMERIC DEFAULT 0,

    -- all-views
    purchases_all_views                    BIGINT  DEFAULT 0,
    sales_all_views                        NUMERIC DEFAULT 0,
    units_sold_all_views                   BIGINT,
    cost_per_purchase_all_views            NUMERIC,
    purchase_rate_all_views                NUMERIC,
    purchase_rate_over_clicks_all_views    NUMERIC,
    roas_all_views                         NUMERIC,

    -- new-to-brand
    purchases_new_to_brand      BIGINT,
    purchase_rate_new_to_brand  NUMERIC,

    -- funnel
    detail_page_views           BIGINT,
    purchases_halo_all_views    BIGINT,
    sales_halo_all_views        NUMERIC,
    units_sold_halo_all_views   BIGINT,
    detail_page_view_rate       NUMERIC,
    add_to_cart                 BIGINT,
    add_to_list                 BIGINT,
    long_term_sales             NUMERIC,
    long_term_roas              NUMERIC,

    -- combined
    purchases_combined              BIGINT,
    roas_combined                   NUMERIC,
    roas_from_clicks_combined       NUMERIC,
    sales_combined                  NUMERIC,
    units_sold_combined             BIGINT,
    cost_per_purchase_combined      NUMERIC,
    purchase_rate_combined          NUMERIC,

    -- metadata
    source_file_name    TEXT    NOT NULL,
    source_file_hash    TEXT    NOT NULL,
    row_signature       TEXT    NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Unique constraint for upsert
CREATE UNIQUE INDEX IF NOT EXISTS uq_ppc_raw_signature ON ppc_raw (row_signature);

-- Query indexes
CREATE INDEX IF NOT EXISTS idx_ppc_account_date ON ppc_raw (advertiser_account_id, date);
CREATE INDEX IF NOT EXISTS idx_ppc_campaign_date ON ppc_raw (campaign_id, date);
CREATE INDEX IF NOT EXISTS idx_ppc_portfolio_date ON ppc_raw (portfolio_id, date);
CREATE INDEX IF NOT EXISTS idx_ppc_adgroup_date  ON ppc_raw (ad_group_id, date);


-- 2. rule_sets
CREATE TABLE IF NOT EXISTS rule_sets (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT UNIQUE NOT NULL,
    description TEXT,
    rules_json  JSONB NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);


-- 3. bulk_jobs
CREATE TABLE IF NOT EXISTS bulk_jobs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    account_id       TEXT,
    date_from        DATE,
    date_to          DATE,
    rule_set_id      UUID REFERENCES rule_sets(id),
    status           TEXT DEFAULT 'created',
    summary_json     JSONB,
    output_file_path TEXT
);


-- 4. Seed default rule set  (Balanced)
INSERT INTO rule_sets (name, description, rules_json)
VALUES (
    'Balanced',
    'Default balanced optimization rules',
    '{
        "negative_rules": [
            {"name": "No sales high clicks",   "min_clicks": 20, "max_orders": 0, "min_spend": 0},
            {"name": "High ACOS high spend",    "min_acos": 100,  "min_spend": 15, "min_clicks": 5}
        ],
        "harvest_rules": [
            {"name": "Profitable search terms", "min_orders": 2,  "max_acos": 40}
        ],
        "budget_rules": [
            {"name": "Scale winners",           "min_roas": 3.0,  "min_spend": 20, "percentage": 25},
            {"name": "Cut losers",              "max_roas": 0.5,  "min_spend": 20, "percentage": -30}
        ]
    }'::JSONB
)
ON CONFLICT (name) DO NOTHING;
