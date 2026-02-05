"""CSV ingestion with flexible column mapping, chunked upsert, dedup."""

import hashlib, logging, os, io
from datetime import datetime
from typing import Dict, List, Tuple

import pandas as pd
from ..db_connection import get_conn

logger = logging.getLogger(__name__)

# ── Flexible header mapping ──────────────────────────────────────────
# Normalised (lower, stripped) CSV header  →  DB column
_MAP: Dict[str, str] = {}

_PAIRS = [
    ("budget currency",                         "budget_currency"),
    ("date",                                    "date"),
    ("week",                                    "week"),
    ("month",                                   "month"),
    ("year",                                    "year"),
    ("hour",                                    "hour"),
    ("advertiser account name",                 "advertiser_account_name"),
    ("advertiser account id",                   "advertiser_account_id"),
    ("ad product",                              "ad_product"),
    ("portfolio id",                            "portfolio_id"),
    ("portfolio name",                          "portfolio_name"),
    ("campaign id",                             "campaign_id"),
    ("campaign name",                           "campaign_name"),
    ("campaign budget amount",                  "campaign_budget_amount"),
    ("campaign bid strategy",                   "campaign_bid_strategy"),
    ("campaign rule amount",                    "campaign_rule_amount"),
    ("campaign cost type",                      "campaign_cost_type"),
    ("campaign delivery status",                "campaign_delivery_status"),
    ("ad group id",                             "ad_group_id"),
    ("ad group name",                           "ad_group_name"),
    ("ad group delivery status",                "ad_group_delivery_status"),
    ("advertised product id",                   "advertised_product_id"),
    ("advertised product sku",                  "advertised_product_sku"),
    ("placement name",                          "placement_name"),
    ("placement size",                          "placement_size"),
    ("site or app",                             "site_or_app"),
    ("placement classification",                "placement_classification"),
    ("target value",                            "target_value"),
    ("target match type",                       "target_match_type"),
    ("search term",                             "search_term"),
    ("matched target",                          "matched_target"),
    ("impressions",                             "impressions"),
    ("invalid impression rate",                 "invalid_impression_rate"),
    ("clicks",                                  "clicks"),
    ("invalid clicks",                          "invalid_clicks"),
    ("viewable ctr (vctr)",                     "viewable_ctr_vctr"),
    ("ctr",                                     "ctr"),
    ("cpc",                                     "cpc"),
    ("viewable rate",                           "viewable_rate"),
    ("viewable impressions",                    "viewable_impressions"),
    ("total cost",                              "total_cost"),
    ("purchases (all views)",                   "purchases_all_views"),
    ("sales (all views)",                       "sales_all_views"),
    ("units sold (all views)",                  "units_sold_all_views"),
    ("cost per purchase (all views)",           "cost_per_purchase_all_views"),
    ("purchase rate (all views)",               "purchase_rate_all_views"),
    ("purchase rate over clicks (all views)",   "purchase_rate_over_clicks_all_views"),
    ("roas (all views)",                        "roas_all_views"),
    ("purchases (new to brand)",                "purchases_new_to_brand"),
    ("purchase rate (new to brand)",            "purchase_rate_new_to_brand"),
    ("detail page views",                       "detail_page_views"),
    ("purchases (halo, all views)",             "purchases_halo_all_views"),
    ("purchases (halo all views)",              "purchases_halo_all_views"),
    ("sales (halo, all views)",                 "sales_halo_all_views"),
    ("sales (halo all views)",                  "sales_halo_all_views"),
    ("units sold (halo, all views)",            "units_sold_halo_all_views"),
    ("units sold (halo all views)",             "units_sold_halo_all_views"),
    ("detail page view rate",                   "detail_page_view_rate"),
    ("add to cart",                             "add_to_cart"),
    ("add to list",                             "add_to_list"),
    ("long-term sales",                         "long_term_sales"),
    ("long-term roas",                          "long_term_roas"),
    ("purchases (combined)",                    "purchases_combined"),
    ("roas (combined)",                         "roas_combined"),
    ("roas from clicks (combined)",             "roas_from_clicks_combined"),
    ("sales (combined)",                        "sales_combined"),
    ("units sold (combined)",                   "units_sold_combined"),
    ("cost per purchase (combined)",            "cost_per_purchase_combined"),
    ("purchase rate (combined)",                "purchase_rate_combined"),
]
for _csv, _db in _PAIRS:
    _MAP[_csv] = _db

# All valid DB columns for ppc_raw (excluding id)
DB_COLS = sorted({v for v in _MAP.values()}) + [
    "source_file_name", "source_file_hash", "row_signature", "ingested_at",
]

# Signature key fields (order matters)
_SIG_FIELDS = [
    "date", "hour", "advertiser_account_id", "ad_product",
    "portfolio_id", "campaign_id", "ad_group_id",
    "advertised_product_id", "advertised_product_sku",
    "placement_name", "placement_classification",
    "target_value", "target_match_type", "search_term", "matched_target",
]


def _file_hash(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _row_sig(row: dict) -> str:
    parts = []
    for f in _SIG_FIELDS:
        v = row.get(f)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            parts.append("")
        else:
            parts.append(str(v).strip().lower())
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def _normalise_date(val):
    """Accept 'Jan 19, 2026' or '2026-01-19' etc."""
    if pd.isna(val):
        return None
    try:
        return pd.to_datetime(val).strftime("%Y-%m-%d")
    except Exception:
        return None


def _to_numeric(val):
    if pd.isna(val) or val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _to_int(val):
    n = _to_numeric(val)
    return int(n) if n is not None else None


def _to_bigint(val):
    """Handle bigint IDs – some may be strings in CSV."""
    if pd.isna(val) or val is None or val == "":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


# ── Public API ────────────────────────────────────────────────────────

def ingest_file(filepath: str, filename: str, batch_size: int = 500) -> Dict:
    """Parse CSV → upsert into ppc_raw. Returns summary dict."""
    result = dict(success=False, rows_parsed=0, rows_inserted=0,
                  rows_updated=0, errors=[], filename=filename)
    try:
        file_hash = _file_hash(filepath)
        df = pd.read_csv(filepath, low_memory=False)
        # Map headers
        rename = {}
        for col in df.columns:
            key = col.strip().lower()
            if key in _MAP:
                rename[col] = _MAP[key]
        df.rename(columns=rename, inplace=True)

        # Keep only known columns
        known = [c for c in df.columns if c in set(v for v in _MAP.values())]
        df = df[known].copy()

        if "date" not in df.columns or "advertiser_account_id" not in df.columns:
            result["errors"].append("Missing required column: date or advertiser_account_id")
            return result

        result["rows_parsed"] = len(df)

        # Type conversions
        df["date"] = df["date"].apply(_normalise_date)
        for c in ("week", "month", "year", "hour", "impressions", "clicks",
                   "invalid_clicks", "viewable_impressions",
                   "purchases_all_views", "units_sold_all_views",
                   "purchases_new_to_brand", "detail_page_views",
                   "purchases_halo_all_views", "units_sold_halo_all_views",
                   "add_to_cart", "add_to_list",
                   "purchases_combined", "units_sold_combined"):
            if c in df.columns:
                df[c] = df[c].apply(_to_int)

        for c in ("portfolio_id", "campaign_id", "ad_group_id"):
            if c in df.columns:
                df[c] = df[c].apply(_to_bigint)

        numeric_cols = [
            "campaign_budget_amount", "campaign_rule_amount",
            "invalid_impression_rate", "viewable_ctr_vctr", "ctr", "cpc",
            "viewable_rate", "total_cost",
            "sales_all_views", "cost_per_purchase_all_views",
            "purchase_rate_all_views", "purchase_rate_over_clicks_all_views",
            "roas_all_views", "purchase_rate_new_to_brand",
            "sales_halo_all_views", "detail_page_view_rate",
            "long_term_sales", "long_term_roas",
            "roas_combined", "roas_from_clicks_combined", "sales_combined",
            "cost_per_purchase_combined", "purchase_rate_combined",
        ]
        for c in numeric_cols:
            if c in df.columns:
                df[c] = df[c].apply(_to_numeric)

        # Metadata
        df["source_file_name"] = filename
        df["source_file_hash"] = file_hash
        df["ingested_at"] = datetime.utcnow()

        # Replace pandas NaN with None
        df = df.where(df.notna(), None)

        # Compute row signatures
        df["row_signature"] = df.apply(lambda r: _row_sig(r.to_dict()), axis=1)

        # Determine final column list
        all_db = set(v for v in _MAP.values()) | {
            "source_file_name", "source_file_hash", "row_signature", "ingested_at",
        }
        cols = [c for c in df.columns if c in all_db]

        # Build upsert SQL
        cols_sql = ", ".join(cols)
        placeholders = ", ".join([f"%({c})s" for c in cols])
        update_cols = [c for c in cols if c != "row_signature"]
        update_sql = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

        upsert = f"""
            INSERT INTO ppc_raw ({cols_sql}) VALUES ({placeholders})
            ON CONFLICT (row_signature) DO UPDATE SET {update_sql}
        """

        with get_conn() as conn:
            cur = conn.cursor()
            for start in range(0, len(df), batch_size):
                batch = df.iloc[start:start + batch_size]
                rows = batch[cols].to_dict("records")
                # Convert any remaining numpy types
                for row in rows:
                    for k, v in row.items():
                        if isinstance(v, (pd.Timestamp,)):
                            row[k] = v.isoformat()
                        elif hasattr(v, "item"):  # numpy scalar
                            row[k] = v.item()

                cur.executemany(upsert, rows)

            conn.commit()

        # Count inserted vs updated (approximate: we count all as inserted for simplicity,
        # but the ON CONFLICT handles the real dedup)
        result["rows_inserted"] = result["rows_parsed"]
        result["success"] = True

    except Exception as e:
        logger.exception("Ingestion error for %s", filename)
        result["errors"].append(str(e))

    return result
