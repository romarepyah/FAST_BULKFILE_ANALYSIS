"""Aggregation queries for dashboard charts and summary cards."""

import logging
from datetime import date, timedelta
from typing import Dict, List, Optional
from ..db_connection import get_conn

logger = logging.getLogger(__name__)


def daily_metrics(date_from: date, date_to: date, **filters) -> List[Dict]:
    """Return one row per day with computed KPIs."""
    wheres = ["date BETWEEN %(d0)s AND %(d1)s"]
    params: dict = {"d0": date_from, "d1": date_to}

    for col, key in [
        ("advertiser_account_id", "account_id"),
        ("portfolio_id", "portfolio_id"),
        ("campaign_id", "campaign_id"),
        ("ad_group_id", "ad_group_id"),
        ("ad_product", "ad_product"),
        ("placement_name", "placement"),
        ("target_match_type", "match_type"),
    ]:
        v = filters.get(key)
        if v:
            wheres.append(f"{col} = %({key})s")
            params[key] = v

    where = " AND ".join(wheres)

    sql = f"""
        SELECT
            date,
            SUM(COALESCE(impressions,0))                         AS impressions,
            SUM(COALESCE(clicks,0))                              AS clicks,
            SUM(COALESCE(total_cost,0))                          AS spend,
            SUM(COALESCE(sales_combined, sales_all_views, 0))    AS sales,
            SUM(COALESCE(purchases_combined, purchases_all_views, 0)) AS orders,
            CASE WHEN SUM(COALESCE(impressions,0))>0
                 THEN SUM(COALESCE(clicks,0))::numeric / SUM(impressions) * 100
                 ELSE 0 END                                      AS ctr,
            CASE WHEN SUM(COALESCE(clicks,0))>0
                 THEN SUM(COALESCE(total_cost,0))::numeric / SUM(clicks)
                 ELSE 0 END                                      AS cpc,
            CASE WHEN SUM(COALESCE(sales_combined, sales_all_views,0))>0
                 THEN SUM(COALESCE(total_cost,0))::numeric
                      / SUM(COALESCE(sales_combined, sales_all_views,0)) * 100
                 ELSE 0 END                                      AS acos,
            CASE WHEN SUM(COALESCE(total_cost,0))>0
                 THEN SUM(COALESCE(sales_combined, sales_all_views,0))::numeric
                      / SUM(total_cost)
                 ELSE 0 END                                      AS roas
        FROM ppc_raw
        WHERE {where}
        GROUP BY date ORDER BY date
    """
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    # serialise date
    out = []
    for r in rows:
        d = dict(r)
        d["date"] = d["date"].isoformat()
        for k in ("spend","sales","ctr","cpc","acos","roas"):
            if d.get(k) is not None:
                d[k] = round(float(d[k]), 2)
        for k in ("impressions","clicks","orders"):
            d[k] = int(d.get(k) or 0)
        out.append(d)
    return out


def summary_stats(date_from: date, date_to: date, **filters) -> Dict:
    wheres = ["date BETWEEN %(d0)s AND %(d1)s"]
    params: dict = {"d0": date_from, "d1": date_to}
    for col, key in [
        ("advertiser_account_id", "account_id"),
        ("portfolio_id", "portfolio_id"),
        ("campaign_id", "campaign_id"),
    ]:
        v = filters.get(key)
        if v:
            wheres.append(f"{col} = %({key})s")
            params[key] = v

    where = " AND ".join(wheres)
    sql = f"""
        SELECT
            SUM(COALESCE(impressions,0))  AS impressions,
            SUM(COALESCE(clicks,0))       AS clicks,
            SUM(COALESCE(total_cost,0))   AS spend,
            SUM(COALESCE(sales_combined, sales_all_views,0)) AS sales,
            SUM(COALESCE(purchases_combined, purchases_all_views,0)) AS orders
        FROM ppc_raw WHERE {where}
    """
    with get_conn() as conn:
        row = conn.execute(sql, params).fetchone()
    if not row:
        return {}
    d = dict(row)
    d["ctr"]  = round(d["clicks"]/d["impressions"]*100, 2) if d["impressions"] else 0
    d["cpc"]  = round(d["spend"]/d["clicks"], 2)           if d["clicks"]      else 0
    d["acos"] = round(d["spend"]/d["sales"]*100, 2)        if d["sales"]       else 0
    d["roas"] = round(d["sales"]/d["spend"], 2)            if d["spend"]       else 0
    for k in ("impressions","clicks","orders"):
        d[k] = int(d[k] or 0)
    for k in ("spend","sales","ctr","cpc","acos","roas"):
        d[k] = float(d.get(k) or 0)
    return d


def filter_options() -> Dict:
    """Distinct values for each filterable dimension."""
    out: Dict[str, list] = {}
    queries = {
        "accounts":   "SELECT DISTINCT advertiser_account_id AS id, advertiser_account_name AS name FROM ppc_raw WHERE advertiser_account_id IS NOT NULL ORDER BY 2",
        "portfolios":  "SELECT DISTINCT portfolio_id AS id, portfolio_name AS name FROM ppc_raw WHERE portfolio_id IS NOT NULL ORDER BY 2",
        "campaigns":   "SELECT DISTINCT campaign_id AS id, campaign_name AS name FROM ppc_raw WHERE campaign_id IS NOT NULL ORDER BY 2",
        "ad_products": "SELECT DISTINCT ad_product AS val FROM ppc_raw WHERE ad_product IS NOT NULL ORDER BY 1",
        "match_types": "SELECT DISTINCT target_match_type AS val FROM ppc_raw WHERE target_match_type IS NOT NULL ORDER BY 1",
    }
    with get_conn() as conn:
        for key, sql in queries.items():
            rows = conn.execute(sql).fetchall()
            if key in ("ad_products", "match_types"):
                out[key] = [r["val"] for r in rows]
            else:
                out[key] = [dict(r) for r in rows]
    return out


def date_range() -> Dict:
    with get_conn() as conn:
        r = conn.execute("SELECT MIN(date) AS mn, MAX(date) AS mx FROM ppc_raw").fetchone()
    if r and r["mn"]:
        return {"min": r["mn"].isoformat(), "max": r["mx"].isoformat()}
    return {"min": None, "max": None}
