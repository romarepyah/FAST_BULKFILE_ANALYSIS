"""
Suggestion Engine
Analyses ppc_raw data and returns actionable optimization suggestions.
Each suggestion maps directly to one or more bulk-file rows.
"""

import logging
from datetime import date
from typing import Dict, List
from ..db_connection import get_conn

logger = logging.getLogger(__name__)

# ── Every suggestion is a dict with:
#   id         – stable hashable key
#   category   – widget category (for grouping in UI)
#   severity   – high / medium / low  (for sorting)
#   title      – human-readable one-liner
#   detail     – extended explanation
#   metrics    – dict of relevant numbers
#   actions    – list of bulk-row dicts (ready for bulk builder)
# ──────────────────────────────────────────────────────────────


def generate_all(
    date_from: date,
    date_to: date,
    account_id: str | None = None,
    portfolio_id: int | None = None,
) -> List[Dict]:
    """Run every suggestion generator and merge results."""
    suggestions: List[Dict] = []
    data = _load_agg(date_from, date_to, account_id, portfolio_id)
    camp_data = _load_campaign_agg(date_from, date_to, account_id, portfolio_id)
    st_data = _load_search_term_agg(date_from, date_to, account_id, portfolio_id)

    suggestions += _pause_bad_campaigns(camp_data)
    suggestions += _low_ctr_campaigns(camp_data)
    suggestions += _increase_budget_winners(camp_data)
    suggestions += _decrease_budget_losers(camp_data)
    suggestions += _pause_bad_targets(data)
    suggestions += _increase_bid_low_traffic(data)
    suggestions += _decrease_bid_high_acos(data)
    suggestions += _harvest_search_terms(st_data)
    suggestions += _negative_search_terms(st_data)
    suggestions += _cross_negative(date_from, date_to, account_id, portfolio_id)
    suggestions += _pause_zero_impression_targets(data)
    suggestions += _high_spend_no_sales_targets(data)
    suggestions += _low_conversion_rate(data)
    suggestions += _top_of_search_opportunity(camp_data)

    # assign stable IDs
    for i, s in enumerate(suggestions):
        s["id"] = f"sug_{i}_{hash(s['title']) % 100000}"

    return suggestions


# ── Data loaders ─────────────────────────────────────────────

def _where(account_id, portfolio_id):
    w = []
    p: dict = {}
    if account_id:
        w.append("advertiser_account_id = %(aid)s")
        p["aid"] = account_id
    if portfolio_id:
        w.append("portfolio_id = %(pid)s")
        p["pid"] = portfolio_id
    return w, p


def _load_agg(d0, d1, account_id, portfolio_id):
    """Per target-level aggregation."""
    extra_w, params = _where(account_id, portfolio_id)
    params["d0"] = d0
    params["d1"] = d1
    w = " AND ".join(["date BETWEEN %(d0)s AND %(d1)s"] + extra_w)
    sql = f"""
        SELECT
            advertiser_account_id, portfolio_id, portfolio_name,
            campaign_id, campaign_name, ad_group_id, ad_group_name,
            target_value, target_match_type, ad_product,
            SUM(COALESCE(impressions,0)) AS impressions,
            SUM(COALESCE(clicks,0)) AS clicks,
            SUM(COALESCE(total_cost,0)) AS spend,
            SUM(COALESCE(sales_combined, sales_all_views,0)) AS sales,
            SUM(COALESCE(purchases_combined, purchases_all_views,0)) AS orders
        FROM ppc_raw WHERE {w}
        GROUP BY 1,2,3,4,5,6,7,8,9,10
    """
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _load_campaign_agg(d0, d1, account_id, portfolio_id):
    extra_w, params = _where(account_id, portfolio_id)
    params["d0"] = d0
    params["d1"] = d1
    w = " AND ".join(["date BETWEEN %(d0)s AND %(d1)s"] + extra_w)
    sql = f"""
        SELECT
            advertiser_account_id, portfolio_id, portfolio_name,
            campaign_id, campaign_name,
            MAX(campaign_budget_amount) AS budget,
            MAX(ad_product) AS ad_product,
            SUM(COALESCE(impressions,0)) AS impressions,
            SUM(COALESCE(clicks,0)) AS clicks,
            SUM(COALESCE(total_cost,0)) AS spend,
            SUM(COALESCE(sales_combined, sales_all_views,0)) AS sales,
            SUM(COALESCE(purchases_combined, purchases_all_views,0)) AS orders
        FROM ppc_raw WHERE {w}
        GROUP BY 1,2,3,4,5
    """
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _load_search_term_agg(d0, d1, account_id, portfolio_id):
    extra_w, params = _where(account_id, portfolio_id)
    params["d0"] = d0
    params["d1"] = d1
    w = " AND ".join(["date BETWEEN %(d0)s AND %(d1)s",
                       "search_term IS NOT NULL", "search_term != ''"] + extra_w)
    sql = f"""
        SELECT
            advertiser_account_id, portfolio_id, portfolio_name,
            campaign_id, campaign_name, ad_group_id, ad_group_name,
            search_term, target_match_type, target_value, ad_product,
            SUM(COALESCE(impressions,0)) AS impressions,
            SUM(COALESCE(clicks,0)) AS clicks,
            SUM(COALESCE(total_cost,0)) AS spend,
            SUM(COALESCE(sales_combined, sales_all_views,0)) AS sales,
            SUM(COALESCE(purchases_combined, purchases_all_views,0)) AS orders
        FROM ppc_raw WHERE {w}
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11
    """
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


# ── helpers ──────────────────────────────────────────────────
def _acos(spend, sales):
    return round(spend / sales * 100, 1) if sales else None

def _ctr(clicks, impr):
    return round(clicks / impr * 100, 2) if impr else None

def _cvr(orders, clicks):
    return round(orders / clicks * 100, 2) if clicks else None


# ══════════════════════════════════════════════════════════════
# SUGGESTION GENERATORS
# ══════════════════════════════════════════════════════════════

def _pause_bad_campaigns(data) -> list:
    """Campaigns with high spend, zero or very low orders, ACOS > 100%."""
    out = []
    for r in data:
        spend, sales, orders = float(r["spend"]), float(r["sales"]), int(r["orders"])
        if spend < 15:
            continue
        acos = _acos(spend, sales)
        if (orders == 0 and spend >= 20) or (acos and acos > 150):
            out.append({
                "category": "Pause Bad Campaigns",
                "severity": "high",
                "title": f"Pause campaign '{r['campaign_name']}' — "
                         f"${spend:.0f} spend, {orders} orders"
                         + (f", ACOS {acos:.0f}%" if acos else ""),
                "detail": "This campaign is unprofitable. Consider pausing it.",
                "metrics": {"spend": spend, "sales": sales, "orders": orders, "acos": acos},
                "actions": [{
                    "Product": "Sponsored Products",
                    "Entity": "Campaign",
                    "Operation": "update",
                    "Campaign ID": r["campaign_id"],
                    "Campaign Name": r["campaign_name"],
                    "State": "paused",
                    "__account_id_note": r["advertiser_account_id"],
                }],
            })
    return out


def _low_ctr_campaigns(data) -> list:
    """Campaigns with many impressions but CTR below 0.15%."""
    out = []
    for r in data:
        impr, clicks = int(r["impressions"]), int(r["clicks"])
        if impr < 1000:
            continue
        ctr = _ctr(clicks, impr)
        if ctr is not None and ctr < 0.15:
            out.append({
                "category": "Low CTR Campaigns",
                "severity": "medium",
                "title": f"Review '{r['campaign_name']}' — CTR {ctr:.2f}% ({impr:,} impr)",
                "detail": "Very low click-through rate. Review ad copy, targeting, or creative.",
                "metrics": {"impressions": impr, "clicks": clicks, "ctr": ctr},
                "actions": [],  # informational — user decides what to do
            })
    return out


def _increase_budget_winners(data) -> list:
    """Profitable campaigns that may be budget-limited."""
    out = []
    for r in data:
        spend, sales, budget = float(r["spend"]), float(r["sales"]), float(r.get("budget") or 0)
        orders = int(r["orders"])
        if spend < 10 or sales == 0 or budget == 0:
            continue
        acos = _acos(spend, sales)
        if acos and acos < 35 and orders >= 3:
            new_budget = round(budget * 1.3, 2)
            out.append({
                "category": "Increase Budget",
                "severity": "medium",
                "title": f"Increase budget for '{r['campaign_name']}' — "
                         f"ACOS {acos:.0f}%, {orders} orders",
                "detail": f"Profitable campaign. Suggested budget ${budget:.2f} → ${new_budget:.2f} (+30%).",
                "metrics": {"spend": spend, "sales": sales, "orders": orders, "acos": acos,
                            "current_budget": budget, "new_budget": new_budget},
                "actions": [{
                    "Product": "Sponsored Products",
                    "Entity": "Campaign",
                    "Operation": "update",
                    "Campaign ID": r["campaign_id"],
                    "Campaign Name": r["campaign_name"],
                    "Daily Budget": new_budget,
                    "__account_id_note": r["advertiser_account_id"],
                }],
            })
    return out


def _decrease_budget_losers(data) -> list:
    """Campaigns spending a lot with bad ACOS."""
    out = []
    for r in data:
        spend, sales, budget = float(r["spend"]), float(r["sales"]), float(r.get("budget") or 0)
        if spend < 10 or budget == 0:
            continue
        acos = _acos(spend, sales)
        if acos and acos > 80:
            new_budget = round(budget * 0.7, 2)
            out.append({
                "category": "Decrease Budget",
                "severity": "medium",
                "title": f"Cut budget for '{r['campaign_name']}' — ACOS {acos:.0f}%",
                "detail": f"Unprofitable. Suggested ${budget:.2f} → ${new_budget:.2f} (-30%).",
                "metrics": {"spend": spend, "sales": sales, "acos": acos,
                            "current_budget": budget, "new_budget": new_budget},
                "actions": [{
                    "Product": "Sponsored Products",
                    "Entity": "Campaign",
                    "Operation": "update",
                    "Campaign ID": r["campaign_id"],
                    "Campaign Name": r["campaign_name"],
                    "Daily Budget": new_budget,
                    "__account_id_note": r["advertiser_account_id"],
                }],
            })
    return out


def _pause_bad_targets(data) -> list:
    """Targets with high clicks but zero orders."""
    out = []
    for r in data:
        clicks, orders, spend = int(r["clicks"]), int(r["orders"]), float(r["spend"])
        tv = r.get("target_value") or ""
        if not tv or clicks < 15:
            continue
        if orders == 0 and spend >= 10:
            is_asin = "asin" in tv.lower() or tv.startswith("B0")
            entity = "Product Targeting" if is_asin else "Keyword"
            out.append({
                "category": "Pause Bad Targets",
                "severity": "high",
                "title": f"Pause target '{tv[:50]}' in '{r['campaign_name'][:30]}' — "
                         f"{clicks} clicks, 0 orders, ${spend:.2f}",
                "detail": "This target is wasting spend with no conversions.",
                "metrics": {"clicks": clicks, "spend": spend, "orders": 0, "target": tv},
                "actions": [{
                    "Product": "Sponsored Products",
                    "Entity": entity,
                    "Operation": "update",
                    "Campaign ID": r["campaign_id"],
                    "Ad Group ID": r["ad_group_id"],
                    "State": "paused",
                    "Keyword Text": tv if not is_asin else "",
                    "Product Targeting Expression": tv if is_asin else "",
                    "Match Type": r.get("target_match_type", ""),
                    "__account_id_note": r["advertiser_account_id"],
                }],
            })
    return out


def _increase_bid_low_traffic(data) -> list:
    """Targets with very few impressions that could benefit from higher bids."""
    out = []
    for r in data:
        impr, clicks, orders = int(r["impressions"]), int(r["clicks"]), int(r["orders"])
        spend, sales = float(r["spend"]), float(r["sales"])
        tv = r.get("target_value") or ""
        if not tv or impr > 200 or impr < 10:
            continue
        # only suggest if the target is converting
        if orders >= 1 and sales > spend:
            out.append({
                "category": "Increase Bids (Low Traffic)",
                "severity": "low",
                "title": f"Increase bid for '{tv[:50]}' — only {impr} impr but {orders} orders",
                "detail": "This target converts but gets very little traffic. A higher bid could help.",
                "metrics": {"impressions": impr, "clicks": clicks, "orders": orders, "spend": spend},
                "actions": [],  # user sets bid manually — we include it informational
            })
    return out


def _decrease_bid_high_acos(data) -> list:
    """Targets with ACOS > 60% that should have bids reduced."""
    out = []
    for r in data:
        clicks, orders, spend, sales = int(r["clicks"]), int(r["orders"]), float(r["spend"]), float(r["sales"])
        tv = r.get("target_value") or ""
        if not tv or clicks < 10 or sales == 0:
            continue
        acos = _acos(spend, sales)
        if acos and acos > 60:
            out.append({
                "category": "Decrease Bids (High ACOS)",
                "severity": "medium",
                "title": f"Lower bid for '{tv[:50]}' — ACOS {acos:.0f}%, ${spend:.2f} spend",
                "detail": "Target is converting but not profitably. Reduce bid to lower ACOS.",
                "metrics": {"clicks": clicks, "orders": orders, "spend": spend, "acos": acos},
                "actions": [],
            })
    return out


def _harvest_search_terms(st_data) -> list:
    """Search terms with good performance → create exact keyword."""
    out = []
    for r in st_data:
        orders, spend, sales = int(r["orders"]), float(r["spend"]), float(r["sales"])
        mt = (r.get("target_match_type") or "").lower()
        st = r.get("search_term") or ""
        if not st or orders < 2 or mt == "exact":
            continue
        acos = _acos(spend, sales)
        if acos and acos < 40:
            out.append({
                "category": "Harvest Search Terms",
                "severity": "medium",
                "title": f"Harvest '{st[:50]}' → exact keyword — {orders} orders, ACOS {acos:.0f}%",
                "detail": "This search term converts well. Create an exact-match keyword for it.",
                "metrics": {"search_term": st, "orders": orders, "acos": acos, "spend": spend, "sales": sales},
                "actions": [{
                    "Product": "Sponsored Products",
                    "Entity": "Keyword",
                    "Operation": "create",
                    "Campaign ID": r["campaign_id"],
                    "Ad Group ID": r["ad_group_id"],
                    "Campaign Name": r["campaign_name"],
                    "Ad Group Name": r["ad_group_name"],
                    "Keyword Text": st,
                    "Match Type": "Exact",
                    "State": "enabled",
                    "Bid": round(spend / int(r["clicks"]) * 1.1, 2) if int(r["clicks"]) > 0 else 0.50,
                    "__account_id_note": r["advertiser_account_id"],
                }],
            })
    return out


def _negative_search_terms(st_data) -> list:
    """Search terms wasting spend with no conversions → add negative."""
    out = []
    for r in st_data:
        clicks, orders, spend = int(r["clicks"]), int(r["orders"]), float(r["spend"])
        st = r.get("search_term") or ""
        if not st or clicks < 10:
            continue
        if orders == 0 and spend >= 5:
            out.append({
                "category": "Negative Search Terms",
                "severity": "high",
                "title": f"Negative '{st[:50]}' in '{r['campaign_name'][:30]}' — "
                         f"{clicks} clicks, 0 orders, ${spend:.2f}",
                "detail": "This search term wastes spend. Add as negative exact.",
                "metrics": {"search_term": st, "clicks": clicks, "spend": spend},
                "actions": [{
                    "Product": "Sponsored Products",
                    "Entity": "Campaign Negative Keyword",
                    "Operation": "create",
                    "Campaign ID": r["campaign_id"],
                    "Campaign Name": r["campaign_name"],
                    "Keyword Text": st,
                    "Match Type": "Negative Exact",
                    "State": "enabled",
                    "__account_id_note": r["advertiser_account_id"],
                }],
            })
    return out


def _cross_negative(d0, d1, account_id, portfolio_id) -> list:
    """Search terms appearing in multiple campaigns within same portfolio."""
    extra_w, params = _where(account_id, portfolio_id)
    params["d0"] = d0
    params["d1"] = d1
    w = " AND ".join(["date BETWEEN %(d0)s AND %(d1)s",
                       "search_term IS NOT NULL", "search_term != ''",
                       "portfolio_id IS NOT NULL"] + extra_w)
    sql = f"""
        WITH st AS (
            SELECT
                portfolio_id, portfolio_name, search_term,
                campaign_id, campaign_name, target_match_type,
                SUM(COALESCE(total_cost,0)) AS spend,
                SUM(COALESCE(clicks,0)) AS clicks,
                SUM(COALESCE(purchases_combined, purchases_all_views,0)) AS orders,
                SUM(COALESCE(sales_combined, sales_all_views,0)) AS sales
            FROM ppc_raw WHERE {w}
            GROUP BY 1,2,3,4,5,6
        ),
        overlap AS (
            SELECT search_term, portfolio_id
            FROM st GROUP BY 1,2 HAVING COUNT(DISTINCT campaign_id)>1
        )
        SELECT st.* FROM st
        JOIN overlap o ON st.search_term=o.search_term AND st.portfolio_id=o.portfolio_id
        ORDER BY st.search_term, st.spend DESC
    """
    with get_conn() as conn:
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]

    # Group by (portfolio, search_term)
    groups: Dict[tuple, list] = {}
    for r in rows:
        key = (r["portfolio_id"], r["search_term"])
        groups.setdefault(key, []).append(r)

    out = []
    for (pid, term), camps in groups.items():
        if len(camps) < 2:
            continue
        # Owner = exact-match campaign, or highest orders/lowest ACOS
        exact = [c for c in camps if (c.get("target_match_type") or "").lower() == "exact"]
        owner = (sorted(exact, key=lambda x: (-int(x["orders"]), float(x["spend"])))[0]
                 if exact
                 else sorted(camps, key=lambda x: (-int(x["orders"]), float(x["spend"])))[0])

        for c in camps:
            if c["campaign_id"] == owner["campaign_id"]:
                continue
            spend, orders = float(c["spend"]), int(c["orders"])
            out.append({
                "category": "Cross-Campaign Negativing",
                "severity": "high" if spend > 10 else "medium",
                "title": (f"Cross-negative '{term[:40]}' in '{c['campaign_name'][:25]}' "
                          f"(owner: '{owner['campaign_name'][:25]}')"),
                "detail": (f"'{term}' appears in {len(camps)} campaigns in portfolio "
                           f"'{c.get('portfolio_name','')}'. "
                           f"Add negative in non-owner campaign to prevent cannibalisation."),
                "metrics": {"search_term": term, "spend": spend, "orders": orders,
                            "owner_campaign": owner["campaign_name"]},
                "actions": [{
                    "Product": "Sponsored Products",
                    "Entity": "Campaign Negative Keyword",
                    "Operation": "create",
                    "Campaign ID": c["campaign_id"],
                    "Campaign Name": c["campaign_name"],
                    "Keyword Text": term,
                    "Match Type": "Negative Exact",
                    "State": "enabled",
                    "__account_id_note": c.get("advertiser_account_id"),
                }],
            })
    return out


def _pause_zero_impression_targets(data) -> list:
    """Targets that haven't received any impressions over the period."""
    out = []
    for r in data:
        impr = int(r["impressions"])
        tv = r.get("target_value") or ""
        if not tv or impr > 0:
            continue
        out.append({
            "category": "Zero-Impression Targets",
            "severity": "low",
            "title": f"Target '{tv[:50]}' in '{r['campaign_name'][:30]}' — 0 impressions",
            "detail": "This target gets no traffic. Review relevancy or increase bid.",
            "metrics": {"target": tv, "impressions": 0},
            "actions": [],
        })
    return out


def _high_spend_no_sales_targets(data) -> list:
    """Targets with significant spend but zero sales."""
    out = []
    for r in data:
        spend, sales, clicks = float(r["spend"]), float(r["sales"]), int(r["clicks"])
        tv = r.get("target_value") or ""
        if not tv or spend < 20 or sales > 0 or clicks < 5:
            continue
        is_asin = "asin" in tv.lower() or tv.startswith("B0")
        out.append({
            "category": "High Spend No Sales",
            "severity": "high",
            "title": f"Pause '{tv[:50]}' — ${spend:.2f} spend, 0 sales",
            "detail": "Significant wasted spend with no sales.",
            "metrics": {"target": tv, "spend": spend, "clicks": clicks},
            "actions": [{
                "Product": "Sponsored Products",
                "Entity": "Product Targeting" if is_asin else "Keyword",
                "Operation": "update",
                "Campaign ID": r["campaign_id"],
                "Ad Group ID": r["ad_group_id"],
                "State": "paused",
                "Keyword Text": tv if not is_asin else "",
                "Product Targeting Expression": tv if is_asin else "",
                "__account_id_note": r["advertiser_account_id"],
            }],
        })
    return out


def _low_conversion_rate(data) -> list:
    """Targets with many clicks but very low conversion rate < 1%."""
    out = []
    for r in data:
        clicks, orders = int(r["clicks"]), int(r["orders"])
        tv = r.get("target_value") or ""
        if not tv or clicks < 30:
            continue
        cvr = _cvr(orders, clicks)
        if cvr is not None and cvr < 1.0 and orders > 0:
            out.append({
                "category": "Low Conversion Rate",
                "severity": "low",
                "title": f"Review '{tv[:50]}' — CVR {cvr:.1f}%, {clicks} clicks, {orders} orders",
                "detail": "Very low conversion rate. Check listing quality or reduce bid.",
                "metrics": {"target": tv, "clicks": clicks, "orders": orders, "cvr": cvr},
                "actions": [],
            })
    return out


def _top_of_search_opportunity(camp_data) -> list:
    """High-performing campaigns that might benefit from top-of-search placement boost."""
    out = []
    for r in camp_data:
        spend, sales, orders = float(r["spend"]), float(r["sales"]), int(r["orders"])
        if spend < 15 or sales == 0:
            continue
        acos = _acos(spend, sales)
        if acos and acos < 25 and orders >= 5:
            out.append({
                "category": "Top of Search Opportunity",
                "severity": "low",
                "title": f"Boost ToS for '{r['campaign_name'][:40]}' — ACOS {acos:.0f}%, {orders} orders",
                "detail": "Strong campaign. Consider increasing top-of-search placement %.",
                "metrics": {"acos": acos, "orders": orders, "spend": spend, "sales": sales},
                "actions": [{
                    "Product": "Sponsored Products",
                    "Entity": "Bidding Adjustment",
                    "Operation": "update",
                    "Campaign ID": r["campaign_id"],
                    "Campaign Name": r["campaign_name"],
                    "Placement": "Placement Top",
                    "Percentage": 50,
                    "__account_id_note": r["advertiser_account_id"],
                }],
            })
    return out
