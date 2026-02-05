"""
Bulk File Builder
Takes a list of approved suggestion actions and produces an Amazon-format XLSX.
The output file has the same sheet structure as the Amazon download.
"""

import json, os, uuid, logging
from datetime import datetime
from typing import Dict, List

import pandas as pd
from psycopg.types.json import Jsonb
from ..db_connection import get_conn
from ..config import Config

logger = logging.getLogger(__name__)

# ── SP Campaigns sheet columns (exact Amazon order) ──────────
SP_COLS = [
    "Product", "Entity", "Operation",
    "Campaign ID", "Ad Group ID", "Portfolio ID", "Ad ID",
    "Keyword ID", "Product Targeting ID",
    "Campaign Name", "Ad Group Name",
    "Campaign Name (Informational only)", "Ad Group Name (Informational only)",
    "Portfolio Name (Informational only)",
    "Start Date", "End Date", "Targeting Type", "State",
    "Campaign State (Informational only)", "Ad Group State (Informational only)",
    "Daily Budget", "SKU", "ASIN (Informational only)",
    "Eligibility Status (Informational only)",
    "Reason for Ineligibility (Informational only)",
    "Ad Group Default Bid", "Ad Group Default Bid (Informational only)",
    "Bid", "Keyword Text", "Native Language Keyword", "Native Language Locale",
    "Match Type", "Bidding Strategy", "Placement", "Percentage",
    "Product Targeting Expression",
    "Resolved Product Targeting Expression (Informational only)",
    "Audience ID", "Shopper Cohort Percentage", "Shopper Cohort Type",
    "Segment Name",
    "Impressions", "Clicks", "Click-through Rate",
    "Spend", "Sales", "Orders", "Units",
    "Conversion Rate", "ACOS", "CPC", "ROAS",
    "__account_id_note",
]


def build_xlsx(actions: List[Dict], account_id: str = "") -> Dict:
    """
    Convert a list of action dicts into an Amazon-format XLSX file.
    Returns {"success": bool, "job_id": str, "filepath": str, "filename": str, "summary": dict}
    """
    try:
        rows = []
        for act in actions:
            row = {}
            for col in SP_COLS:
                row[col] = act.get(col, "")
            # Default Product to Sponsored Products if not set
            if not row["Product"]:
                row["Product"] = "Sponsored Products"
            rows.append(row)

        df = pd.DataFrame(rows, columns=SP_COLS)

        job_id = str(uuid.uuid4())
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"bulk_{ts}_{job_id[:8]}.xlsx"
        outdir = Config.BULK_OUTPUT_FOLDER
        os.makedirs(outdir, exist_ok=True)
        filepath = os.path.join(outdir, filename)

        with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Sponsored Products Campaigns", index=False)

        # Summary
        summary = {
            "total_actions": len(rows),
            "by_entity": {},
            "by_operation": {},
        }
        for r in rows:
            e = r.get("Entity", "?")
            o = r.get("Operation", "?")
            summary["by_entity"][e] = summary["by_entity"].get(e, 0) + 1
            summary["by_operation"][o] = summary["by_operation"].get(o, 0) + 1

        # Save job record
        try:
            with get_conn() as conn:
                conn.execute(
                    """INSERT INTO bulk_jobs
                       (id, account_id, status, summary_json, output_file_path)
                       VALUES (%(id)s, %(aid)s, 'generated', %(s)s, %(p)s)""",
                    {"id": job_id, "aid": account_id, "s": Jsonb(summary), "p": filepath},
                )
        except Exception:
            logger.exception("Failed to save bulk job record")

        return {"success": True, "job_id": job_id, "filepath": filepath,
                "filename": filename, "summary": summary}

    except Exception as e:
        logger.exception("Bulk build failed")
        return {"success": False, "error": str(e)}


def list_jobs(limit: int = 30) -> List[Dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM bulk_jobs ORDER BY created_at DESC LIMIT %(l)s",
            {"l": limit},
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["id"] = str(d["id"])
        d["created_at"] = d["created_at"].isoformat() if d["created_at"] else None
        if d.get("date_from"):
            d["date_from"] = d["date_from"].isoformat()
        if d.get("date_to"):
            d["date_to"] = d["date_to"].isoformat()
        out.append(d)
    return out
