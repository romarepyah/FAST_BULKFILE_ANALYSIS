"""Flask routes – pages + JSON API."""

import os, json, logging
from datetime import date, datetime, timedelta

from flask import Blueprint, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename

from .config import Config
from .services import (ingestion, aggregation, suggestions, bulk_builder,
                       fast_analysis, adv_suggestions)

logger = logging.getLogger(__name__)

pages = Blueprint("pages", __name__)
api   = Blueprint("api",   __name__, url_prefix="/api")


@api.errorhandler(Exception)
def api_error(e):
    logger.exception("API error")
    return jsonify(success=False, error=str(e)), 500


# ── Pages ────────────────────────────────────────────────────
@pages.route("/")
def dashboard():
    return render_template("dashboard.html")

@pages.route("/upload")
def upload_page():
    return render_template("upload.html")

@pages.route("/suggestions")
def suggestions_page():
    return render_template("suggestions.html")

@pages.route("/jobs")
def jobs_page():
    return render_template("jobs.html")

@pages.route("/fast-analysis")
def fast_analysis_page():
    return render_template("fast_analysis.html")


# ── API: Upload ──────────────────────────────────────────────
@api.route("/upload", methods=["POST"])
def upload_files():
    files = request.files.getlist("files[]")
    if not files:
        return jsonify(success=False, error="No files"), 400

    os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
    results = []
    for f in files:
        if not f.filename:
            continue
        name = secure_filename(f.filename)
        path = os.path.join(Config.UPLOAD_FOLDER, name)
        f.save(path)
        try:
            res = ingestion.ingest_file(path, name)
            results.append(res)
        finally:
            if os.path.exists(path):
                os.remove(path)

    total = dict(files=len(results),
                 rows_parsed=sum(r["rows_parsed"] for r in results),
                 rows_inserted=sum(r["rows_inserted"] for r in results),
                 errors=[e for r in results for e in r["errors"]])
    return jsonify(success=True, summary=total, details=results)


# ── API: Metrics ─────────────────────────────────────────────
@api.route("/metrics/daily")
def metrics_daily():
    d0 = request.args.get("date_from")
    d1 = request.args.get("date_to")
    if not d0 or not d1:
        d1 = date.today()
        d0 = d1 - timedelta(days=30)
    else:
        d0 = date.fromisoformat(d0)
        d1 = date.fromisoformat(d1)

    kw = {}
    for k in ("account_id", "portfolio_id", "campaign_id", "ad_group_id",
              "ad_product", "placement", "match_type"):
        v = request.args.get(k)
        if v:
            kw[k] = int(v) if k.endswith("_id") and k != "account_id" else v
    data = aggregation.daily_metrics(d0, d1, **kw)
    return jsonify(success=True, metrics=data)


@api.route("/metrics/summary")
def metrics_summary():
    d0 = request.args.get("date_from")
    d1 = request.args.get("date_to")
    if not d0 or not d1:
        d1 = date.today()
        d0 = d1 - timedelta(days=30)
    else:
        d0 = date.fromisoformat(d0)
        d1 = date.fromisoformat(d1)

    kw = {}
    for k in ("account_id", "portfolio_id", "campaign_id"):
        v = request.args.get(k)
        if v:
            kw[k] = v
    data = aggregation.summary_stats(d0, d1, **kw)
    return jsonify(success=True, summary=data)


@api.route("/metrics/filters")
def metrics_filters():
    f = aggregation.filter_options()
    dr = aggregation.date_range()
    return jsonify(success=True, filters=f, date_range=dr)


# ── API: Suggestions ─────────────────────────────────────────
@api.route("/suggestions")
def get_suggestions():
    d0 = request.args.get("date_from")
    d1 = request.args.get("date_to")
    if not d0 or not d1:
        d1 = date.today()
        d0 = d1 - timedelta(days=30)
    else:
        d0 = date.fromisoformat(d0)
        d1 = date.fromisoformat(d1)
    aid = request.args.get("account_id")
    pid = request.args.get("portfolio_id")
    if pid:
        pid = int(pid)
    data = suggestions.generate_all(d0, d1, aid, pid)
    return jsonify(success=True, suggestions=data)


# ── API: Bulk generate ───────────────────────────────────────
@api.route("/bulk/generate", methods=["POST"])
def bulk_generate():
    body = request.get_json(force=True)
    actions = body.get("actions", [])
    account_id = body.get("account_id", "")
    if not actions:
        return jsonify(success=False, error="No actions provided"), 400
    result = bulk_builder.build_xlsx(actions, account_id)
    return jsonify(result)


@api.route("/bulk/jobs")
def bulk_jobs():
    jobs = bulk_builder.list_jobs()
    return jsonify(success=True, jobs=jobs)


# ── API: Fast Bulk Analysis ──────────────────────────────────
@api.route("/fast-analysis", methods=["POST"])
def fast_analysis_upload():
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify(success=False, error="No file"), 400
    name = secure_filename(f.filename)
    if not (name.lower().endswith(".xlsx") or name.lower().endswith(".xls")):
        return jsonify(success=False, error="Only .xlsx and .xls files supported"), 400
    path = os.path.join(Config.UPLOAD_FOLDER, "fa_" + name)
    f.save(path)
    try:
        result = fast_analysis.analyze_bulk_file(path)
        return jsonify(success=True, analysis=result)
    finally:
        if os.path.exists(path):
            os.remove(path)


# ── API: Advanced Suggestions (for Fast Analysis) ───────────
@api.route("/fast-analysis/suggestions", methods=["POST"])
def fast_analysis_suggestions():
    body = request.get_json(force=True)
    analysis_data = body.get("analysis")
    if not analysis_data:
        return jsonify(success=False, error="No analysis data"), 400
    thresholds = body.get("thresholds", {})
    sugs = adv_suggestions.generate_suggestions(analysis_data, thresholds)
    return jsonify(success=True, suggestions=sugs)


@api.route("/fast-analysis/bulk-csv", methods=["POST"])
def fast_analysis_bulk_csv():
    body = request.get_json(force=True)
    selected = body.get("suggestions", [])
    if not selected:
        return jsonify(success=False, error="No suggestions selected"), 400
    xlsx_bytes = adv_suggestions.build_bulk_xlsx(selected)
    from flask import Response
    return Response(
        xlsx_bytes,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=bulk_actions.xlsx",
        },
    )


@api.route("/bulk/jobs/<job_id>/download")
def bulk_download(job_id):
    from .db_connection import get_conn
    with get_conn() as conn:
        row = conn.execute(
            "SELECT output_file_path FROM bulk_jobs WHERE id=%(id)s",
            {"id": job_id},
        ).fetchone()
    if not row or not row["output_file_path"]:
        return jsonify(success=False, error="Not found"), 404
    path = row["output_file_path"]
    if not os.path.exists(path):
        return jsonify(success=False, error="File missing"), 404
    return send_file(path, as_attachment=True)
