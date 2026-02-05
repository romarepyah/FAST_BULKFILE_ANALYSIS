"""Vercel entry point - Fast Analysis only."""

import os
import sys

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))

from flask import Flask, render_template, request, jsonify, Response
from werkzeug.utils import secure_filename

# Create Flask app
app = Flask(__name__,
            template_folder='app/templates',
            static_folder='app/static')
app.config['UPLOAD_FOLDER'] = '/tmp'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max

# Import services
from app.services import fast_analysis, adv_suggestions


# ── Pages ────────────────────────────────────────────────────
@app.route("/")
def index():
    """Redirect to fast analysis page."""
    return render_template("fast_analysis.html")


@app.route("/fast-analysis")
def fast_analysis_page():
    """Fast Bulk File Analysis page."""
    return render_template("fast_analysis.html")


# ── API: Fast Bulk Analysis ──────────────────────────────────
@app.route("/api/fast-analysis", methods=["POST"])
def fast_analysis_upload():
    """Upload and analyze bulk file."""
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify(success=False, error="No file"), 400

    name = secure_filename(f.filename)
    if not (name.lower().endswith(".xlsx") or name.lower().endswith(".xls")):
        return jsonify(success=False, error="Only .xlsx and .xls files supported"), 400

    path = os.path.join(app.config['UPLOAD_FOLDER'], "fa_" + name)
    f.save(path)

    try:
        result = fast_analysis.analyze_bulk_file(path)
        return jsonify(success=True, analysis=result)
    finally:
        if os.path.exists(path):
            os.remove(path)


# ── API: Advanced Suggestions (for Fast Analysis) ───────────
@app.route("/api/fast-analysis/suggestions", methods=["POST"])
def fast_analysis_suggestions():
    """Generate optimization suggestions."""
    body = request.get_json(force=True)
    analysis_data = body.get("analysis")
    if not analysis_data:
        return jsonify(success=False, error="No analysis data"), 400

    thresholds = body.get("thresholds", {})
    sugs = adv_suggestions.generate_suggestions(analysis_data, thresholds)
    return jsonify(success=True, suggestions=sugs)


@app.route("/api/fast-analysis/bulk-csv", methods=["POST"])
def fast_analysis_bulk_csv():
    """Generate bulk CSV/XLSX file."""
    body = request.get_json(force=True)
    selected = body.get("suggestions", [])
    if not selected:
        return jsonify(success=False, error="No suggestions selected"), 400

    xlsx_bytes = adv_suggestions.build_bulk_xlsx(selected)
    return Response(
        xlsx_bytes,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=bulk_actions.xlsx",
        },
    )


# Error handler
@app.errorhandler(Exception)
def handle_error(e):
    app.logger.exception("Error occurred")
    return jsonify(success=False, error=str(e)), 500


# For local development
if __name__ == "__main__":
    app.run(debug=True)
