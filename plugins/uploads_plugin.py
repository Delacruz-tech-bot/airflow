import os
import math
from datetime import datetime
from airflow.plugins_manager import AirflowPlugin
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from flask import Blueprint, send_from_directory, request, flash, redirect, url_for
from werkzeug.utils import secure_filename

UPLOADS_DIR = os.path.join(os.path.expanduser("~"), "airflow", "uploads")
ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx"}
PER_PAGE = 10

# Ensure uploads directory exists
os.makedirs(UPLOADS_DIR, exist_ok=True)


class UploadsView(AppBuilderBaseView):
    default_view = "list_files"

    @expose("/", methods=["GET"])
    def list_files(self):
        files = []
        if os.path.isdir(UPLOADS_DIR):
            for name in os.listdir(UPLOADS_DIR):
                filepath = os.path.join(UPLOADS_DIR, name)
                if os.path.isfile(filepath):
                    size_bytes = os.path.getsize(filepath)
                    if size_bytes < 1024:
                        size_str = f"{size_bytes} B"
                    elif size_bytes < 1024 * 1024:
                        size_str = f"{size_bytes / 1024:.1f} KB"
                    else:
                        size_str = f"{size_bytes / (1024 * 1024):.1f} MB"
                    ctime = os.path.getctime(filepath)
                    created_dt = datetime.fromtimestamp(ctime)
                    created_str = created_dt.strftime("%Y-%m-%d %H:%M:%S")
                    files.append({
                        "name": name,
                        "size": size_str,
                        "size_bytes": size_bytes,
                        "created": created_str,
                        "created_ts": ctime,
                    })

        # Sorting
        sort_by = request.args.get("sort_by", "created")
        sort_dir = request.args.get("sort_dir", "desc")
        if sort_by not in ("name", "size", "created"):
            sort_by = "created"
        if sort_dir not in ("asc", "desc"):
            sort_dir = "desc"

        sort_key_map = {
            "name": lambda f: f["name"].lower(),
            "size": lambda f: f["size_bytes"],
            "created": lambda f: f["created_ts"],
        }
        files.sort(key=sort_key_map[sort_by], reverse=(sort_dir == "desc"))

        total_files = len(files)
        total_pages = max(1, math.ceil(total_files / PER_PAGE))
        page = request.args.get("page", 1, type=int)
        page = max(1, min(page, total_pages))
        start = (page - 1) * PER_PAGE
        end = start + PER_PAGE
        page_files = files[start:end]

        return self.render_template(
            "uploads_plugin/list.html",
            files=page_files,
            page=page,
            total_pages=total_pages,
            total_files=total_files,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

    @expose("/upload", methods=["POST"])
    def upload_file(self):
        if "file" not in request.files:
            flash("No file selected.", "warning")
            return redirect(url_for("UploadsView.list_files"))

        uploaded = request.files["file"]
        if uploaded.filename == "":
            flash("No file selected.", "warning")
            return redirect(url_for("UploadsView.list_files"))

        filename = secure_filename(uploaded.filename)
        if not filename:
            flash("Invalid filename.", "danger")
            return redirect(url_for("UploadsView.list_files"))

        ext = os.path.splitext(filename)[1].lower()
        if ext not in ALLOWED_EXTENSIONS:
            allowed = ", ".join(sorted(ALLOWED_EXTENSIONS))
            flash(f"File type '{ext}' not allowed. Accepted types: {allowed}", "danger")
            return redirect(url_for("UploadsView.list_files"))

        uploaded.save(os.path.join(UPLOADS_DIR, filename))
        flash(f"File '{filename}' uploaded successfully.", "success")
        return redirect(url_for("UploadsView.list_files"))

    @expose("/preview/<filename>")
    def preview_file(self, filename):
        import pandas as pd

        safe_name = secure_filename(filename)
        filepath = os.path.join(UPLOADS_DIR, safe_name)
        if not os.path.isfile(filepath):
            flash(f"File '{filename}' not found.", "warning")
            return redirect(url_for("UploadsView.list_files"))

        ext = os.path.splitext(safe_name)[1].lower()
        try:
            if ext == ".csv":
                df = pd.read_csv(filepath, nrows=100)
            elif ext in (".xls", ".xlsx"):
                df = pd.read_excel(filepath, nrows=100)
            else:
                flash(f"Preview not supported for '{ext}' files.", "warning")
                return redirect(url_for("UploadsView.list_files"))
        except Exception as e:
            flash(f"Error reading file: {e}", "danger")
            return redirect(url_for("UploadsView.list_files"))

        total_rows = len(df)
        columns = df.columns.tolist()
        rows = df.values.tolist()

        return self.render_template(
            "uploads_plugin/preview.html",
            filename=safe_name,
            columns=columns,
            rows=rows,
            total_rows=total_rows,
        )

    @expose("/download/<filename>")
    def download_file(self, filename):
        return send_from_directory(UPLOADS_DIR, filename, as_attachment=True)

    @expose("/delete/<filename>", methods=["POST"])
    def delete_file(self, filename):
        filepath = os.path.join(UPLOADS_DIR, secure_filename(filename))
        if os.path.isfile(filepath):
            os.remove(filepath)
            flash(f"File '{filename}' deleted.", "success")
        else:
            flash(f"File '{filename}' not found.", "warning")
        return redirect(url_for("UploadsView.list_files"))


v_appbuilder_view = UploadsView()
v_appbuilder_package = {
    "name": "Uploads",
    "category": "SNC Reports",
    "view": v_appbuilder_view,
}

bp = Blueprint(
    "uploads_plugin",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/uploads_plugin",
)


class UploadsPlugin(AirflowPlugin):
    name = "uploads_plugin"
    appbuilder_views = [v_appbuilder_package]
    flask_blueprints = [bp]
