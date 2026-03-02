import os
import math
from datetime import datetime
from airflow.plugins_manager import AirflowPlugin
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from flask import Blueprint, send_from_directory, request

DOWNLOADS_DIR = os.path.join(os.path.expanduser("~"), "airflow", "downloads")
PER_PAGE = 10

class DownloadsView(AppBuilderBaseView):
    default_view = "list_files"

    @expose("/")
    def list_files(self):
        files = []
        if os.path.isdir(DOWNLOADS_DIR):
            for name in os.listdir(DOWNLOADS_DIR):
                filepath = os.path.join(DOWNLOADS_DIR, name)
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
        sort_by = request.args.get("sort_by", "name")
        sort_dir = request.args.get("sort_dir", "asc")
        if sort_by not in ("name", "size", "created"):
            sort_by = "name"
        if sort_dir not in ("asc", "desc"):
            sort_dir = "asc"

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
            "downloads_plugin/list.html",
            files=page_files,
            page=page,
            total_pages=total_pages,
            total_files=total_files,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

    @expose("/download/<filename>")
    def download_file(self, filename):
        return send_from_directory(DOWNLOADS_DIR, filename, as_attachment=True)


v_appbuilder_view = DownloadsView()
v_appbuilder_package = {
    "name": "Downloads",
    "category": "SNC Reports",
    "view": v_appbuilder_view,
}

bp = Blueprint(
    "downloads_plugin",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/downloads_plugin",
)


class DownloadsPlugin(AirflowPlugin):
    name = "downloads_plugin"
    appbuilder_views = [v_appbuilder_package]
    flask_blueprints = [bp]
