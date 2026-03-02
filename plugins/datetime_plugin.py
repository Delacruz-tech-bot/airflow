from airflow.plugins_manager import AirflowPlugin
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from flask import Blueprint
import datetime

class DateTimeView(AppBuilderBaseView):
    default_view = "display"

    @expose("/")
    def display(self):
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return self.render_template("datetime_plugin/display.html", current_time=current_time)

v_appbuilder_view = DateTimeView()
v_appbuilder_package = {
    "name": "Date Time Display",
    "category": "SNC Reports",
    "view": v_appbuilder_view
}

bp = Blueprint(
    "datetime_plugin",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/datetime_plugin"
)

class DateTimePlugin(AirflowPlugin):
    name = "datetime_plugin"
    appbuilder_views = [v_appbuilder_package]
    flask_blueprints = [bp]
