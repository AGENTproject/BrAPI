from flask import Blueprint, render_template

admin_bp = Blueprint('admin_bp', __name__)


@admin_bp.route('/')
def index():
    return render_template('index.html')
