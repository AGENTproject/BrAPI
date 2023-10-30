from flask import Blueprint

admin_bp = Blueprint('admin_bp', __name__)


@admin_bp.route('/')
def index():
    return "brapi on the frame of agent project"
