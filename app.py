from flask import Flask, render_template
from flask_cors import CORS
import os
from flask import send_from_directory
from update_feature import update_bp
from clone_feature import clone_bp

app = Flask(__name__)
CORS(app)

app.register_blueprint(update_bp)
app.register_blueprint(clone_bp)
@app.route('/favicon.ico')
def favicon():
    return send_from_directory(
        os.path.join(app.root_path, 'static'),
        'favicon.ico', 
        mimetype='image/vnd.microsoft.icon'
    )

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)