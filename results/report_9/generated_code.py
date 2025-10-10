import json
import logging
import hashlib
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, abort
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text
import requests
import os
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///broker.db'
app.config['UPLOAD_FOLDER'] = 'uploads'
db = SQLAlchemy(app)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Submission(db.Model):
    id = Column(Integer, primary_key=True)
    user_id = Column(String(50))
    submission_type = Column(String(10))  # 'FABS' or 'FPDS'
    status = Column(String(20), default='pending')  # pending, validated, published, deleted
    publish_status = Column(String(20), default='not_published')
    file_path = Column(String(200))
    updated_at = Column(DateTime, default=datetime.utcnow)
    publish_hash = Column(String(64))  # To prevent duplicates
    flexfields = Column(Text)  # JSON string for flexfields
    deletion_date = Column(DateTime, nullable=True)
    gtas_locked = Column(Boolean, default=False)

db.create_all()

class GTASWindow(db.Model):
    id = Column(Integer, primary_key=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    active = Column(Boolean, default=False)

db.create_all()

# Cache for D Files generation to avoid duplicates
d_file_cache = {}

# Load historical data (assume from a source)
def load_historical_fabs():
    # Assumption: Load from a CSV or API
    response = requests.get('https://example.com/fabs_historical_data')  # Replace with actual
    if response.status_code == 200:
        data = response.json()
        for item in data:
            sub = Submission(user_id=item['user_id'], submission_type='FABS', 
                             publish_hash=hashlib.sha256(json.dumps(item).encode()).hexdigest())
            derive_fields(sub, item)
            db.session.add(sub)
        db.session.commit()
    logger.info("Historical FABS data loaded")

def derive_fields(sub, data):
    # Derive FREC, office names, etc. based on rules
    # Assumptions from stories: Derive fundingAgencyCode, etc.
    if 'fundingAgencyCode' not in data:
        # Simple derivation logic
        data['fundingAgencyCode'] = data.get('agency_code', 'unknown') if sub.submission_type == 'FABS' else ''
    sub.flexfields = json.dumps(data)
    # Other derivations: PPoPCode, etc.

def validate_submission(data, submission_type):
    errors = []
    warnings = []
    # Validation rules from stories
    if submission_type == 'FABS':
        # Check DUNS for ActionTypes B, C, D
        if data.get('actionType') in ['B', 'C', 'D']:
            if not check_sam_registration(data.get('duns')):
                warnings.append("DUNS registered in SAM")
        # ZIP validations
        if 'zip' not in data or not data['zip'].strip():
            if not (data.get('ppoPzip') and len(data['ppoPzip']) >= 5):
                errors.append("PPoPZIP is required")
        # Loan records: accept zero/blank
        if data.get('recordType') == 'loan':
            if data.get('federalActionObligation') in [0, '', None]:
                pass  # Allowed
        # Flexfields in warnings/errors
        for field in data.get('flexfields', []):
            warnings.append(f"Flexfield: {field}")
    return errors, warnings

def check_sam_registration(duns):
    # Mock SAM check
    registered_duns = ['123456789']  # Mock
    return duns in registered_duns

def prevent_duplicate_publish(sub):
    if sub.publish_hash:
        existing = Submission.query.filter_by(publish_hash=sub.publish_hash, publish_status='published').first()
        if existing:
            raise ValueError("Duplicate submission detected")
    sub.publish_hash = hashlib.sha256((sub.user_id + str(sub.updated_at)).encode()).hexdigest()

@app.route('/submission/upload', methods=['POST'])
def upload_submission():
    user_id = request.form.get('user_id')
    submission_type = request.form.get('type')  # FABS or FPDS
    file = request.files.get('file')
    if not file:
        abort(400, "No file provided")
    filename = secure_filename(file.filename)
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(file_path)
    
    data = json.loads(request.form.get('data', '{}'))  # Assume JSON data
    errors, warnings = validate_submission(data, submission_type)
    if errors:
        return jsonify({'status': 'error', 'errors': errors})
    
    sub = Submission(user_id=user_id, submission_type=submission_type, file_path=file_path)
    derive_fields(sub, data)
    db.session.add(sub)
    db.session.commit()
    logger.info(f"Submission uploaded by {user_id}, type: {submission_type}, errors: {len(errors)}, warnings: {len(warnings)}")
    return jsonify({'submission_id': sub.id, 'errors': errors, 'warnings': warnings})

@app.route('/submission/publish/<int:submission_id>', methods=['POST'])
def publish_submission(submission_id):
    sub = Submission.query.get_or_500(submission_id)
    if sub.status == 'published':
        abort(400, "Already published")
    gtas = GTASWindow.query.filter_by(active=True).first()
    if gtas and gtas.start_date <= datetime.utcnow() <= gtas.end_date:
        abort(400, "GTAS period locked")
    
    try:
        prevent_duplicate_publish(sub)
        sub.publish_status = 'published'
        sub.status = 'published'
        # Send to USAspending if grant
        if sub.submission_type == 'FABS' and 'grant' in json.loads(sub.flexfields).get('recordType', ''):
            # Mock API call
            requests.post('https://usaspending.gov/api/submit', json=json.loads(sub.flexfields))
        # Sync D1 file if needed
        generate_d1_file(sub)
        db.session.commit()
        logger.info(f"Submission {submission_id} published")
        return jsonify({'status': 'published'})
    except ValueError as e:
        abort(400, str(e))

@app.route('/api/generate_d_file', methods=['POST'])
def generate_d_file():
    params = request.json
    cache_key = json.dumps(params, sort_keys=True)
    if cache_key in d_file_cache:
        logger.info("D File from cache")
        return d_file_cache[cache_key]
    # Mock generation
    d_data = {'type': params.get('type', 'FABS'), 'data': []}
    d_file_cache[cache_key] = d_data
    return jsonify(d_data)

@app.route('/submission/delete/<int:submission_id>', methods=['DELETE'])
def delete_submission(submission_id):
    sub = Submission.query.get_or_500(submission_id)
    sub.deletion_date = datetime.utcnow()
    sub.status = 'deleted'
    # Process deletions for 12-19-2017
    if sub.deletion_date.date() == datetime(2017, 12, 19).date():
        # Special processing
        logger.info(f"Processed deletion for 12-19-2017: {submission_id}")
    db.session.commit()
    return jsonify({'status': 'deleted'})

@app.route('/download/published_fabs')
def download_published_fabs():
    subs = Submission.query.filter_by(submission_type='FABS', publish_status='published').all()
    files = [{'file_path': sub.file_path, 'updated_at': sub.updated_at.isoformat()} for sub in subs]
    return jsonify(files)

@app.route('/permissions/reset', methods=['POST'])
def reset_permissions():
    # Reset to Staging MAX permissions (mock)
    logger.info("Permissions reset to Staging MAX")
    return jsonify({'status': 'reset'})

@app.route('/api/helpful_error', methods=['POST'])
def file_level_error():
    file = request.files.get('file')
    if not file.filename.endswith(('.csv', '.txt')):
        abort(400, "Invalid file extension. Expected .csv or .txt")
    return jsonify({'message': 'File accepted with helpful error context'})

# Initial load
if __name__ == '__main__':
    if not os.path.exists(app.config['UPLOAD_FOLDER']):
        os.makedirs(app.config['UPLOAD_FOLDER'])
    load_historical_fabs()
    app.run(debug=True)