# Database models and configurations
from datetime import datetime
import logging
from flask import Flask, request, jsonify, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
import newrelic.agent

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///broker.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Configure New Relic monitoring
newrelic.agent.initialize('newrelic.ini')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database Models
class FABSSubmission(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    agency_id = db.Column(db.String, nullable=False)
    status = db.Column(db.String, default='draft')
    publish_status = db.Column(db.String, default='unpublished')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, onupdate=datetime.utcnow)
    file_path = db.Column(db.String)
    is_historical = db.Column(db.Boolean, default=False)
    flex_fields = db.Column(db.JSON)

class ValidationRule(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    rule_code = db.Column(db.String, unique=True, nullable=False)
    description = db.Column(db.Text)
    error_message = db.Column(db.Text)
    is_active = db.Column(db.Boolean, default=True)

class GTASWindow(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    start_date = db.Column(db.DateTime, nullable=False)
    end_date = db.Column(db.DateTime, nullable=False)
    is_active = db.Column(db.Boolean, default=False)

class UserTesting(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    test_name = db.Column(db.String, nullable=False)
    test_date = db.Column(db.DateTime, nullable=False)
    participants = db.Column(db.Integer)
    feedback = db.Column(db.Text)
    status = db.Column(db.String, default='scheduled')

# Application Routes
@app.route('/api/submissions/<int:submission_id>', methods=['GET', 'POST'])
def handle_submission(submission_id):
    if request.method == 'POST':
        data = request.json
        submission = FABSSubmission.query.get(submission_id)
        if submission:
            # Prevent double publishing
            if submission.publish_status == 'publishing':
                return jsonify({'error': 'Publication in progress'}), 400
            
            submission.publish_status = 'publishing'
            db.session.commit()
            
            # Process submission
            try:
                process_submission(submission)
                submission.publish_status = 'published'
                db.session.commit()
                return jsonify({'status': 'published'})
            except Exception as e:
                submission.publish_status = 'error'
                db.session.commit()
                logger.error(f"Submission error: {str(e)}")
                return jsonify({'error': str(e)}), 500
        else:
            return jsonify({'error': 'Submission not found'}), 404
    else:
        submission = FABSSubmission.query.get_or_404(submission_id)
        return jsonify({
            'id': submission.id,
            'status': submission.status,
            'publish_status': submission.publish_status,
            'created_at': submission.created_at.isoformat(),
            'updated_at': submission.updated_at.isoformat() if submission.updated_at else None
        })

def process_submission(submission):
    """Process FABS submission with validation and derivation"""
    # Validate submission
    validate_flex_fields(submission.flex_fields)
    
    # Process historical data if needed
    if submission.is_historical:
        process_historical_data(submission)

    # Perform derivations
    derive_fields(submission)
    
    logger.info(f"Processed submission {submission.id}")

def validate_flex_fields(flex_fields):
    """Validate flex fields according to schema v1.1"""
    if not flex_fields:
        return
    
    max_length = 255  # For LegalEntityAddressLine3 per v1.1
    if 'LegalEntityAddressLine3' in flex_fields:
        if len(flex_fields['LegalEntityAddressLine3']) > max_length:
            raise ValueError('LegalEntityAddressLine3 exceeds maximum length')

def derive_fields(submission):
    """Derive fields like FundingAgencyCode, FREC, etc."""
    if not submission.flex_fields:
        return
    
    # Example derivation for FundingAgencyCode
    if 'FundingAgencyCode' not in submission.flex_fields:
        submission.flex_fields['FundingAgencyCode'] = '00*****'
    
    # Other derivations would go here
    logger.info(f"Derived fields for submission {submission.id}")

@app.route('/api/validation_rules', methods=['GET'])
def get_validation_rules():
    rules = ValidationRule.query.filter_by(is_active=True).all()
    return jsonify([{
        'code': rule.rule_code,
        'description': rule.description,
        'error_message': rule.error_message
    } for rule in rules])

@app.route('/api/gtas/window', methods=['GET'])
def check_gtas_window():
    window = GTASWindow.query.filter_by(is_active=True).first()
    if window:
        return jsonify({
            'is_active': True,
            'start_date': window.start_date.isoformat(),
            'end_date': window.end_date.isoformat()
        })
    return jsonify({'is_active': False})

@app.route('/api/fabs/generate-dfile', methods=['POST'])
def generate_dfile():
    """Generate D File from FABS data"""
    data = request.json
    agency_id = data.get('agency_id')
    
    # Check cache first
    cached = check_dfile_cache(agency_id)
    if cached:
        return jsonify({'status': 'cached', 'file': cached})
    
    # Generate new file
    dfile = generate_fpds_integrated_file(agency_id)
    cache_dfile(agency_id, dfile)
    
    return jsonify({'status': 'generated', 'file': dfile})

# Helper functions
def check_dfile_cache(agency_id):
    """Check if a recent D file exists in cache"""
    # Implementation would use Redis or similar cache
    return None

def cache_dfile(agency_id, dfile):
    """Cache the generated D file"""
    # Implementation would use Redis or similar cache
    pass

def generate_fpds_integrated_file(agency_id):
    """Generate FPDS integrated file for an agency"""
    # Implementation would pull from database and format
    return f"d_file_{agency_id}_{datetime.now().date().isoformat()}.csv"

@app.route('/api/user-testing', methods=['POST'])
def schedule_user_testing():
    data = request.json
    test = UserTesting(
        test_name=data['test_name'],
        test_date=datetime.fromisoformat(data['test_date']),
        participants=data.get('participants', 0),
        feedback=data.get('feedback', ''),
        status='scheduled'
    )
    db.session.add(test)
    db.session.commit()
    return jsonify({'id': test.id}), 201

@app.route('/api/resources/update', methods=['POST'])
def update_resources_page():
    """Update the resources page with new styles"""
    # This would interface with a CMS or static site generator
    return jsonify({'status': 'update queued'})

# Initialize database
with app.app_context():
    db.create_all()

if __name__ == '__main__':
    app.run(debug=True)