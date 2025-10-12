import logging
from datetime import datetime
from functools import wraps
from flask import Flask, request, jsonify, redirect, url_for, render_template
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import redis
import newrelic.agent

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///broker.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
Base = declarative_base()

# Initialize New Relic monitoring
newrelic.agent.initialize('/path/to/newrelic.ini')

# Redis for caching
cache = redis.StrictRedis(host='localhost', port=6379, db=0)

# Database models
class Submission(Base):
    __tablename__ = 'submissions'
    id = Column(Integer, primary_key=True)
    agency = Column(String(100))
    file_name = Column(String(255))
    publish_status = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by = Column(String(100))
    flexfields = Column(Text)  # JSON string of flex fields

class ValidationRule(Base):
    __tablename__ = 'validation_rules'
    id = Column(Integer, primary_key=True)
    rule_code = Column(String(50))
    rule_description = Column(Text)
    is_active = Column(Integer, default=1)

class PublishedFile(Base):
    __tablename__ = 'published_files'
    id = Column(Integer, primary_key=True)
    file_name = Column(String(255))
    agency = Column(String(100))
    publish_date = Column(DateTime, default=datetime.utcnow)
    file_path = Column(String(255))

# Create database
engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('broker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Helper functions
def cache_results(key_prefix):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = f"{key_prefix}:{str(args)}:{str(kwargs)}"
            cached_result = cache.get(cache_key)
            if cached_result:
                return cached_result
            result = func(*args, **kwargs)
            cache.set(cache_key, result, ex=3600)  # Cache for 1 hour
            return result
        return wrapper
    return decorator

# API Endpoints
@app.route('/api/submissions', methods=['POST'])
def create_submission():
    data = request.json
    session = Session()
    try:
        submission = Submission(
            agency=data.get('agency'),
            file_name=data.get('file_name'),
            publish_status='pending',
            created_by=data.get('created_by'),
            flexfields=data.get('flexfields', '{}')
        )
        session.add(submission)
        session.commit()
        return jsonify({"message": "Submission created", "id": submission.id}), 201
    except Exception as e:
        session.rollback()
        logger.error(f"Error creating submission: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()

@app.route('/api/submissions/<int:submission_id>/publish', methods=['POST'])
def publish_submission(submission_id):
    session = Session()
    submission = session.query(Submission).get(submission_id)
    if not submission:
        return jsonify({"error": "Submission not found"}), 404
    
    if submission.publish_status == 'published':
        return jsonify({"error": "Already published"}), 400
    
    submission.publish_status = 'published'
    submission.updated_at = datetime.utcnow()
    
    try:
        session.commit()
        # Process file publishing logic here
        return jsonify({"message": "Submission published"}), 200
    except Exception as e:
        session.rollback()
        logger.error(f"Error publishing submission: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()

@app.route('/api/files/published', methods=['GET'])
def get_published_files():
    agency = request.args.get('agency')
    session = Session()
    try:
        query = session.query(PublishedFile)
        if agency:
            query = query.filter(PublishedFile.agency == agency)
        files = query.all()
        return jsonify([{
            "id": f.id,
            "file_name": f.file_name,
            "agency": f.agency,
            "publish_date": str(f.publish_date),
            "file_path": f.file_path
        } for f in files]), 200
    except Exception as e:
        logger.error(f"Error fetching published files: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()

@app.route('/api/files/upload', methods=['POST'])
def upload_file():
    file = request.files.get('file')
    if not file:
        return jsonify({"error": "No file uploaded"}), 400
    
    # Validate file extension
    if not file.filename.lower().endswith('.csv'):
        return jsonify({
            "error": "Invalid file type",
            "message": "Please upload a CSV file"
        }), 400
    
    try:
        # Process file upload
        df = pd.read_csv(file)
        return jsonify({
            "message": "File uploaded successfully",
            "rows": len(df),
            "columns": list(df.columns)
        }), 200
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/validation/rules', methods=['GET'])
@cache_results('validation_rules')
def get_validation_rules():
    session = Session()
    try:
        rules = session.query(ValidationRule).filter(ValidationRule.is_active == 1).all()
        return jsonify([{
            "id": r.id,
            "code": r.rule_code,
            "description": r.rule_description
        } for r in rules]), 200
    except Exception as e:
        logger.error(f"Error fetching validation rules: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()

@app.route('/api/validation/run', methods=['POST'])
def run_validation():
    data = request.json
    submission_id = data.get('submission_id')
    
    if not submission_id:
        return jsonify({"error": "submission_id is required"}), 400
    
    session = Session()
    try:
        submission = session.query(Submission).get(submission_id)
        if not submission:
            return jsonify({"error": "Submission not found"}), 404
        
        # Simulate validation logic
        result = {
            "submission_id": submission.id,
            "status": "completed",
            "errors": [],
            "warnings": [],
            "is_valid": True
        }
        
        return jsonify(result), 200
    except Exception as e:
        logger.error(f"Error running validation: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()

# UI Routes
@app.route('/')
def home():
    return render_template('home.html')

@app.route('/resources')
def resources():
    return render_template('resources.html', styles='new_broker')

@app.route('/fabs')
def fabs_landing():
    return render_template('fabs_landing.html')

@app.route('/dabs')
def dabs_landing():
    return render_template('dabs_landing.html')

@app.route('/help')
def help_page():
    return render_template('help.html')

@app.route('/submission-dashboard')
def submission_dashboard():
    return render_template('submission_dashboard.html')

if __name__ == '__main__':
    app.run(debug=True)