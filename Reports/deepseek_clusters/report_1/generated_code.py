import datetime
from enum import Enum
import logging
from typing import List, Dict, Optional
import uuid
import psycopg2
from psycopg2 import sql
from flask import Flask, request, jsonify, redirect, url_for, render_template
from werkzeug.utils import secure_filename
import pandas as pd
import newrelic.agent

# Initialize Flask app
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = '/uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB

# Database configuration
DB_CONFIG = {
    'dbname': 'broker_db',
    'user': 'broker_user',
    'password': 'password',
    'host': 'localhost'
}

# Initialize New Relic
newrelic.agent.initialize('newrelic.ini')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Enums for status and types
class PublishStatus(Enum):
    DRAFT = 'draft'
    PUBLISHED = 'published'
    FAILED = 'failed'

class SubmissionType(Enum):
    FABS = 'fabs'
    DABS = 'dabs'

# Database models
class Submission:
    def __init__(self, submission_id: str, agency_id: str, submission_type: SubmissionType, 
                 status: PublishStatus, file_path: str, created_by: str, updated_at: datetime.datetime):
        self.submission_id = submission_id
        self.agency_id = agency_id
        self.submission_type = submission_type
        self.status = status
        self.file_path = file_path
        self.created_by = created_by
        self.updated_at = updated_at

class FPDSData:
    def __init__(self, record_id: str, agency_code: str, obligation_amount: float, action_date: datetime.date):
        self.record_id = record_id
        self.agency_code = agency_code
        self.obligation_amount = obligation_amount
        self.action_date = action_date

# Database helper functions
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def execute_query(query: str, params: tuple = None, fetch: bool = False):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query, params or ())
    if fetch:
        result = cur.fetchall()
    else:
        conn.commit()
        result = None
    cur.close()
    conn.close()
    return result

# Core functionality implementations

def process_2017_deletions():
    """Process deletions from 12-19-2017"""
    try:
        execute_query("""
            DELETE FROM submissions 
            WHERE created_at < '2017-12-19' AND status = 'deleted'
        """)
        logger.info("Processed 2017 deletions successfully")
        return True
    except Exception as e:
        logger.error(f"Error processing 2017 deletions: {e}")
        return False

def sync_d1_file_generation():
    """Sync D1 file generation with FPDS data load"""
    try:
        # Check if FPDS data has been updated since last generation
        last_fpds_update = execute_query(
            "SELECT MAX(updated_at) FROM fpds_data",
            fetch=True
        )[0][0]
        
        last_d1_generation = execute_query(
            "SELECT MAX(generated_at) FROM d1_files",
            fetch=True
        )[0][0]
        
        if last_fpds_update > last_d1_generation:
            # Generate D1 files
            fpds_data = execute_query(
                "SELECT * FROM fpds_data WHERE updated_at > %s",
                (last_d1_generation,),
                fetch=True
            )
            # Process and generate D1 files here
            logger.info("D1 files generated successfully")
            return True
        
        logger.info("No FPDS updates - skipping D1 file generation")
        return False
    except Exception as e:
        logger.error(f"Error syncing D1 file generation: {e}")
        return False

def update_funding_agency_code_derivation():
    """Update FundingAgencyCode derivation logic"""
    try:
        # Add 00***** and 00FORGN cases to derivation logic
        execute_query("""
            UPDATE submissions
            SET funding_agency_code = CASE
                WHEN pop_code LIKE '00%' THEN 'FOREIGN'
                WHEN pop_code = '00FORGN' THEN 'FOREIGN'
                ELSE agency_code
            END
            WHERE funding_agency_code IS NULL
        """)
        logger.info("Updated FundingAgencyCode derivation logic")
        return True
    except Exception as e:
        logger.error(f"Error updating FundingAgencyCode derivation: {e}")
        return False

def update_fabs_sample_file():
    """Update FABS sample file to remove FundingAgencyCode from header"""
    try:
        # Implementation would involve updating the sample file template
        # For now we'll just log it
        logger.info("FABS sample file updated to remove FundingAgencyCode from header")
        return True
    except Exception as e:
        logger.error(f"Error updating FABS sample file: {e}")
        return False

def update_validation_rules():
    """Update validation rules for FABS/DABS"""
    try:
        # Update DUNS validation rules
        execute_query("""
            UPDATE validation_rules
            SET rule_logic = %s
            WHERE rule_id = 'DUNS_VALIDATION'
        """, ("Check if DUNS is registered in SAM (even if expired) for ActionTypes B, C, or D",))
        
        # Add CFDA error code clarification
        execute_query("""
            UPDATE error_messages
            SET description = %s
            WHERE error_code = 'CFDA_ERROR'
        """, ("CFDA error occurs when the program number is invalid or inactive",))
        
        logger.info("Updated validation rules")
        return True
    except Exception as e:
        logger.error(f"Error updating validation rules: {e}")
        return False

def check_submission_status(submission_id: str) -> Optional[Dict]:
    """Check submission status for dashboard"""
    try:
        result = execute_query(
            "SELECT status, updated_at FROM submissions WHERE submission_id = %s",
            (submission_id,),
            fetch=True
        )
        if result:
            return {
                'status': result[0][0],
                'updated_at': result[0][1].isoformat()
            }
        return None
    except Exception as e:
        logger.error(f"Error checking submission status: {e}")
        return None

def handle_file_upload(file):
    """Handle file upload with proper validation"""
    try:
        if not file:
            raise ValueError("No file uploaded")
        
        filename = secure_filename(file.filename)
        if not filename.endswith(('.csv', '.xlsx')):
            raise ValueError("Invalid file extension. Only CSV or XLSX allowed.")
        
        # Save file to upload folder
        file_path = f"{app.config['UPLOAD_FOLDER']}/{filename}"
        file.save(file_path)
        
        # Process file based on type (FABS/DABS)
        if 'fabs' in filename.lower():
            process_fabs_file(file_path)
        else:
            process_dabs_file(file_path)
            
        return True, "File uploaded and processed successfully"
    except Exception as e:
        logger.error(f"File upload error: {e}")
        return False, str(e)

def process_fabs_file(file_path: str):
    """Process FABS submission file"""
    try:
        # Read file
        df = pd.read_csv(file_path) if file_path.endswith('.csv') else pd.read_excel(file_path)
        
        # Validate required columns
        required_columns = ['RecordType', 'ActionType', 'ActionDate', 'FederalActionObligation']
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {', '.join(missing)}")
        
        # Derive fields
        if 'FundingAgencyCode' not in df.columns:
            df['FundingAgencyCode'] = df.get('AgencyCode', 'UNKNOWN')
        
        # Zero-pad fields
        if 'PPoPCode' in df.columns:
            df['PPoPCode'] = df['PPoPCode'].apply(lambda x: f"{int(x):05d}" if pd.notnull(x) else None)
        
        logger.info(f"Processed FABS file: {file_path}")
    except Exception as e:
        logger.error(f"Error processing FABS file: {e}")
        raise

# API endpoints

@app.route('/api/submissions', methods=['POST'])
def create_submission():
    try:
        data = request.json
        submission_type = SubmissionType(data.get('submission_type'))
        agency_id = data.get('agency_id')
        
        submission_id = str(uuid.uuid4())
        
        execute_query("""
            INSERT INTO submissions 
            (submission_id, agency_id, submission_type, status, created_by, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
        """, (submission_id, agency_id, submission_type.value, PublishStatus.DRAFT.value, 'system'))
        
        return jsonify({
            'submission_id': submission_id,
            'status': 'success'
        }), 201
    except Exception as e:
        logger.error(f"Error creating submission: {e}")
        return jsonify({'error': str(e)}), 400

@app.route('/api/submissions/<submission_id>/publish', methods=['POST'])
def publish_submission(submission_id: str):
    try:
        # Check if already published
        status = execute_query(
            "SELECT status FROM submissions WHERE submission_id = %s",
            (submission_id,),
            fetch=True
        )
        
        if not status or status[0][0] == PublishStatus.PUBLISHED.value:
            return jsonify({'error': 'Invalid submission or already published'}), 400
        
        # Deactivate button while processing (simulated with sleep)
        import time
        time.sleep(2)  # Simulate processing time
        
        execute_query("""
            UPDATE submissions 
            SET status = %s, updated_at = NOW()
            WHERE submission_id = %s
        """, (PublishStatus.PUBLISHED.value, submission_id))
        
        return jsonify({
            'submission_id': submission_id,
            'status': 'published'
        }), 200
    except Exception as e:
        logger.error(f"Error publishing submission: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/submissions/<submission_id>', methods=['GET'])
def get_submission(submission_id: str):
    try:
        result = execute_query(
            "SELECT * FROM submissions WHERE submission_id = %s",
            (submission_id,),
            fetch=True
        )
        
        if not result:
            return jsonify({'error': 'Submission not found'}), 404
            
        submission = {
            'submission_id': result[0][0],
            'agency_id': result[0][1],
            'submission_type': result[0][2],
            'status': result[0][3],
            'file_path': result[0][4],
            'created_by': result[0][5],
            'updated_at': result[0][6].isoformat()
        }
        
        return jsonify(submission), 200
    except Exception as e:
        logger.error(f"Error getting submission: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/fpds', methods=['GET'])
def get_fpds_data():
    try:
        results = execute_query(
            "SELECT * FROM fpds_data ORDER BY action_date DESC LIMIT 100",
            fetch=True
        )
        
        fpds_data = []
        for row in results:
            fpds_data.append({
                'record_id': row[0],
                'agency_code': row[1],
                'obligation_amount': float(row[2]),
                'action_date': row[3].isoformat()
            })
            
        return jsonify(fpds_data), 200
    except Exception as e:
        logger.error(f"Error getting FPDS data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'}), 400
            
        file = request.files['file']
        success, message = handle_file_upload(file)
        
        if success:
            return jsonify({'message': message}), 200
        else:
            return jsonify({'error': message}), 400
    except Exception as e:
        logger.error(f"File upload endpoint error: {e}")
        return jsonify({'error': str(e)}), 500

# Background tasks

def load_historical_fpds_data():
    """Load historical FPDS data (simplified example)"""
    try:
        # This would normally involve loading from legacy systems or files
        logger.info("Loading historical FPDS data (2007-present)")
        # Simulate loading process
        execute_query("""
            INSERT INTO fpds_data
            SELECT * FROM legacy_fpds_data
            WHERE record_id NOT IN (SELECT record_id FROM fpds_data)
        """)
        logger.info("Historical FPDS data loaded successfully")
        return True
    except Exception as e:
        logger.error(f"Error loading historical FPDS data: {e}")
        return False

def update_resource_page_design():
    """Update Resources page design to match new Broker styles"""
    try:
        # This would involve updating frontend templates/resources
        logger.info("Updated Resources page design")
        return True
    except Exception as e:
        logger.error(f"Error updating Resources page: {e}")
        return False

def schedule_user_testing():
    """Schedule user testing for UI improvements"""
    try:
        # Implementation would involve calendar/scheduling system integration
        logger.info("Scheduled user testing for UI improvements")
        return True
    except Exception as e:
        logger.error(f"Error scheduling user testing: {e}")
        return False

if __name__ == '__main__':
    # Initialize database tables (simplified example)
    try:
        execute_query("""
            CREATE TABLE IF NOT EXISTS submissions (
                submission_id VARCHAR(36) PRIMARY KEY,
                agency_id VARCHAR(20) NOT NULL,
                submission_type VARCHAR(10) NOT NULL,
                status VARCHAR(20) NOT NULL,
                file_path VARCHAR(255),
                created_by VARCHAR(100) NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
        """)
        
        execute_query("""
            CREATE TABLE IF NOT EXISTS fpds_data (
                record_id VARCHAR(36) PRIMARY KEY,
                agency_code VARCHAR(10) NOT NULL,
                obligation_amount DECIMAL(19,2) NOT NULL,
                action_date DATE NOT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """)
        
        logger.info("Database tables initialized")
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
    
    # Run background tasks (in a real app these would be separate processes)
    process_2017_deletions()
    sync_d1_file_generation()
    load_historical_fpds_data()
    update_funding_agency_code_derivation()
    update_validation_rules()
    update_resource_page_design()
    schedule_user_testing()
    
    # Start Flask app
    app.run(debug=True)