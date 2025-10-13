import datetime
import logging
from functools import wraps
from typing import List, Dict, Optional
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import newrelic.agent

# Setup database models
Base = declarative_base()

class FABSSubmission(Base):
    __tablename__ = 'fabs_submissions'
    id = Column(Integer, primary_key=True)
    user_id = Column(String)
    status = Column(String)
    publish_status = Column(String)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    file_path = Column(String)
    is_historical = Column(Boolean)
    validation_errors = Column(String)

class FPDSData(Base):
    __tablename__ = 'fpds_data'
    id = Column(Integer, primary_key=True)
    action_type = Column(String)
    duns = Column(String)
    action_date = Column(DateTime)
    federal_action_obligation = Column(Integer)
    funding_agency_code = Column(String)

class Agency(Base):
    __tablename__ = 'agencies'
    id = Column(Integer, primary_key=True)
    code = Column(String, unique=True)
    name = Column(String)

class UserTestingSession(Base):
    __tablename__ = 'user_testing_sessions'
    id = Column(Integer, primary_key=True)
    test_name = Column(String)
    scheduled_time = Column(DateTime)
    participants = Column(String)
    status = Column(String)

# Initialize database
engine = create_engine('sqlite:///broker.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# Utility functions
def setup_newrelic_monitoring():
    newrelic.agent.initialize('newrelic.ini')
    return newrelic.agent.register_application()

def log_submission_activity(submission_id: int, action: str):
    logging.info(f"Submission {submission_id} {action} at {datetime.datetime.now()}")

def validate_duns_number(duns: str, action_type: str, action_date: datetime.datetime) -> bool:
    """Validate DUNS number based on business rules"""
    if not duns:
        return False
    
    sam_record = session.query(FPDSData).filter_by(duns=duns).first()
    if not sam_record:
        return False
    
    if action_type in ('B', 'C', 'D'):
        return True
    
    if sam_record.action_date and action_date:
        return action_date >= sam_record.action_date
    
    return False

# Core functionality implementations
def process_historical_deletions(cutoff_date: datetime.datetime):
    """Process deletions from 12-19-2017"""
    cutoff = datetime.datetime(2017, 12, 19)
    deleted_records = session.query(FPDSData).filter(FPDSData.action_date <= cutoff).delete()
    session.commit()
    return deleted_records

def derive_funding_agency_code(duns: str) -> Optional[str]:
    """Derive funding agency code based on DUNS"""
    if duns.startswith('00') and (duns.startswith('00FORGN') or duns.startswith('00' + '0'*5)):
        return 'FOREIGN'
    
    agency = session.query(Agency).filter(Agency.code.startswith(duns[:2])).first()
    return agency.code if agency else None

def update_publish_status(submission_id: int, new_status: str):
    """Update publish status and prevent duplicate publishing"""
    submission = session.query(FABSSubmission).get(submission_id)
    if not submission:
        raise ValueError("Submission not found")
    
    if submission.publish_status == new_status:
        return False
    
    submission.publish_status = new_status
    submission.updated_at = datetime.datetime.now()
    session.commit()
    log_submission_activity(submission_id, f"status changed to {new_status}")
    return True

def generate_d1_file(sync_with_fpds: bool = True) -> str:
    """Generate D1 file, optionally syncing with FPDS data"""
    if sync_with_fpds:
        last_fpds_update = session.query(FPDSData).order_by(FPDSData.action_date.desc()).first()
        if last_fpds_update and (datetime.datetime.now() - last_fpds_update.action_date).days < 1:
            return "D1 file already up-to-date with FPDS data"
    
    # Implementation for D1 file generation would go here
    return "D1 file generated successfully"

def validate_ppop_zip(zip_code: str) -> bool:
    """Validate PPoP ZIP code according to business rules"""
    if not zip_code:
        return False
    
    # Basic validation - can be extended with more complex rules
    if len(zip_code) not in (5, 9, 10):  # 10 accounts for ZIP+4 with hyphen
        return False
    
    return zip_code.replace('-', '').isdigit()

def get_user_testing_summary() -> Dict:
    """Generate summary of user testing sessions"""
    sessions = session.query(UserTestingSession).all()
    return {
        'total_sessions': len(sessions),
        'upcoming_sessions': [s.test_name for s in sessions if s.scheduled_time > datetime.datetime.now()],
        'completed_sessions': [s.test_name for s in sessions if s.scheduled_time <= datetime.datetime.now()]
    }

def update_fabs_sample_file(remove_header: bool = False) -> str:
    """Update FABS sample file implementation"""
    if remove_header:
        # Implementation to modify sample file would go here
        return "FABS sample file updated without header"
    return "FABS sample file updated"

def validate_flexfields(flexfields: List[str]) -> Dict[str, List[str]]:
    """Validate flexfields without performance impact"""
    warnings = []
    errors = []
    
    for field in flexfields:
        if not field:
            warnings.append("Empty flexfield detected")
        elif len(field) > 255:
            errors.append(f"Flexfield too long: {field[:20]}...")
    
    return {'warnings': warnings, 'errors': errors}

def get_agency_landing_page_data() -> Dict:
    """Get data for agency landing page (FABS/DABS navigation)"""
    fabs_count = session.query(FABSSubmission).count()
    return {
        'fabs_available': fabs_count > 0,
        'last_updated': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def handle_file_upload(file_path: str, expected_extension: str) -> Dict:
    """Handle file upload with proper validation"""
    if not file_path.endswith(expected_extension):
        return {
            'success': False,
            'error': f"Invalid file extension. Expected {expected_extension}",
            'suggestions': [
                f"Rename your file to use the {expected_extension} extension",
                "Check the file format requirements"
            ]
        }
    
    return {'success': True, 'message': "File uploaded successfully"}

def derive_office_name(office_code: str) -> str:
    """Derive office name from office code"""
    # This would typically query a database table of office codes
    office_map = {
        'ABC': 'Administrative Business Center',
        'DEF': 'Division of Executive Functions',
        'GHI': 'Grants and Housing Initiatives'
    }
    return office_map.get(office_code, f"Unknown Office ({office_code})")

def update_validation_rules() -> bool:
    """Update validation rules from DB-2213"""
    # Implementation would update the validation rules table
    return True

def check_gtas_window() -> bool:
    """Check if current time is within GTAS submission window"""
    now = datetime.datetime.now()
    gtas_start = datetime.datetime(now.year, now.month, 1)  # Example: first day of month
    gtas_end = gtas_start + datetime.timedelta(days=7)      # Example: 7-day window
    return gtas_start <= now <= gtas_end

def get_dashboard_status_labels() -> Dict[str, str]:
    """Return correct status labels for submission dashboard"""
    return {
        'draft': 'In Progress',
        'validated': 'Ready for Review',
        'published': 'Submitted to USAspending',
        'rejected': 'Needs Corrections'
    }

def check_historical_data_completeness() -> Dict[str, bool]:
    """Verify all required historical data is loaded"""
    required_tables = ['FPDSData', 'FABSSubmission']
    results = {}
    
    for table in required_tables:
        count = session.execute(f"SELECT COUNT(*) FROM {table.lower()}").scalar()
        results[table] = count > 0
        
    return results

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    setup_newrelic_monitoring()
    
    # Example: Process deletions
    deleted_count = process_historical_deletions(datetime.datetime.now())
    print(f"Deleted {deleted_count} historical records")
    
    # Example: Validate DUNS
    is_valid = validate_duns_number("123456789", "B", datetime.datetime.now())
    print(f"DUNS validation result: {is_valid}")
    
    # Example: Get user testing summary
    print(get_user_testing_summary())