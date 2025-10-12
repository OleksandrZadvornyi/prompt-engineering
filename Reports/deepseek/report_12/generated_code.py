import datetime
from typing import List, Dict, Optional
import logging
from logging.handlers import RotatingFileHandler
import requests
import json
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from functools import lru_cache
import hashlib

# Configure logging for better troubleshooting
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = RotatingFileHandler('broker.log', maxBytes=1024*1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

Base = declarative_base()

# Database models
class Submission(Base):
    __tablename__ = 'submissions'
    id = Column(Integer, primary_key=True)
    agency = Column(String(100))
    publish_status = Column(String(50))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, onupdate=datetime.datetime.utcnow)
    file_path = Column(String(255))
    is_fabs = Column(Boolean)
    created_by = Column(String(100))
    validation_errors = Column(Text)
    
class ValidationRule(Base):
    __tablename__ = 'validation_rules'
    id = Column(Integer, primary_key=True)
    rule_code = Column(String(50))
    description = Column(Text)
    error_message = Column(Text)
    is_active = Column(Boolean, default=True)
    
class PublishedAwardFinancialAssistance(Base):
    __tablename__ = 'published_award_financial_assistance'
    id = Column(Integer, primary_key=True)
    agency_code = Column(String(10))
    cfda_number = Column(String(6))
    action_type = Column(String(1))
    record_type = Column(String(1))
    duns_number = Column(String(9))
    ppop_code = Column(String(5))
    ppop_congressional_district = Column(String(2))
    legal_entity_address_line3 = Column(String(100))
    funding_agency_code = Column(String(10))
    federal_action_obligation = Column(String(20))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)

class GTASWindow(Base):
    __tablename__ = 'gtas_window'
    id = Column(Integer, primary_key=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    is_active = Column(Boolean, default=False)

class UserTesting(Base):
    __tablename__ = 'user_testing'
    id = Column(Integer, primary_key=True)
    test_name = Column(String(255))
    tester_name = Column(String(100))
    test_date = Column(DateTime)
    findings = Column(Text)
    status = Column(String(50))
    ui_improvements = Column(Text)

# Initialize database
engine = create_engine('sqlite:///broker.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Data processing functions
def process_deletions(deletion_date: str) -> bool:
    """Process deletions from 12-19-2017"""
    try:
        formatted_date = datetime.datetime.strptime(deletion_date, '%m-%d-%Y')
        session = Session()
        
        # Delete records marked for deletion on the given date
        deleted_count = session.query(Submission).filter(
            Submission.updated_at <= formatted_date,
            Submission.publish_status == 'deleted'
        ).delete()
        
        session.commit()
        logger.info(f"Processed {deleted_count} deletions from {deletion_date}")
        return True
    except Exception as e:
        logger.error(f"Error processing deletions: {str(e)}")
        session.rollback()
        return False

def update_publish_status(submission_id: int, new_status: str) -> bool:
    """Update publish status of a submission and log the change"""
    try:
        session = Session()
        submission = session.query(Submission).filter_by(id=submission_id).first()
        if submission:
            submission.publish_status = new_status
            session.commit()
            logger.info(f"Updated submission {submission_id} to status {new_status}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error updating publish status: {str(e)}")
        session.rollback()
        return False

def validate_flexfields(flexfields: Dict) -> bool:
    """Validate flexfields to ensure they don't impact performance"""
    MAX_FLEXFIELDS = 50
    if len(flexfields) > MAX_FLEXFIELDS:
        logger.warning(f"Too many flexfields: {len(flexfields)}")
        return False
    return True

def generate_d_file(submission_id: int, force_regenerate: bool = False) -> Optional[str]:
    """Generate D file only if FPDS data has been updated"""
    session = Session()
    submission = session.query(Submission).filter_by(id=submission_id).first()
    
    if not submission:
        return None
    
    # Check if FPDS data has updated since last generation
    last_fpds_update = session.query(Submission.updated_at).filter(
        Submission.is_fabs == False,
        Submission.updated_at > submission.updated_at
    ).order_by(Submission.updated_at.desc()).first()
    
    if not force_regenerate and not last_fpds_update:
        return "No FPDS updates - using cached D file"
    
    # Generate new D file
    try:
        file_content = generate_d_file_content(submission_id)
        file_path = f"/d_files/{submission_id}_{datetime.datetime.now().timestamp()}.csv"
        
        with open(file_path, 'w') as f:
            f.write(file_content)
        
        # Update submission record with new file path
        submission.file_path = file_path
        session.commit()
        return file_path
    except Exception as e:
        logger.error(f"Error generating D file: {str(e)}")
        return None

@lru_cache(maxsize=100)
def generate_d_file_content(submission_id: int) -> str:
    """Generate content for D file with caching to prevent duplicate processing"""
    session = Session()
    submission = session.query(Submission).filter_by(id=submission_id).first()
    # Simulate content generation
    return f"D file content for submission {submission_id}\nGenerated at {datetime.datetime.now()}"

def update_validation_rules() -> bool:
    """Update validation rules according to DB-2213"""
    try:
        session = Session()
        # Update specific rules as needed
        rule = session.query(ValidationRule).filter_by(rule_code='CFDA').first()
        if rule:
            rule.description = "Updated CFDA validation per DB-2213"
            rule.error_message = "Invalid CFDA number. Please check the CFDA catalog."
        
        session.commit()
        logger.info("Validation rules updated")
        return True
    except Exception as e:
        logger.error(f"Error updating validation rules: {str(e)}")
        session.rollback()
        return False

def derive_funding_agency_code() -> bool:
    """Derive FundingAgencyCode for improved data quality"""
    try:
        session = Session()
        submissions = session.query(PublishedAwardFinancialAssistance).filter(
            PublishedAwardFinancialAssistance.funding_agency_code == None
        ).all()
        
        for sub in submissions:
            # Simple derivation logic - in reality this would be more complex
            sub.funding_agency_code = sub.agency_code
        
        session.commit()
        logger.info(f"Derived funding agency codes for {len(submissions)} records")
        return True
    except Exception as e:
        logger.error(f"Error deriving funding agency codes: {str(e)}")
        session.rollback()
        return False

def update_ui_testing_schedule(test_name: str, start_date: str, end_date: str) -> bool:
    """Update UI testing schedule for stakeholder awareness"""
    try:
        test_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        session = Session()
        
        testing = UserTesting(
            test_name=test_name,
            test_date=test_date,
            status='scheduled',
            ui_improvements='Pending test results'
        )
        
        session.add(testing)
        session.commit()
        logger.info(f"Scheduled UI test: {test_name}")
        return True
    except Exception as e:
        logger.error(f"Error scheduling UI test: {str(e)}")
        return False

def disable_publish_button(submission_id: int) -> bool:
    """Disable publish button while processing"""
    try:
        session = Session()
        submission = session.query(Submission).filter_by(id=submission_id).first()
        if submission:
            # In a real app, this would update the UI state through a different mechanism
            logger.info(f"Publish button disabled for submission {submission_id}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error disabling publish button: {str(e)}")
        return False

def check_gtas_window() -> bool:
    """Check if we're in a GTAS window period (site lockdown)"""
    session = Session()
    now = datetime.datetime.now()
    window = session.query(GTASWindow).filter(
        GTASWindow.start_date <= now,
        GTASWindow.end_date >= now,
        GTASWindow.is_active == True
    ).first()
    
    return window is not None

def get_fpds_data(since_date: Optional[datetime.datetime] = None) -> List[Dict]:
    """Get FPDS data updated since a given date"""
    session = Session()
    query = session.query(PublishedAwardFinancialAssistance).filter(
        PublishedAwardFinancialAssistance.record_type == 'C'  # Contracts
    )
    
    if since_date:
        query = query.filter(PublishedAwardFinancialAssistance.updated_at >= since_date)
    
    return [row.__dict__ for row in query.all()]

def validate_duns_number(duns: str, action_type: str, action_date: str) -> bool:
    """Validate DUNS number according to business rules"""
    if len(duns) != 9 or not duns.isdigit():
        return False
    
    # Special cases for certain action types
    if action_type in ('B', 'C', 'D'):
        return True  # These action types have different validation rules
    
    try:
        action_date = datetime.datetime.strptime(action_date, '%Y-%m-%d')
        # In reality we would check SAM registration dates here
        return True
    except:
        return False

def get_fabs_submission(submission_id: int) -> Optional[Dict]:
    """Get FABS submission details with validation errors"""
    session = Session()
    submission = session.query(Submission).filter_by(id=submission_id, is_fabs=True).first()
    if not submission:
        return None
    
    result = {
        'id': submission.id,
        'agency': submission.agency,
        'status': submission.publish_status,
        'errors': json.loads(submission.validation_errors or '[]') if submission.validation_errors else [],
        'file_path': submission.file_path,
        'created_by': submission.created_by,
        'created_at': submission.created_at.isoformat(),
        'updated_at': submission.updated_at.isoformat()
    }
    
    return result

def get_updated_fabs_records(since: str) -> List[Dict]:
    """Get FABS records updated since a given date"""
    try:
        since_date = datetime.datetime.fromisoformat(since)
        session = Session()
        
        records = session.query(PublishedAwardFinancialAssistance).filter(
            PublishedAwardFinancialAssistance.updated_at >= since_date,
            PublishedAwardFinancialAssistance.record_type == 'A'  # Financial Assistance
        ).all()
        
        return [{
            'id': r.id,
            'agency_code': r.agency_code,
            'cfda_number': r.cfda_number,
            'updated_at': r.updated_at.isoformat()
        } for r in records]
    except Exception as e:
        logger.error(f"Error getting updated FABS records: {str(e)}")
        return []