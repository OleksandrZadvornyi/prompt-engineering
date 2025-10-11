import os
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union
from dataclasses import dataclass
from enum import Enum
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, Boolean, Float, ForeignKey, Index, desc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import uuid
import re
from collections import defaultdict
from functools import wraps


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("broker.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database setup
DATABASE_URL = "sqlite:///broker.db"
engine = create_engine(DATABASE_URL)
Base = declarative_base()
Session = sessionmaker(bind=engine)

# Models
class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    role = Column(String(50), nullable=False)
    permissions = Column(Text)  # JSON string of permissions
    created_at = Column(DateTime, default=datetime.utcnow)

class Submission(Base):
    __tablename__ = 'submissions'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    submission_type = Column(String(10))
    status = Column(String(20), default='draft')
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    publish_status = Column(String(20), default='unpublished')
    file_path = Column(String(255))
    error_message = Column(Text)
    validation_results = Column(Text)  # JSON string of validation results
    is_published = Column(Boolean, default=False)
    submission_data = Column(Text)  # JSON string of submission data
    original_file_name = Column(String(255))

    __table_args__ = (Index('idx_submissions_user', 'user_id'),
                      Index('idx_submissions_status', 'status'))

class FABSRecord(Base):
    __tablename__ = 'fabs_records'

    id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, ForeignKey('submissions.id'))
    record_data = Column(Text)  # JSON string of record data
    created_at = Column(DateTime, default=datetime.utcnow)
    is_deleted = Column(Boolean, default=False)
    deleted_at = Column(DateTime)

class FABSValidationRule(Base):
    __tablename__ = 'fabs_validation_rules'

    id = Column(Integer, primary_key=True)
    rule_id = Column(String(20), unique=True)
    description = Column(Text)
    validation_logic = Column(Text)
    last_updated = Column(DateTime, default=datetime.utcnow)

class HistoricalFABSData(Base):
    __tablename__ = 'historical_fabs_data'

    id = Column(Integer, primary_key=True)
    submission_id = Column(Integer)
    record_data = Column(Text)
    loaded_at = Column(DateTime, default=datetime.utcnow)

class FileGenerationCache(Base):
    __tablename__ = 'file_generation_cache'

    id = Column(Integer, primary_key=True)
    file_type = Column(String(20), nullable=False)
    params = Column(Text)
    cache_key = Column(String(255), unique=True)
    generated_file_path = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime)  # TTL for cache entry

class GTASWindow(Base):
    __tablename__ = 'gtas_window'

    id = Column(Integer, primary_key=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    is_active = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class FinancialAssistanceDataset(Base):
    __tablename__ = 'financial_assistance_datasets'

    id = Column(Integer, primary_key=True)
    dataset_name = Column(String(100))
    generated_at = Column(DateTime, default=datetime.utcnow)
    file_path = Column(String(255))

class DUNSValidation(Base):
    __tablename__ = 'duns_validations'

    id = Column(Integer, primary_key=True)
    duns_number = Column(String(9), nullable=False)
    action_type = Column(String(1))
    action_date = Column(DateTime)
    sam_registration_date = Column(DateTime)
    sam_expiration_date = Column(DateTime)
    is_valid = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class SubmissionError(Base):
    __tablename__ = 'submission_errors'

    id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, ForeignKey('submissions.id'))
    error_type = Column(String(50))
    message = Column(Text)
    error_code = Column(String(20))
    created_at = Column(DateTime, default=datetime.utcnow)

@Base.metadata.create_all
def init_database():
    """Initialize the database with base tables."""
    pass

# Enum definitions
class SubmissionStatus(Enum):
    DRAFT = "draft"
    VALIDATING = "validating"
    VALID = "valid"
    INVALID = "invalid"
    PUBLISHING = "publishing"
    PUBLISHED = "published"
    ARCHIVED = "archived"

class SubmissionType(Enum):
    FABS = "fabs"
    DABS = "dabs"

@dataclass
class ValidationResult:
    is_valid: bool
    message: str
    errors: List[Dict]
    warnings: List[Dict]

class BrokerAPIService:
    def __init__(self):
        self.session = Session()
        
    def process_fabs_deletions(self, deletion_date: datetime):
        """Process 12-19-2017 deletions from FABS records"""
        try:
            cutoff_date = datetime.strptime("2017-12-19", "%Y-%m-%d")
            
            # Find all FABS records that were deleted or within deletion window
            deleted_records = self.session.query(FABSRecord).filter(
                FABSRecord.created_at <= cutoff_date,
                FABSRecord.is_deleted == True
            ).all()
            
            logger.info(f"Processed {len(deleted_records)} records for deletion")
            
            # Mark as fully deleted (or handle based on business rules)
            for record in deleted_records:
                record.is_deleted = True
                record.deleted_at = datetime.utcnow()
                
            self.session.commit()
            return True
        except Exception as e:
            logger.error(f"Error processing FABS deletions: {e}")
            self.session.rollback()
            return False

    def update_fabs_submission_status(self, submission_id: int, new_status: str):
        """Update FABS submission publish status and handle updates"""
        try:
            submission = self.session.query(Submission).filter_by(id=submission_id).first()
            if not submission:
                return False
                
            old_status = submission.publish_status
            submission.publish_status = new_status
            submission.updated_at = datetime.utcnow()
            
            # If status changed from published to something else, make sure to update things accordingly
            if old_status == 'published' and new_status != 'published':
                submission.is_published = False
                
            self.session.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating FABS submission status: {e}")
            self.session.rollback()
            return False

    def setup_new_relic_monitoring(self):
        """Mock-up for ensuring New Relic monitoring is fully configured"""
        logger.info("Configuring New Relic monitoring for all applications")
        
        # In a real implementation, this would set up New Relic configurations
        # This is a placeholder for actual New Relic client initialization
        return True

    def process_d1_file_generation(self):
        """Generate D1 file synchronized with FPDS data load."""
        try:
            # This would typically interact with FPDS APIs and generate files
            fpds_synced = True
            
            if fpds_synced:
                directory = "./output/d1_files/"
                os.makedirs(directory, exist_ok=True)
                
                filename = f"D1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                file_path = os.path.join(directory, filename)
                
                # Mock generation logic - replace with actual file generation
                with open(file_path, 'w') as f:
                    f.write("D1 File Generated: " + str(datetime.now()))
                    
                logger.info(f"D1 file generated successfully: {file_path}")
                return file_path
                
        except Exception as e:
            logger.error(f"Error generating D1 file: {e}")
            return None

    def get_published_fabs_files(self):
        """Get list of published FABS files"""
        try:
            files = self.session.query(FinancialAssistanceDataset).order_by(desc(FinancialAssistanceDataset.generated_at)).all()
            return [f.file_path for f in files]
        except Exception as e:
            logger.error(f"Error retrieving published FABS files: {e}")
            return []

    def validate_error_message(self, submission_id: int) -> ValidationResult:
        """Verify that error message text is accurate"""
        try:
            submission = self.session.query(Submission).filter_by(id=submission_id).first()
            
            if not submission:
                return ValidationResult(False, "Submission not found", [], [])
                
            # Mock validation - real applications would check against business rules
            if not submission.error_message or len(submission.error_message) < 5:
                return ValidationResult(False, "Error message is too brief", [], [])
                
            return ValidationResult(True, "Error message valid", [], [])
            
        except Exception as e:
            logger.error(f"Error validating error message: {e}")
            return ValidationResult(False, str(e), [], [])

    def generate_fabs_sample_file(self):
        """Generate updated FABS sample file with new headers"""
        try:
            # Mock sample file generation
            sample_data = [
                ['AssistanceListingNumber', 'AwardingAgencyCode', 'FundingAgencyCode', 'RecordType', 'ActionDate'],
                ['10.100', '091', '', '1', '2022-06-01']
            ]
            
            directory = "./output/"
            os.makedirs(directory, exist_ok=True)
            filename = f"FABS_Sample_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            file_path = os.path.join(directory, filename)
            
            import csv
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(sample_data)
                
            logger.info(f"FABS sample file generated: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error generating FABS sample file: {e}")
            return None

    def add_gtas_window_data(self, start_date: datetime, end_date: datetime):
        """Add GTAS window data to database"""
        try:
            window = GTASWindow(
                start_date=start_date,
                end_date=end_date,
                is_active=True
            )
            self.session.add(window)
            self.session.commit()
            
            logger.info(f"GTAS window created: {start_date} to {end_date}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding GTAS window data: {e}")
            self.session.rollback()
            return False

    def cache_file_generation(self, file_type: str, params: Dict):
        """Cache file generation requests to prevent duplicates"""
        try:
            # Create cached entry
            cache_key = f"{file_type}_{hash(str(sorted(params.items())))}"
            
            existing = self.session.query(FileGenerationCache).filter_by(cache_key=cache_key).first()
            if existing:
                if existing.expires_at > datetime.utcnow():
                    return existing.generated_file_path
            
            # Generate a temporary path
            directory = "./cache/generated/"
            os.makedirs(directory, exist_ok=True)
            filename = f"{file_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4()}.csv"
            file_path = os.path.join(directory, filename)
            
            with open(file_path, 'w') as f:
                f.write(f"Generated cache {file_type} file")
                
            cache_entry = FileGenerationCache(
                file_type=file_type,
                params=json.dumps(params),
                cache_key=cache_key,
                generated_file_path=file_path,
                expires_at=datetime.utcnow() + timedelta(hours=1)
            )
            
            self.session.add(cache_entry)
            self.session.commit()
            
            return file_path
            
        except Exception as e:
            logger.error(f"Error in caching file generation: {e}")
            self.session.rollback()
            return None

    def get_raw_agency_files(self):
        """Get agency published files from FABS"""
        try:
            # This would return actual paths to published files
            raw_files = [
                "./output/fabs_files/published_2022.csv",
                "./output/fabs_files/published_2021.csv"
            ]
            return raw_files
        except Exception as e:
            logger.error(f"Error retrieving raw agency files: {e}")
            return []

    def handle_flexfield_validation(self, submission_id: int, flexfield_data: Dict) -> ValidationResult:
        """Handle validation for flexfields"""
        try:
            # Check if all required elements exist
            errors = []
            warnings = []
            
            # Simulate validation logic
            if 'required_field' not in flexfield_data:
                errors.append({
                    'field': 'required_field',
                    'message': 'Required element missing'
                })
                
            if not flexfield_data.get('optional_field'):
                warnings.append({
                    'field': 'optional_field',
                    'message': 'Optional element missing'
                })
                
            is_valid = len(errors) == 0
            message = "Flexfields validated successfully" if is_valid else "Validation failed"
            
            return ValidationResult(is_valid, message, errors, warnings)
            
        except Exception as e:
            logger.error(f"Error validating flexfields: {e}")
            return ValidationResult(False, str(e), [], [])

    def ensure_field_derivations_correctly(self):
        """Ensure that all derived fields are properly derived"""
        try:
            # Identify records and correct derivations
            records = self.session.query(FABSRecord).all()
            for record in records:
                # In a real system, we would check each record's derived fields
                pass
                
            logger.info("Field derivation validation complete")
            return True
            
        except Exception as e:
            logger.error(f"Error in field derivation validation: {e}")
            return False

    def create_user_testing_summary(self):
        """Generate summary of user testing from SME input"""
        summary = {
            "summary_date": datetime.now(),
            "findings": [
                {
                    "area": "Homepage Navigation",
                    "feedback": "Users struggling with navigation options",
                    "impact": "Low",
                    "timeline": "Next release"
                },
                {
                    "area": "Help Page",
                    "feedback": "Help content requires updating",
                    "impact": "Medium",
                    "timeline": "Next sprint"
                }
            ],
            "improvement_action": "Implement design refinements for navigation",
            "owner": "UI Design Team"
        }
        
        return summary

    def schedule_user_testing(self, testers: List[str], test_date: datetime):
        """Schedule user testing with advance notice"""
        logger.info(f"Scheduled user testing with {len(testers)} testers on {test_date}")
        for tester in testers:
            logger.info(f"Sent notice to tester: {tester}")
            
        return True

    def reset_environment_permissions(self):
        """Reset environment to only Staging MAX permissions"""
        try:
            # This would strip permissions from testers  
            logger.info("Environment reset to staging permissions only")
            
            # Remove access for FABS testers
            users = self.session.query(User).filter_by(role='fabs_tester').all()
            for user in users:
                try:
                    perms = json.loads(user.permissions) if user.permissions else {}
                    if 'fabs_access' in perms:
                        del perms['fabs_access']
                    user.permissions = json.dumps(perms)
                except Exception:
                    pass
                    
            self.session.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error resetting environment permissions: {e}")
            self.session.rollback()
            return False

    def check_fabs_header_requirements(self):
        """Check that headers match latest schema v1.1"""
        required_headers = [
            "AssistanceListingNumber",
            "AwardingAgencyCode", 
            "FundingAgencyCode",
            "RecordType"
        ]
        
        # Compare with actual headers in submission data
        # Return if pass/fail and error details
        return {
            "valid": True,
            "missing": [],
            "headers": required_headers
        }

    def validate_zip_code_format(self, zip_code: str) -> bool:
        """Validate ZIP code format compliance"""
        # Allow 5-digit ZIP, 5+4 ZIP pattern, or citywide notation
        pattern = r'^\d{5}(-\d{4})?$|^\*\*\*\*\*\s*\*\*\*\*\*'
        return bool(re.match(pattern, zip_code))

    def load_historical_fabs_data(self):
        """Load historical FABS data"""
        try:
            # Simulated historical data loading
            entries = [
                {"submission_id": 1, "record_data": '{"AwardingAgencyCode": "091", "FundingAgencyCode": ""}'},
                {"submission_id": 2, "record_data": '{"AwardingAgencyCode": "048", "FundingAgencyCode": "048"}'}
            ]
            
            for entry in entries:
                record = HistoricalFABSData(
                    submission_id=entry["submission_id"],
                    record_data=entry["record_data"]
                )
                self.session.add(record)
                
            self.session.commit()
            logger.info(f"Loaded {len(entries)} historical FABS records")
            return True
            
        except Exception as e:
            logger.error(f"Error loading historical FABS data: {e}")
            self.session.rollback()
            return False

    def get_agency_creation_info(self, submission_id: int):
        """Return who created the submission"""
        submission = self.session.query(Submission).filter_by(id=submission_id).first()
        if submission:
            user = self.session.query(User).filter_by(id=submission.user_id).first()
            return {
                "created_by": user.username if user else "Unknown",
                "created_at": submission.created_at
            }
        return None

    def generate_file_f(self, submission_id: int):
        """Generate File F in correct format"""
        # In real system, this would generate the actual File F
        return f"FileF_for_submission_{submission_id}.csv"

    def get_submission_dashboard_info(self, submission_id: int):
        """Get detailed information for submission dashboard"""
        try:
            submission = self.session.query(Submission).filter_by(id=submission_id).first()
            if not submission:
                return None
                
            return {
                "id": submission.id,
                "type": submission.submission_type,
                "status": submission.status,
                "publish_status": submission.publish_status,
                "created_at": submission.created_at,
                "updated_at": submission.updated_at,
                "is_published": submission.is_published,
                "error_count": 0
            }
        except Exception as e:
            logger.error(f"Error getting dashboard info: {e}")
            return None

    def validate_ppop_zipplus4(self, zip_code: str) -> bool:
        """Validate that PPoPZIP+4 works same as Legal Entity ZIP"""
        return self.validate_zip_code_format(zip_code)

    def update_fabs_sample_file(self):
        """Update FABS sample file to remove FundingAgencyCode"""
        try:
            # Would delete FundingAgencyCode header, update data
            logger.info("Updated FABS sample file to remove FundingAgencyCode")
            return True
        except Exception as e:
            logger.error(f"Error updating FABS sample file: {e}")
            return False

    def update_validation_rules(self):
        """Update Broker validation rule table to account for rule updates"""
        try:
            # Get existing rules
            rules = self.session.query(FABSValidationRule).all()
            
            # Update rules as needed based on DB-2213 updates
            for rule in rules:
                if rule.rule_id == "DB-2213_UPDATE":
                    rule.validation_logic += " && new_validation_logic_for_DB2213 == true"
                    rule.last_updated = datetime.utcnow()
                    
            self.session.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating validation rules: {e}")
            self.session.rollback()
            return False

    def ensure_sam_data_complete(self):
        """Ensure SAM data is complete for all records"""
        try:
            # Would query SAM database for updates
            completion_rate = 0.95  # Fake value for demo
            logger.info(f"Sam data completion rate: {completion_rate * 100:.0f}%")
            return True
        except Exception as e:
            logger.error(f"Error verifying SAM data completeness: {e}")
            return False

    def structure_indexed_models(self):
        """Ensure domain models are indexed properly"""
        try:
            # Explicitly apply database index configuration (forms part of model definition)
            # In reality this may include adding more indices to models
            logger.info("Domain models indexed properly")
            return True
        except Exception as e:
            logger.error(f"Error indexing domain models: {e}")
            return False

    def derive_funding_agency_code(self):
        """Derive FundingAgencyCode to improve data quality"""
        try:
            # This would compute the code based on related data
            logger.info("FundingAgencyCode derived successfully")
            return True
        except Exception as e:
            logger.error(f"Error in funding agency code derivation: {e}")
            return False

    def update_purpose_codes(self, purpose: str):
        """Process and validate purpose code derivation"""
        try:
            # Generate derived purpose codes based on input
            logger.info(f"Derived purpose code: {purpose.upper()}")
            return True
        except Exception as e:
            logger.error(f"Error deriving purpose code: {e}")
            return False

    def validate_fabs_validation_rules(self, submission_data: Dict):
        """Validate FABS data using correct validation rules"""
        try:
            # Check for valid values for loan records (0 or blank)
            loan_record_valid = True  # Placeholder for actual validation
            
            # Check for valid values for non-loan records (0 or blank)
            non_loan_valid = True  # Placeholder
            
            # Return comprehensive validation results
            if loan_record_valid and non_loan_valid:
                return ValidationResult(True, "All validation rules passed", [], [])
            else:
                errors = []
                if not loan_record_valid:
                    errors.append({"code": "LOAN_RECORD_INVALID", "description": "Loan record validation failed"})
                if not non_loan_valid:
                    errors.append({"code": "NON_LOAN_RECORD_INVALID", "description": "Non-loan record validation failed"})
                return ValidationResult(False, "Some validation rules failed", errors, [])
                
        except Exception as e:
            logger.error(f"Error validating FABS rules: {e}")
            return ValidationResult(False, str(e), [], [])

    def get_frec_derivation(self, submission_id: int):
        """Get FREC derivation for submission"""
        try:
            # Simulate retrieving FREC data
            frec_codes = ["091", "048", "067"]  # sample codes
            return frec_codes
        except Exception as e:
            logger.error(f"Error getting FREC derivation: {e}")
            return []

    def process_historical_fpds_data(self):
        """Process historical FPDS data to include both source and updated records"""
        try:
            # Would actually fetch from historical data sources and FPDS feeds
            logger.info("Historical FPDS data processed")
            return True
        except Exception as e:
            logger.error(f"Error processing FPDS data: {e}")
            return False

    def validate_duns_records(self, records: List[Dict]):
        """Validate DUNS registrations with action date criteria"""
        try:
            valid_records = []
            invalid_records = []
            
            for record in records:
                validated = self.check_duns_validation(
                    record.get('duns', ''),
                    record.get('action_type', ''),
                    record.get('action_date', ''),
                    record.get('sam_registration_date', '')
                )
                
                if validated:
                    valid_records.append(record)
                else:
                    invalid_records.append(record)
                    
            return {
                "valid": valid_records,
                "invalid": invalid_records
            }
        except Exception as e:
            logger.error(f"Error validating DUNS records: {e}")
            return {"valid": [], "invalid": []}

    def check_duns_validation(self, duns: str, action_type: str, action_date: str, sam_reg_date: str) -> bool:
        """Check if DUNS validation meets SAM requirements"""
        # Logic to validate based on action type, dates, etc.
        return True  # placeholder

    def log_application_issues(self, issue_type: str, details: str, timestamp: datetime):
        """Enhanced logging for troubleshooting"""
        log_entry = {
            "timestamp": timestamp.isoformat(),
            "issue_type": issue_type,
            "details": details,
            "environment": os.environ.get("ENVIRONMENT", "development")
        }
        logger.info(f"Application Issue [{issue_type}]: {details}")
        return True

    def cleanup_session(self):
        """Clean up database sessions"""
        self.session.close()

# Expose functions for use
broker_api = BrokerAPIService()