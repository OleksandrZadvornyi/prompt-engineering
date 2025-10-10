# Module 1: Data processing and validation logic

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
import hashlib
import json
import logging
import re
import sqlite3
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from typing import NamedTuple

class ValidationError(Exception):
    """Custom exception for validation errors"""
    pass

class SubmissionStatus(Enum):
    DRAFT = "draft"
    VALIDATING = "validating"
    VALIDATED = "validated"
    PUBLISHING = "publishing"
    PUBLISHED = "published"

@dataclass
class ValidationRule:
    id: int
    name: str
    description: str
    sql_query: str
    category: str

@dataclass
class Submission:
    submission_id: str
    agency_code: str
    status: SubmissionStatus
    created_at: datetime
    updated_at: datetime
    published_at: Optional[datetime]
    
class DataProcessor(ABC):
    @abstractmethod
    def process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def validate_data(self, data: Dict[str, Any]) -> List[str]:
        pass

class FABSDataProcessor(DataProcessor):
    def __init__(self):
        self.validation_rules = []
        self._load_validation_rules()
        
    def _load_validation_rules(self):
        # Initialize validation rules based on DB-2213
        self.validation_rules = [
            ValidationRule(1, "zero_and_blank_loan", "Accept zeros and blanks in loan records", 
                          "SELECT * FROM loan_records WHERE field IN ('0', '')", "loan"),
            ValidationRule(2, "zero_and_blank_nonloan", "Accept zeros and blanks in non-loan records",
                          "SELECT * FROM nonloan_records WHERE field IN ('0', '')", "nonloan"),
            ValidationRule(3, "legal_entity_line3_max_length", "Check LegalEntityAddressLine3 max length",
                          "SELECT * FROM records WHERE LENGTH(legal_entity_line3) > 100", "address"),
        ]
    
    def process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        # Apply transformations for FABS data processing
        transformed = data.copy()
        
        # Handle FREC derivation
        if 'frec_code' not in transformed:
            transformed['frec_code'] = self._derive_frec_code(transformed.get('agency_code', ''))
            
        # Process zip codes
        if 'ppop_zip' in transformed:
            transformed['popp_zip'] = self._normalize_zip(transformed['popp_zip'])
            
        return transformed
    
    def validate_data(self, data: Dict[str, Any]) -> List[str]:
        errors = []
        
        # Check for schema compliance
        if 'funding_agency_code' in data and data['funding_agency_code'].lower() == 'remove':
            errors.append("FundingAgencyCode header has been removed from schema")
            
        # Check DUNS validation rules
        duns_action_types = ['B', 'C', 'D']
        if (data.get('duns') and 
            data.get('action_type') in duns_action_types and 
            data.get('duns_registered') is False):
            errors.append("DUNS must be valid when ActionType is B, C, or D")
            
        return errors
    
    def _derive_frec_code(self, agency_code: str) -> str:
        # Simplified algorithm for demonstration
        return f"FREC-{agency_code[:6]}"
    
    def _normalize_zip(self, zip_code: str) -> str:
        # Normalize zip code format as per standard requirements
        if not zip_code:
            return ""
            
        # Remove any non-digits
        digits_only = re.sub(r'\D', '', zip_code)
        
        # Pad to 5 digits if needed
        if len(digits_only) == 4:
            return digits_only.zfill(5)
        elif len(digits_only) == 9:
            return digits_only[:5] + '-' + digits_only[5:]
        else:
            return digits_only.zfill(5)[:5]

class FPDSDataProcessor(DataProcessor):
    def __init__(self):
        self.last_update_time = None
        
    def process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        transformed = data.copy()
        
        # Sync with FPDS data load timing
        transformed['last_updated'] = datetime.utcnow()
        return transformed
    
    def validate_data(self, data: Dict[str, Any]) -> List[str]:
        errors = []
        
        # Check if data needs regeneration or validation
        if (self.last_update_time and 
            data.get('last_updated', datetime.min) < self.last_update_time):
            errors.append("No new FPDS data to regenerate")
            
        return errors

# Module 2: System Configuration and API management

class SystemConfig:
    """Configuration manager for broker system"""
    
    def __init__(self):
        self.configurations = {
            'fabs': {
                'schema_version': '1.1',
                'validation_rules': [],
                'sample_file': '/static/sample_fabs_1.1.csv'
            },
            'fpds': {
                'data_update_frequency': 'daily',
                'data_retention_period': 5
            },
            'ui': {
                'ui_version': 'v2',
                'design_sprint_schedule': ['round1', 'round2'],
                'help_page_edits': ['round1_complete', 'round2_in_progress']
            }
        }
    
    def get_setting(self, section: str, key: str) -> Any:
        return self.configurations.get(section, {}).get(key)
    
    def set_setting(self, section: str, key: str, value: Any):
        if section not in self.configurations:
            self.configurations[section] = {}
        self.configurations[section][key] = value

class ResourceManager:
    """Manages resources like files and data sources"""
    
    def __init__(self):
        self.resource_locks = {}
        self.cache = {}
    
    def acquire_resource_lock(self, resource_id: str, timeout_minutes: int = 5) -> bool:
        """Acquire lock on a resource to prevent duplicate operations"""
        if resource_id in self.resource_locks:
            return False
            
        self.resource_locks[resource_id] = datetime.utcnow()
        return True
    
    def release_resource_lock(self, resource_id: str):
        """Release lock on resource"""
        self.resource_locks.pop(resource_id, None)
    
    def get_cached_result(self, cache_key: str) -> Optional[Any]:
        """Retrieve cached result"""
        if cache_key in self.cache and self._is_cache_valid(cache_key):
            return self.cache[cache_key]
        return None
    
    def set_cached_result(self, cache_key: str, data: Any, ttl_minutes: int = 10):
        """Set cache for given result"""
        self.cache[cache_key] = {
            'data': data,
            'timestamp': datetime.utcnow(),
            'ttl': ttl_minutes
        }

class APICallbackHandler:
    """Handles various API callbacks from different modules"""
    
    def __init__(self):
        self.callbacks = {}
    
    def register_callback(self, event_type: str, handler_func):
        """Register callback for specific events"""
        self.callbacks[event_type] = handler_func
    
    def trigger_callback(self, event_type: str, **kwargs):
        """Trigger registered callback"""
        if event_type in self.callbacks:
            return self.callbacks[event_type](**kwargs)
        return None

# Module 3: FABS and FPDS Submission Systems

class SubmissionManager:
    """Handles submission lifecycle and tracking"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.submissions = {}  # In-memory storage
        self.validator = FABSValidationEngine()
        
    def create_submission(self, agency_code: str, data: Dict[str, Any], 
                         submission_type: str = 'fabs'):
        """Create new submission record"""
        submission_id = hashlib.md5(str(datetime.utcnow()).encode()).hexdigest()[:16]
        
        submission = Submission(
            submission_id=submission_id,
            agency_code=agency_code,
            status=SubmissionStatus.DRAFT,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            published_at=None
        )
        
        self.submissions[submission_id] = submission
        
        # Create entry in database
        cursor = self.db.execute("""
            INSERT INTO submissions (
                submission_id, agency_code, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?)
        """, (submission_id, agency_code, SubmissionStatus.DRAFT.value, 
              submission.created_at, submission.updated_at))
        
        return submission_id
    
    def validate_submission(self, submission_id: str) -> List[str]:
        """Validate submission data"""
        if submission_id not in self.submissions:
            raise ValueError("Submission not found")
        
        submission = self.submissions[submission_id]
        submission.status = SubmissionStatus.VALIDATING
        submission.updated_at = datetime.utcnow()
        
        # Get raw data for validation
        raw_data = self._get_submission_data(submission_id)
        
        errors = self.validator.validate(raw_data)
        
        # Update submission status based on validation
        if not errors:
            submission.status = SubmissionStatus.VALIDATED
        else:
            submission.status = SubmissionStatus.DRAFT
            
        submission.updated_at = datetime.utcnow()
        cursor = self.db.execute("""
            UPDATE submissions SET 
                status=?, updated_at=? WHERE submission_id=?
        """, (submission.status.value, submission.updated_at, submission_id))
        
        return errors
    
    def publish_submission(self, submission_id: str) -> bool:
        """Publish validated submission"""
        if submission_id not in self.submissions:
            raise ValueError("Submission not found")
            
        submission = self.submissions[submission_id]
        
        if submission.status != SubmissionStatus.VALIDATED:
            raise ValueError("Submission must be validated first")
            
        try:
            # Mark as publishing
            submission.status = SubmissionStatus.PUBLISHING
            submission.updated_at = datetime.utcnow()
            submission.published_at = datetime.utcnow()
            
            # Update database
            cursor = self.db.execute("""
                UPDATE submissions SET 
                    status=?, updated_at=?, published_at=? WHERE submission_id=?
            """, (submission.status.value, submission.updated_at, 
                  submission.published_at, submission_id))
            
            # Process real-time publishing
            self._process_publishing(submission)
            
            # Finalize
            submission.status = SubmissionStatus.PUBLISHED
            cursor = self.db.execute("""
                UPDATE submissions SET 
                    status=? WHERE submission_id=?
            """, (submission.status.value, submission_id))
            
            return True
        except Exception as e:
            # Revert status on failure
            submission.status = SubmissionStatus.VALIDATED
            cursor = self.db.execute("""
                UPDATE submissions SET 
                    status=? WHERE submission_id=?
            """, (submission.status.value, submission_id))
            
            raise e
    
    def _get_submission_data(self, submission_id: str) -> Dict[str, Any]:
        """Get raw submission data from store"""
        # In implementation this would query the actual data source
        return {'data': 'mock_data_for_submission_{}'.format(submission_id)}
    
    def _process_publishing(self, submission: Submission):
        """Process actual publishing steps"""
        print(f"Publishing submission {submission.submission_id}")

class FABSValidationEngine:
    """Core validation logic engine"""
    
    def __init__(self):
        self.rules = []
        self._build_rules()
        
    def _build_rules(self):
        self.rules = [
            "validate_header_fields",
            "validate_zip_codes",
            "validate_duns_validity",
            "validate_cfda_values",
            "validate_funding_agency_code"
        ]
    
    def validate(self, data: Dict[str, Any]) -> List[str]:
        errors = []
        
        # Apply all validation rules
        for rule in self.rules:
            try:
                getattr(self, rule)(data)
            except ValidationError as e:
                errors.append(str(e))
                
        return errors
    
    def validate_header_fields(self, data: Dict[str, Any]):
        # Implementation would check for header fields compliance
        if 'funding_agency_code' in data and data['funding_agency_code'] == 'REMOVED':
            raise ValidationError("FundingAgencyCode header has been removed from schema")
    
    def validate_zip_codes(self, data: Dict[str, Any]):
        zip_fields = ['ppop_zip', 'legal_entity_zip']
        for field in zip_fields:
            if field in data:
                zip_val = data[field]
                if not self._is_valid_zip(zip_val):
                    raise ValidationError(f"Invalid ZIP code format: {zip_val}")
    
    def _is_valid_zip(self, zip_code: str) -> bool:
        pattern = r'^\d{5}(-\d{4})?$'
        return re.match(pattern, zip_code) is not None
    
    def validate_duns_validity(self, data: Dict[str, Any]):
        required_fields = ['duns', 'action_type']
        for field in required_fields:
            if field not in data:
                raise ValidationError(f"Missing required field: {field}")
                
        # Specific validation based on action type
        if data.get('duns') and data.get('action_type') in ['B', 'C', 'D']:
            # Placeholder for SAM validation simulation
            pass
    
    def validate_cfda_values(self, data: Dict[str, Any]):
        cfda_field = data.get('cfda')
        if cfda_field and cfda_field not in ["01.001", "01.002"]:
            raise ValidationError(f"Invalid CFDA value: {cfda_field}")
            
    def validate_funding_agency_code(self, data: Dict[str, Any]):
        # Placeholder to ensure proper handling for funding agency code
        pass

# Module 4: Database schema and migration support

class MigrationHelper:
    """Helper for handling database migrations and data consistency"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.connection = sqlite3.connect(connection_string)
        
    def apply_migration(self, version: str):
        """Apply specific migration"""
        print(f"Applying migration {version}")
        
        if version == "DB-2213":
            self._apply_db_2213_migration()
        elif version == "FABS-header-update":
            self._apply_fabs_header_change()
        elif version == "FPDS-historical-load":
            self._apply_fpds_historical_load()
    
    def _apply_db_2213_migration(self):
        """Update validation rule table structure"""
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS validation_rules (
                rule_id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                description TEXT,
                sql_query TEXT NOT NULL,
                category TEXT NOT NULL DEFAULT 'general'
            );
        """)
        
        self.connection.commit()
        
    def _apply_fabs_header_change(self):
        """Remove FundingAgencyCode column as per schema v1.1"""
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS fabs_submission_metadata (
                submission_id TEXT PRIMARY KEY REFERENCES submissions(submission_id),
                schema_version TEXT,
                funding_agency_disabled BOOLEAN DEFAULT FALSE
            );
        """)
        
        self.connection.commit()

class DatabaseManager:
    """Main interface for database operations"""
    
    def __init__(self, connection_string: str):
        self.connection = sqlite3.connect(connection_string, check_same_thread=False)
        
    def create_tables(self):
        """Initialize all tables"""
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS submissions (
                submission_id TEXT PRIMARY KEY,
                agency_code TEXT,
                status TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                published_at TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_submissions_status ON submissions(status);
            CREATE INDEX IF NOT EXISTS idx_submissions_created ON submissions(created_at);
        """)
        
        self.connection.commit()
    
    def get_submission(self, submission_id: str) -> Optional[Tuple]:
        """Get submission details"""
        cursor = self.connection.execute("""
            SELECT * FROM submissions WHERE submission_id=?
        """, (submission_id,))
        return cursor.fetchone()
    
    def update_submission_status(self, submission_id: str, status: str):
        """Update submission status"""
        timestamp = datetime.utcnow()
        self.connection.execute("""
            UPDATE submissions SET status=?, updated_at=? WHERE submission_id=?
        """, (status, timestamp, submission_id))
        
        self.connection.commit()

# Module 5: Core system controller and orchestrator

class BrokerSystem:
    """Main system controller"""
    
    def __init__(self):
        self.config = SystemConfig()
        self.data_processor = FABSDataProcessor()
        self.fpds_processor = FPDSDataProcessor()
        self.submission_manager = None
        self.resource_manager = ResourceManager()
        self.api_handler = APICallbackHandler()
        self.db_manager = DatabaseManager(':memory:')
        self.db_manager.create_tables()
        
        self._setup_callbacks()
    
    def initialize_system(self):
        """Initialize and configure all system modules"""
        self.submission_manager = SubmissionManager(self.db_manager.connection)
        print("Broker system initialized successfully")
        
    def _setup_callbacks(self):
        """Setup internal callbacks"""
        self.api_handler.register_callback("submission_created", self._on_submission_created)
        self.api_handler.register_callback("publication_started", self._on_publication_start)
        self.api_handler.register_callback("data_processing_completed", self._on_processing_complete)
    
    def _on_submission_created(self, submission_id: str):
        """Callback when a new submission is created"""
        print(f"New submission created: {submission_id}")
        
    def _on_publication_start(self, submission_id: str):
        """Callback when publication starts"""
        print(f"Publication started for submission: {submission_id}")
        
    def _on_processing_complete(self, submission_id: str):
        """Callback when processing is complete"""
        print(f"Processing completed for submission: {submission_id}")
        
    def process_fabs_submission(self, agency_code: str, data: Dict[str, Any]) -> str:
        """Process FABS submission request"""
        # Validate incoming data
        clean_data = self.data_processor.process_data(data)
        errors = self.data_processor.validate_data(clean_data)
        
        if errors:
            raise ValidationError("Data validation failed: " + '; '.join(errors))
        
        # Create and track submission
        submission_id = self.submission_manager.create_submission(agency_code, clean_data)
        self.api_handler.trigger_callback("submission_created", submission_id=submission_id)
        
        # Perform validation
        validation_errors = self.submission_manager.validate_submission(submission_id)
        
        if validation_errors:
            raise ValidationError("Submission validation failed: " + '; '.join(validation_errors))
            
        return submission_id
    
    def publish_submission(self, submission_id: str) -> bool:
        """Publish validated submission"""
        self.api_handler.trigger_callback("publication_started", submission_id=submission_id)
        success = self.submission_manager.publish_submission(submission_id)
        self.api_handler.trigger_callback("data_processing_completed", submission_id=submission_id)
        return success

# Module 6: Utility classes for file handling and export

class FileWriter:
    """Handles file writing operations"""
    
    def __init__(self):
        self.export_queue = []
        
    def write_submission_to_file(self, submission_id: str, filename: str, 
                                submission_data: Dict[str, Any]):
        """Write submission to CSV file"""
        try:
            with open(filename, 'w') as f:
                # Write headers using schema v1.1 specification
                headers = list(submission_data.keys())
                f.write(','.join(headers) + '\n')
                
                # Write row data
                values = [str(submission_data.get(h, '')) for h in headers]
                f.write(','.join(values) + '\n')
                
            print(f"Written submission {submission_id} to {filename}")
            return True
            
        except Exception as e:
            print(f"Error writing submission to file: {e}")
            return False

class HistoricalDataLoader:
    """Handles loading of historical data including FABS/FEDERAL_DATA"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        
    def load_historical_fabs(self):
        """Load historical FABS data with correct FREC derivations"""
        print("Loading historical FABS data with FREC derivations...")
        # This would normally pull from external sources and insert into database
        
    def load_historical_fpds(self):
        """Load historical FPDS data"""
        print("Loading historical FPDS data...")
        # Similar approach
        
    def sync_with_source(self, source_type: str) -> bool:
        """Sync data source with latest information"""
        if source_type == "fabs":
            self.load_historical_fabs()
            return True
        elif source_type == "fpds":
            self.load_historical_fpds()
            return True
        return False

# Final integration and execution
if __name__ == "__main__":
    # Initialize the complete system
    system = BrokerSystem()
    system.initialize_system()
    
    # Example usage scenarios from user stories
    try:
        # Simulate a submission from an agency
        test_data = {
            'agency_code': '123456',
            'funding_agency_code': 'REMOVED',  # This should trigger a validation error
            'duns': '123456789',
            'action_type': 'B',
            'ppop_zip': '12345',
            'cfda': '01.001'
        }
        
        print("Starting FABS submission process:")
        submission_id = system.process_fabs_submission('123456', test_data)
        print(f"Submission created with ID: {submission_id}")
        
        print("Attempting to publish...")
        success = system.publish_submission(submission_id)
        print(f"Publication successful: {success}")
        
    except ValidationError as e:
        print(f"Validation error occurred: {e}")
    except Exception as e:
        print(f"System error occurred: {e}")

    # Test historical data loading
    historical_loader = HistoricalDataLoader(system.db_manager.connection)
    historical_loader.sync_with_source("fabs")
    historical_loader.sync_with_source("fpds")

    print("System running successfully!")