import sqlite3
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import json
import logging
from enum import Enum
from dataclasses import dataclass
from abc import ABC, abstractmethod

# Logger setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SubmissionType(Enum):
    FABS = "FABS"
    DABS = "DABS"

@dataclass
class Submission:
    id: str
    type: SubmissionType
    status: str
    timestamp: datetime
    validated_data: Optional[Dict] = None
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []

class ValidationRule:
    def __init__(self, rule_id: str, description: str, validation_function):
        self.rule_id = rule_id
        self.description = description
        self.validation_function = validation_function
    
    def validate(self, data) -> bool:
        try:
            return self.validation_function(data)
        except Exception as e:
            logger.error(f"Validation failed for rule {self.rule_id}: {e}")
            return False

class DatabaseManager:
    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create submissions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS submissions (
                id TEXT PRIMARY KEY,
                submission_type TEXT NOT NULL,
                status TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                validated_data TEXT,
                errors TEXT
            )
        ''')
        
        # Create validations table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS validation_rules (
                rule_id TEXT PRIMARY KEY,
                description TEXT,
                enabled BOOLEAN DEFAULT 1
            )
        ''')
        
        # Create resources table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS resources (
                id INTEGER PRIMARY KEY,
                title TEXT,
                content TEXT,
                last_updated DATETIME
            )
        ''')
        
        conn.commit()
        conn.close()

class FABSDataProcessor:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.validation_rules = []
        self._setup_validation_rules()
    
    def _setup_validation_rules(self):
        self.validation_rules = [
            ValidationRule("rule_001", "Zero and blank values accepted for loans", 
                          lambda x: 'loan' in str(x.get('record_type', '')).lower() or 
                                   x.get('funding_agency_code') in [None, '', 0]),
            ValidationRule("rule_002", "PPoPZIP+4 validation", 
                          lambda x: x.get('p_pop_zip_plus_four') is not None),
            ValidationRule("rule_003", "FundingAgencyCode derivation", 
                          lambda x: x.get('funding_agency_code') is not None),
        ]
    
    def process_submission(self, submission_data: Dict) -> Dict:
        """Process FABS submission with validation"""
        submission = Submission(
            id=submission_data.get("submission_id"),
            type=SubmissionType.FABS,
            status="processing",
            timestamp=datetime.now(),
            validated_data=submission_data
        )
        
        # Apply validation rules
        validation_errors = []
        for rule in self.validation_rules:
            if not rule.validate(submission_data):
                validation_errors.append(f"Validation failed: {rule.description}")
        
        if validation_errors:
            submission.errors = validation_errors
            submission.status = "validation_failed"
        else:
            submission.status = "validated"
            # Derive additional fields
            derived_fields = self._derive_fields(submission_data)
            submission.validated_data.update(derived_fields)
        
        self._save_submission(submission)
        return {
            "status": submission.status,
            "errors": submission.errors,
            "submission_id": submission.id,
            "timestamp": submission.timestamp
        }
    
    def _derive_fields(self, data: Dict) -> Dict:
        """Derive additional fields"""
        derived = {}
        
        # Sample field derivation
        if 'p_pop_zip_code' in data and len(str(data['p_pop_zip_code'])) == 5:
            derived['zip_plus_four'] = f"{data['p_pop_zip_code']}0000"
        
        # Funding agency code derivation
        if 'funding_agency_code' not in data:
            derived['funding_agency_code'] = None
        
        return derived
    
    def _save_submission(self, submission: Submission):
        """Save submission to database"""
        conn = sqlite3.connect(self.db_manager.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO submissions 
            (id, submission_type, status, timestamp, validated_data, errors)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            submission.id,
            submission.type.value,
            submission.status,
            submission.timestamp,
            json.dumps(submission.validated_data) if submission.validated_data else None,
            json.dumps(submission.errors) if submission.errors else None
        ))
        
        conn.commit()
        conn.close()

class FABSHelpPage:
    def __init__(self):
        self.page_content = {
            "help_center": {
                "title": "FABS Help Center",
                "sections": {
                    "submission_guide": "Step-by-step guide for submitting FABS data",
                    "error_codes": "Understanding common error codes in FABS",
                    "validation_rules": "List of FABS validation rules"
                }
            },
            "faq": [
                {"question": "How often are financial assistance data published?", 
                 "answer": "Daily at 6 PM ET"},
                {"question": "How do I fix a submission error?", 
                 "answer": "Review the error message and correct the data according to the validation rule."}
            ]
        }

class BrokerResourceManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        
    def update_resource(self, resource_id: int, title: str, content: str):
        """Update resource page content"""
        conn = sqlite3.connect(self.db_manager.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO resources 
            (id, title, content, last_updated)
            VALUES (?, ?, ?, ?)
        ''', (resource_id, title, content, datetime.now()))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Resource {resource_id} updated successfully")

class GTASWindowManager:
    def __init__(self):
        self.window_active = False
        self.start_time = None
        self.end_time = None
        self.lockdown_reasons = []
        
    def activate_window(self, start_time: datetime, end_time: datetime):
        """Activate GTAS submission window"""
        self.window_active = True
        self.start_time = start_time
        self.end_time = end_time
        logger.info(f"GTAS window activated from {start_time} to {end_time}")
        
    def deactivate_window(self):
        """Deactivate GTAS submission window"""
        self.window_active = False
        self.start_time = None
        self.end_time = None
        logger.info("GTAS window deactivated")

class DFileGeneratorManager:
    def __init__(self):
        self.cached_requests = {}
        self.cache_expiry_minutes = 10
        
    def request_generation(self, identifier: str, data_source: str) -> str:
        """Generate D File with caching"""
        cache_key = f"{identifier}_{data_source}"
        
        # Check if already cached and valid
        if cache_key in self.cached_requests:
            cached_result = self.cached_requests[cache_key]
            if datetime.now() - cached_result['timestamp'] < timedelta(minutes=self.cache_expiry_minutes):
                logger.info(f"Returning cached result for {cache_key}")
                return cached_result['result']
        
        # Generate new file
        generated_file = self._generate_d_file(identifier, data_source)
        
        # Cache the result
        self.cached_requests[cache_key] = {
            'result': generated_file,
            'timestamp': datetime.now()
        }
        
        logger.info(f"D File generated: {generated_file}")
        return generated_file
    
    def _generate_d_file(self, identifier: str, data_source: str) -> str:
        """Simulate D File generation"""
        return f"d_file_{identifier}_{data_source}_{datetime.now().isoformat()}.csv"

class FabsValidationManager:
    def __init__(self):
        self.field_validators = {}
        self._setup_field_validators()
        
    def _setup_field_validators(self):
        self.field_validators = {
            'ppop_zip_code': self._validate_ppop_zip_code,
            'funding_agency_code': self._validate_funding_agency_code,
            'legal_entity_address_line_3': self._validate_legal_entity_addr_line3
        }
    
    def validate_submission(self, input_data: Dict) -> tuple:
        """Validate submission data against rules"""
        errors = []
        warnings = []
        
        for field, validator in self.field_validators.items():
            if field in input_data:
                result = validator(input_data[field])
                if isinstance(result, list):
                    errors.extend(result)
                elif isinstance(result, dict):
                    errors.extend(result.get('errors', []))
                    warnings.extend(result.get('warnings', []))
        
        return errors, warnings
    
    def _validate_ppop_zip_code(self, zip_code: str) -> List[str]:
        """Validate PPoP ZIP code"""
        if zip_code is None or zip_code == "":
            return []
        if not str(zip_code).isdigit():
            return ["PPoP ZIP must be numeric"]
        if len(str(zip_code)) not in [5, 9]:  # Could be 5-digit or 9-digit (ZIP+4)
            return ["Invalid ZIP code length"]
        return []
    
    def _validate_funding_agency_code(self, code: Any) -> List[str]:
        """Validate funding agency code with special cases"""
        if code in ['', None, 0]:
            return []
        if not isinstance(code, (str, int)):
            return ["Funding agency code must be string or integer"]
        return []
    
    def _validate_legal_entity_addr_line3(self, value: str) -> Dict[str, list]:
        """Validate legal entity address line 3"""
        if value is None or value == "":
            return {'warnings': ['Missing Legal Entity Address Line 3']}
        return {}

class SubmissionDashboard:
    def __init__(self):
        self.submissions = []
        
    def add_submission(self, submission: Submission):
        self.submissions.append(submission)
        
    def get_dashboard_data(self, limit: int = 10) -> List[Dict]:
        """Get recent submissions for dashboard display"""
        recent = sorted(self.submissions, key=lambda x: x.timestamp, reverse=True)[:limit]
        return [
            {
                'id': s.id,
                'type': s.type.value,
                'status': s.status,
                'timestamp': s.timestamp.isoformat(),
                'errors': len(s.errors) if s.errors else 0
            } for s in recent
        ]

def main():
    """Example usage demonstration"""
    # Initialize components
    db_manager = DatabaseManager()
    fabs_processor = FABSDataProcessor(db_manager)
    resource_manager = BrokerResourceManager(db_manager)
    gtas_manager = GTASWindowManager()
    manager = DFileGeneratorManager()
    validation_manager = FabsValidationManager()
    dashboard = SubmissionDashboard()

    print("=== Broker System Demo ===")
    
    # Simulate processing a submission
    sample_data = {
        "submission_id": "SUB-12345",
        "p_pop_zip_code": "12345",
        "funding_agency_code": "",
        "legal_entity_address_line_3": "",
        "record_type": "Loan"
    }
    
    # Process FABS submission
    result = fabs_processor.process_submission(sample_data)
    print("Submission Result:", result)
    
    # Validate using FABS validators
    errors, warnings = validation_manager.validate_submission(sample_data)
    print(f"Validation Results - Errors: {errors}, Warnings: {warnings}")
    
    # Test dashboard
    submission_obj = Submission("SUB-12345", SubmissionType.FABS, "processed", datetime.now())
    dashboard.add_submission(submission_obj)
    print("Dashboard Data:", dashboard.get_dashboard_data())
    
    # Test GTAS window management
    gtas_manager.activate_window(
        datetime.now(), 
        datetime.now() + timedelta(hours=2)
    )
    
    # Test D File generation
    dfile_result = manager.request_generation("TEST_001", "fpds_source")
    print("Generated D File:", dfile_result)
    
    # Update resource page
    resource_manager.update_resource(1, "Resources Page", "<h1>FABS Resources</h1><p>Updated content...</p>")
    print("Resource page updated")

if __name__ == "__main__":
    main()