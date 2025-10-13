from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
import re

class FileType(Enum):
    FABS = "FABS"
    DABS = "DABS"

class SubmissionStatus(Enum):
    DRAFT = "draft"
    VALIDATED = "validated"
    PUBLISHED = "published"
    ERROR = "error"

@dataclass
class Submission:
    id: str
    type: FileType
    status: SubmissionStatus
    created_by: str
    created_at: datetime
    updated_at: datetime
    publish_status: str
    file_data: Dict[str, Any]

@dataclass
class ValidationRule:
    rule_id: str
    description: str
    category: str
    severity: str
    updated_at: datetime

@dataclass
class ErrorRecord:
    error_code: str
    error_description: str
    field_name: str
    row_number: int
    submission_id: str

@dataclass
class FundingAgency:
    code: str
    name: str

@dataclass
class PPoPData:
    zip_plus_4: str
    congressional_district: str
    office_code: str
    office_name: str

class ValidationService:
    def __init__(self):
        self.validation_rules = {}
        self.error_codes = {
            'CFDA_ERROR': 'CFDA field value does not match expected format',
            'DUNS_ERROR': 'DUNS invalid or expired',
            'ZIP_ERROR': 'Invalid ZIP code format',
            'PPoP_ERROR': 'PPoP validation failed'
        }
    
    def add_rule(self, rule: ValidationRule):
        self.validation_rules[rule.rule_id] = rule
    
    def validate_submission(self, submission: Submission) -> List[ErrorRecord]:
        errors = []
        
        # Validate file extensions
        if submission.type == FileType.FABS and not submission.file_data.get('file_path', '').endswith('.csv'):
            errors.append(ErrorRecord(
                error_code='FILE_EXTENSION_ERROR',
                error_description='Only CSV files accepted for FABS submission',
                field_name='file_path',
                row_number=0,
                submission_id=submission.id
            ))
        
        # Validate DUNS
        duns_validated = self._validate_duns(submission)
        if not duns_validated:
            errors.append(ErrorRecord(
                error_code='DUNS_ERROR',
                error_description='DUNS invalid or expired',
                field_name='duns',
                row_number=1,
                submission_id=submission.id
            ))

        # Validate ZIP format
        zip_validated = self._validate_zip_format(submission)
        if not zip_validated:
            errors.append(ErrorRecord(
                error_code='ZIP_ERROR',
                error_description='ZIP code format invalid',
                field_name='popp_zip',
                row_number=1,
                submission_id=submission.id
            ))
            
        # Validate PPoP Congressional District
        district_validated = self._validate_popp_district_format(submission)
        if not district_validated:
            errors.append(ErrorRecord(
                error_code='PPoP_ERROR',
                error_description='PPoP Congressional District invalid',
                field_name='popp_congressional_district',
                row_number=1,
                submission_id=submission.id
            ))
            
        return errors
        
    def _validate_duns(self, submission: Submission) -> bool:
        # Simplified validation - actual implementation would check SAM registry
        duns = submission.file_data.get('duns', '')
        return duns and re.match(r'^\d{9}$', duns) is not None
    
    def _validate_zip_format(self, submission: Submission) -> bool:
        # Check ZIP code valid format (allowing short form without +4)
        zip_code = submission.file_data.get('popp_zip', '')
        if not zip_code:
            return False
        # Support 5-digit or 9-digit ZIP codes
        return bool(re.match(r'^\d{5}(-\d{4})?$', zip_code))
        
    def _validate_popp_district_format(self, submission: Submission) -> bool:
        district = submission.file_data.get('popp_congressional_district', '')
        if not district:
            return True  # Allow empty or optional field
        return bool(re.match(r'^\d{2}-\d{2}$', district)) or district == '99'

class SubmissionService:
    def __init__(self):
        self.submissions: Dict[str, Submission] = {}
        self.validation_service = ValidationService()
        self.last_published_timestamp = None
        
    def upload_submission(self, submission_data: Dict[str, Any], user_id: str) -> Submission:
        submission_id = f"sub_{len(self.submissions)+1}"
        submission = Submission(
            id=submission_id,
            type=FileType(submission_data['file_type']),
            status=SubmissionStatus.DRAFT,
            created_by=user_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            publish_status="NOT_PUBLISHED",
            file_data=submission_data
        )
        self.submissions[submission_id] = submission
        return submission
    
    def validate_submission(self, submission_id: str) -> List[ErrorRecord]:
        submission = self.submissions[submission_id]
        errors = self.validation_service.validate_submission(submission)
        if not errors:
            submission.status = SubmissionStatus.VALIDATED
        else:
            submission.status = SubmissionStatus.ERROR
        return errors
    
    def publish_submission(self, submission_id: str) -> bool:
        submission = self.submissions[submission_id]
        if submission.status != SubmissionStatus.VALIDATED:
            return False
            
        # Prevent duplicate publishing
        if submission.publish_status == "PUBLISHED":
            return False
            
        # Update submission status
        submission.status = SubmissionStatus.PUBLISHED
        submission.publish_status = "PUBLISHED"
        submission.updated_at = datetime.now()
        self.last_published_timestamp = datetime.now()
        return True

class FABSFileProcessor:
    def __init__(self):
        self.historical_loader = HistoricalDataLoader()
        
    def process_fabs_submission(self, submission: Submission) -> bool:
        # Derive FundingAgencyCode
        funding_agency_code = self.derive_funding_agency_code(submission)
        
        # Handle special cases for PPoPCode 
        ppop_code = self.process_ppopcode(submission)
        
        # Generate D1 file in sync with FPDS
        self.sync_d1_generation_with_fpds(submission)
        
        # Load historical data from previous submissions if needed
        self.historical_loader.load_historical_data(submission)
        
        return True
        
    def derive_funding_agency_code(self, submission: Submission) -> str:
        # Logic for deriving FundingAgencyCode based on agency codes and data sources
        frec = submission.file_data.get('frec')
        if frec and len(frec) >= 5:
            return frec[:5]
        return submission.file_data.get('agency_code', '00000')
        
    def process_ppopcode(self, submission: Submission) -> str:
        ppop_code = submission.file_data.get('popp_code', '')
        # Handle 00***** and 00FORGN cases
        if ppop_code.startswith('00') and (ppop_code.endswith('*****') or ppop_code.endswith('FORGN')):
            return ppop_code
        return ppop_code
        
    def sync_d1_generation_with_fpds(self, submission: Submission):
        # Simulate synchronization with FPDS data load
        last_load_time = submission.file_data.get('last_fpds_load', datetime.now() - timedelta(days=1))
        if datetime.now() - last_load_time < timedelta(minutes=5):
            # No updates, reuse existing D1 file
            pass
        else:
            # Regenerate D1 file
            self.regenerate_d1_file(submission)

    def regenerate_d1_file(self, submission: Submission):
        # Simplified regeneration logic
        submission.file_data['d1_file_generated'] = datetime.now()

class HistoricalDataLoader:
    def __init__(self):
        self.frec_derivations = {}
        
    def load_historical_data(self, submission: Submission):
        # Logic for loading historical data with FREC derivations
        if submission.file_data.get('is_historical') or submission.type == FileType.FABS:
            # Apply FREC derivation logic 
            frec_code = submission.file_data.get('agency_code', '')[:4]
            submission.file_data['derived_frec'] = self.extract_frec_from_agency(frec_code)
        
    def extract_frec_from_agency(self, agency_code: str) -> str:
        return f"{agency_code}00" if len(agency_code) < 4 else agency_code
        
    def load_fpds_and_historical_data(self) -> List[Tuple[str, datetime]]:
        # Return list of filename and timestamp pairs for historical FPDS data
        return [
            ('fpds_2007.csv', datetime(2007, 1, 1)),
            ('fpds_2008.csv', datetime(2008, 1, 1)),
            ('fpds_2009.csv', datetime(2009, 1, 1))
        ]

class PPoPValidator:
    def __init__(self):
        self.zip_validation_cache = {}
        
    def validate_popp_zip_plus_four(self, zip_code: str, use_cache=True) -> bool:
        if use_cache:
            cached_result = self.zip_validation_cache.get(zip_code)
            if cached_result is not None:
                return cached_result
                
        # Validate standard 5+4 ZIP formats
        valid_format = bool(re.match(r'^\d{5}(-\d{4})?$', zip_code))
        
        if use_cache:
            self.zip_validation_cache[zip_code] = valid_format
            
        return valid_format
    
    def convert_to_5_or_9_digit_zip(self, zip_input: str) -> str:
        # Convert to either 5-digit or 9-digit format for consistency
        cleaned = ''.join(char for char in zip_input if char.isdigit())
        if len(cleaned) <= 5:
            return cleaned.zfill(5)
        elif len(cleaned) <= 9:
            return f"{cleaned[:5]}-{cleaned[5:]}"
        return cleaned

class D1FileGenerator:
    def __init__(self):
        self.cache = {}  # Cache for generated files to avoid duplicates
        
    def generate_file(self, submission_id: str, source_data: Dict[str, Any]) -> str:
        # Create unique cache key based on submission content and timestamp
        cache_key = f"{submission_id}_{datetime.now().timestamp()}"
        if cache_key in self.cache:
            return self.cache[cache_key]
            
        # Generate D1 file using standard data model
        generated_file_name = f"D1_{submission_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        # In real implementation, this would write to disk and return filename
        
        self.cache[cache_key] = generated_file_name
        return generated_file_name

def main():
    # Setup services
    submission_service = SubmissionService()
    fabs_processor = FABSFileProcessor()
    pop_validator = PPoPValidator()
    d1_generator = D1FileGenerator()
    
    # Example user flow
    print("=== Processing FABS Submission ===")
    
    # Upload a new submission  
    submission_data = {
        'file_type': 'FABS',
        'file_path': 'sample_data.csv',
        'agency_code': '1234',
        'frec': '123456',
        'duns': '123456789',
        'popp_zip': '12345',
        'popp_congressional_district': '01-02',
        'is_historical': False
    }
    
    submission = submission_service.upload_submission(submission_data, 'user_456')
    print(f"Uploaded submission ID: {submission.id}")
    
    # Validate submission
    print("\n--- Validating submission ---")
    errors = submission_service.validate_submission(submission.id)
    if errors:
        print("Validation errors:")
        for err in errors:
            print(f"- {err}")

    # Process FABS specific transformations
    print("\n--- Processing FABS-specific logic ---")
    fabs_processor.process_fabs_submission(submission)
    print("FABS processing complete")

    # Validate PPoP ZIP+4 formats
    print("\n--- Validating PPoP ZIP formats ---")
    test_zips = ['12345', '12345-6789', '1234567890']
    for zip_code in test_zips:
        print(f"ZIP {zip_code}: {'Valid' if pop_validator.validate_popp_zip_plus_four(zip_code) else 'Invalid'}")

    # Generate D1 file
    print("\n--- Generating D1 file ---")
    d1_file = d1_generator.generate_file(submission.id, submission.file_data)
    print(f"Generated D1 file: {d1_file}")

    # Publish submission
    print("\n--- Publishing submission ---")
    if submission_service.publish_submission(submission.id):
        print("Successfully published!")
    else:
        print("Could not publish due to validation issue")

if __name__ == "__main__":
    main()