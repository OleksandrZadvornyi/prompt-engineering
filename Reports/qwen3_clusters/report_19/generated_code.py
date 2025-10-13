import datetime
import re
from typing import Dict, List, Optional
from collections import defaultdict

class ValidationRule:
    def __init__(self, rule_id: str, description: str, category: str = "general"):
        self.rule_id = rule_id
        self.description = description
        self.category = category
        self.last_updated = datetime.datetime.now()
        
class ValidationError:
    def __init__(self, code: str, message: str, field: str = None):
        self.code = code
        self.message = message
        self.field = field
        
class SubmissionStatus:
    SUBMITTED = "submitted"
    VALIDATED = "validated"
    PUBLISHED = "published"
    ERROR = "error"

class BrokerDatabase:
    def __init__(self):
        self.validation_rules = {}
        self.submissions = {}
        self.agency_data = {}
        self.duns_validations = {}
        self.flex_fields = {}

class FABSSubmissionManager:
    def __init__(self, db: BrokerDatabase):
        self.db = db
        self.submission_cache = {}
        self.gtas_window_data = None

    def validate_submission(self, submission_id: str) -> List[ValidationError]:
        """Validate FABS submission"""
        submission = self.db.submissions.get(submission_id)
        errors = []
        
        # Check file extension
        if submission and submission.get('file_extension') not in ['.csv', '.xlsx']:
            errors.append(ValidationError("FILE_EXT_INVALID", "Invalid file type"))
            
        # Validate flex fields
        if submission and 'flex_fields' in submission:
            for field_name, value in submission['flex_fields'].items():
                if not value and field_name in submission.get('required_fields', []):
                    errors.append(ValidationError("REQUIRED_FIELD_MISSING", f"Required field {field_name} is missing"))
                    
        return errors

class FABSValidationManager:
    def __init__(self, db: BrokerDatabase):
        self.db = db
        self.error_codes = {
            "INVALID_ZIP": "ZIP code format is invalid",
            "MISSING_REQUIRED_FIELD": "Required field is missing",
            "UNAUTHORIZED_ACCESS": "Access denied - not authorized",
            "OVERSIZE_ERROR": "Field exceeds maximum length",
            "CONTRACT_TYPE_MISMATCH": "This record contains contract type data but appears as a grant"
        }

    def validate_zip_code(self, zip_code: str) -> bool:
        """Validate Zip Code format including 5-digit and 9-digit formats"""
        pattern = r'^\d{5}$|^\d{5}-\d{4}$'
        return bool(re.match(pattern, zip_code))

    def validate_funding_agency_code(self, code: str) -> bool:
        """Validate Funding Agency Code according to rules"""
        if not code:
            return False
        # Ensure it's a valid agency code format
        return len(code.strip()) <= 4

    def validate_cfdas(self, cfdas: List[str]) -> bool:
        """Validate CFDA numbers"""
        for cfdas_entry in cfdas:
            if not cfdas_entry.isdigit() or len(cfdas_entry) != 6:
                return False
        return True

    def handle_cfda_error(self, cfda_code: str, error_context: str) -> str:
        """Provide specific CFDA error explanation"""
        return f"CFDA Code {cfda_code} failed validation due to: {error_context}"

class SubmissionDashboard:
    def __init__(self, db: BrokerDatabase):
        self.db = db

    def get_submissions_by_status(self, status: str) -> List[Dict]:
        """Return submissions matching the given status"""
        result = []
        for sub_id, sub_info in self.db.submissions.items():
            if sub_info.get('status') == status:
                result.append({
                    'submission_id': sub_id,
                    'agency': sub_info.get('agency'),
                    'last_modified': sub_info.get('last_modified'),
                    'status': sub_info.get('status')
                })
        return result

    def update_submission_status(self, submission_id: str, status: str):
        """Update submission status"""
        if submission_id in self.db.submissions:
            self.db.submissions[submission_id]['status'] = status
            
    def get_submission_details(self, submission_id: str) -> Optional[Dict]:
        """Get detailed submission info"""
        return self.db.submissions.get(submission_id)

class GTASWindowLoader:
    def __init__(self, db: BrokerDatabase):
        self.db = db

    def load_gtas_window_data(self):
        """Load GTAS window data into database"""
        self.db.gtas_window_data = {
            'start_date': '2023-01-01',
            'end_date': '2023-01-15',
            'is_locked': True
        }
        print("GTAS Window data loaded successfully")

class HistoricalFABSLoader:
    def __init__(self, db: BrokerDatabase):
        self.db = db

    def derive_frec_codes(self, frec_value: str) -> str:
        """Derive FREC codes based on input"""
        if not frec_value:
            return ""
        # Simplified logic for illustration
        return frec_value.upper()

    def process_historical_data(self, data_rows: List[Dict]):
        """Process historical FABS data including FREC derivation"""
        for row in data_rows:
            row['derived_frec'] = self.derive_frec_codes(row.get('frec'))
            # Add more processing here as needed
        return data_rows

class SampleFileLinker:
    def __init__(self, db: BrokerDatabase):
        self.db = db
    
    def get_sample_file_path(self, submission_type: str) -> str:
        """Get actual sample file path based on submission type"""
        sample_files = {
            'FABS': '/static/Sample_FABS_File.csv',
            'DABS': '/static/Sample_DABS_File.csv'
        }
        return sample_files.get(submission_type, '/static/Sample_File.csv')

class FABSCustomErrors:
    def __init__(self, db: BrokerDatabase):
        self.db = db
        
    def get_error_message_for_rule(self, error_code: str) -> str:
        """Map error codes to custom error messages"""
        error_mapping = {
            'DUPLICATE_PUBLISH': 'Submission already published. Please refresh before attempting to publish again.',
            'MISSING_AWARD_TYPE': 'Award type is required for this agency. Please review your submission.',
            'INVALID_ZIP_FORMAT': 'The ZIP code provided does not match expected format.'
        }
        return error_mapping.get(error_code, 'An error occurred during submission validation.')

class FileHandler:
    def __init__(self, db: BrokerDatabase):
        self.db = db

    def process_fabs_file(self, uploaded_content: str) -> List[Dict]:
        """Simulate parsing FABS file content into structured data"""
        lines = uploaded_content.strip().split('\n')
        parsed_lines = []
        # Skip header line
        for line_num, line in enumerate(lines[1:], 1):
            if not line.strip():
                continue
            try:
                values = line.split(',')
                if len(values) >= 5:
                    parsed_line = {
                        'line_number': line_num,
                        'record_type': values[0],
                        'award_type': values[1],
                        'zip': values[2],
                        'agency_code': values[3],
                        'cfda': values[4]
                    }
                    parsed_lines.append(parsed_line)
            except Exception as e:
                print(f"Error parsing line {line_num}: {e}")
        return parsed_lines

class AgencyData:
    def __init__(self, db: BrokerDatabase):
        self.db = db

    def update_agency_header_info(self, agency_id: str, timestamp: str):
        """Updated timestamp display in agency header"""
        if agency_id not in self.db.agency_data:
            self.db.agency_data[agency_id] = {}
        self.db.agency_data[agency_id]['updated_at'] = timestamp
        self.db.agency_data[agency_id]['date_time'] = timestamp

class ZIPValidator:
    def __init__(self):
        self.zip_patterns = {
            'standard_five': r'^\d{5}$',
            'extended_nine': r'^\d{5}-\d{4}$',
            'citywide': r'^\d{5}$'  # For city-wide inputs
        }
    
    def validate_zip_format(self, zip_input: str) -> bool:
        """Validate various ZIP formats accepted"""
        patterns = [self.zip_patterns[p] for p in ['standard_five', 'extended_nine', 'citywide']]
        return any(re.match(p, zip_input) for p in patterns)
        
    def normalize_zip(self, zip_input: str) -> str:
        """Normalize ZIP input to standard format"""
        # Remove spaces and hyphens
        clean_zip = re.sub(r'[^\d]', '', zip_input)
        if len(clean_zip) == 9:
            return f"{clean_zip[:5]}-{clean_zip[5:]}"
        elif len(clean_zip) == 5:
            return clean_zip
        else:
            raise ValueError("Invalid ZIP code format")

# Main executor class
class BrokerSystem:
    def __init__(self):
        self.db = BrokerDatabase()
        self.fabs_manager = FABSSubmissionManager(self.db)
        self.fabs_validator = FABSValidationManager(self.db)
        self.submit_dashboard = SubmissionDashboard(self.db)
        self.gtas_loader = GTASWindowLoader(self.db)
        self.historical_loader = HistoricalFABSLoader(self.db)
        self.sample_linker = SampleFileLinker(self.db)
        self.custom_errors = FABSCustomErrors(self.db)
        self.file_handler = FileHandler(self.db)
        self.agency_info = AgencyData(self.db)
        self.zip_validator = ZIPValidator()
        self.setup_initial_configs()

    def setup_initial_configs(self):
        """Initialize core configuration"""
        # Set up validation rules in database
        self.db.validation_rules["DB-2213"] = ValidationRule(
            "DB-2213", 
            "Update to validation rules based on latest requirements", 
            "data_quality"
        )
        self.db.validation_rules["ZIP_FORMAT_RULE"] = ValidationRule(
            "ZIP_FORMAT_RULE", 
            "Validates ZIP code formats", 
            "form_validation"
        )
        # Add initial dummy submission to database
        self.db.submissions["SUBMIT_001"] = {
            "agency": "TEST_AGENCY",
            "status": SubmissionStatus.SUBMITTED,
            "last_modified": datetime.datetime.now(),
            "file_extension": ".csv",
            "flex_fields": {"federal_action_obligation": "1000.00", "other_field": ""},
            "required_fields": ["federal_action_obligation"]
        }
        
    def run_integration_test(self):
        """Run a basic integration test using all clusters"""
        print("Starting Integration Tests...")
        
        # Test Cluster 0 - Broker data and validations
        errors = self.fabs_manager.validate_submission("SUBMIT_001")
        print(f"Validation errors: {len(errors)}")

        # Test Cluster 1 - D File generation and submission handling
        sample_submission = self.submit_dashboard.get_submissions_by_status(SubmissionStatus.SUBMITTED)
        print(f"Submitted submissions: {len(sample_submission)}")
        
        # Test Cluster 2 - FABS historical loading and updates
        raw_data = [{"frec": "ABC123", "award_id": "A123"}, {"frec": "", "award_id": "B456"}]
        processed = self.historical_loader.process_historical_data(raw_data)
        print(f"Processed historical data items: {len(processed)}")
        
        # Test Cluster 3 - File handling and validations
        test_content = "# Record Type,Award Type,ZIP,Agency,CFDA\n1,Grant,12345,AGENCY_1,012345\n2,Contract,67890,AGENCY_2,067890"
        parsed = self.file_handler.process_fabs_file(test_content)
        print(f"Parsed file lines: {len(parsed)}")
        
        # Test Cluster 4 - D1 sync with FPDS
        self.db.submissions["SUBMIT_002"] = {
            "agency": "FPDS_SYNC_TEST",
            "status": SubmissionStatus.VALIDATED,
            "last_modified": datetime.datetime.now(),
            "file_extension": ".xlsx"
        }
        sync_errors = self.fabs_manager.validate_submission("SUBMIT_002")
        print(f"D1/FPSD sync validation errors: {len(sync_errors)}")
        
        # Test Cluster 5 - User interface updates and navigation
        sample_path = self.sample_linker.get_sample_file_path("FABS")
        print(f"Sample file path for FABS: {sample_path}")
        
        # Test ZIP validation logic (Clusters 2, 4, 5)
        test_zips = ["12345", "12345-6789", "123", "not_a_zip"]
        for test_zip in test_zips:
            try:
                valid = self.zip_validator.validate_zip_format(test_zip)
                normalized = self.zip_validator.normalize_zip(test_zip)
                print(f"'{test_zip}' is valid: {valid}, normalized: {normalized}")
            except ValueError as e:
                print(f"Error normalizing ZIP '{test_zip}': {str(e)}")

if __name__ == "__main__":
    broker_system = BrokerSystem()
    broker_system.run_integration_test()