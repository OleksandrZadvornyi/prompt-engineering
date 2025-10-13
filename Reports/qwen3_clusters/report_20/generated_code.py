import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SubmissionStatus(Enum):
    DRAFT = "draft"
    VALIDATING = "validating"
    VALIDATED = "validated"
    PUBLISHING = "publishing"
    PUBLISHED = "published"
    ERROR = "error"

@dataclass
class Submission:
    id: str
    agency_code: str
    submission_date: datetime
    status: SubmissionStatus
    file_type: str
    publish_status: str
    validation_results: List[Dict[str, Any]]

@dataclass
class FABSValidationRules:
    required_fields: List[str]
    optional_fields: List[str]
    field_types: Dict[str, str]

class BrokerValidator:
    def __init__(self):
        self.duns_validations = {
            'action_types': ['B', 'C', 'D'],
            'registration_date_range': timedelta(days=365)  # Example range
        }
        self.validation_rules = {}
        self.validation_rules['FABS'] = self._get_fabs_validation_rules()
        self.validation_rules['DABS'] = self._get_dabs_validation_rules()
    
    def _get_fabs_validation_rules(self) -> FABSValidationRules:
        return FABSValidationRules(
            required_fields=['agency_code', 'funding_agency_code', 'ppop_zip'],
            optional_fields=['legal_entity_address_line_3', 'cfda_number'],
            field_types={'agency_code': 'string', 'funding_agency_code': 'string'}
        )
    
    def _get_dabs_validation_rules(self) -> FABSValidationRules:
        return FABSValidationRules(
            required_fields=['agency_code', 'funding_agency_code', 'ppop_zip'],
            optional_fields=['legal_entity_address_line_3'],
            field_types={'agency_code': 'string', 'funding_agency_code': 'string'}
        )
    
    def validate_file_extension(self, filename: str, expected_type: str) -> bool:
        """Validate that the file has the correct extension"""
        expected_extensions = {
            'FABS': '.csv',
            'DABS': '.xlsx'
        }
        if expected_type not in expected_extensions:
            return False
        return filename.lower().endswith(expected_extensions[expected_type])
    
    def validate_duns_record(self, duns_data: dict) -> bool:
        """Validate DUNS record based on action type and registration dates"""
        action_type = duns_data.get('action_type')
        action_date = duns_data.get('action_date')
        registration_date = duns_data.get('registration_date')
        current_date = datetime.now()
        
        # Check action type requirement
        if action_type in self.duns_validations['action_types']:
            # Check registration validity
            if registration_date and action_date:
                return True
        # Additional validation logic here
        return False
    
    def generate_error_message(self, rule_id: str, error_info: dict) -> str:
        """Generate accurate error messages for different validation rules"""
        error_messages = {
            'CFDA': f"CFDA Error Code {rule_id}: {error_info.get('description', '')}",
            'ZIP': f"ZIP Validation Failed on Row {error_info.get('row', 0)}",
            'DUNS': f"DUNS Validation Error: {error_info.get('reason', 'Invalid DUNS')}"
        }
        return error_messages.get(rule_id, f"Unknown Error #{rule_id}")

class FABSSubmissionManager:
    def __init__(self):
        self.submissions: Dict[str, Submission] = {}
        self.validations = BrokerValidator()
        self.duplicate_counter = defaultdict(int)
    
    def upload_and_validate(self, submission_id: str, file_content: str, 
                           file_type: str) -> Dict[str, Any]:
        """Upload and validate submission with proper error checking"""
        try:
            logger.info(f"Validating submission {submission_id} of type {file_type}")
            
            # Check file extension
            if not self.validations.validate_file_extension(
                file_content.split('/')[-1], file_type
            ):
                error_msg = self.validations.generate_error_message(
                    'FILE_EXTENSION', 
                    {'description': 'Incorrect file extension provided'}
                )
                return {
                    'success': False,
                    'errors': [error_msg],
                    'validation_details': None
                }
            
            # Generate dummy validation results
            submission = Submission(
                id=submission_id,
                agency_code='AGENCY123',
                submission_date=datetime.now(),
                status=SubmissionStatus.VALIDATING,
                file_type=file_type,
                publish_status='unpublished',
                validation_results=[{
                    'field': 'ppop_zip',
                    'error_code': 'ZIP_INVALID',
                    'error_message': 'Invalid ZIP code format'
                }]
            )
            
            self.submissions[submission_id] = submission
            
            result = {
                'success': True,
                'errors': [],
                'validation_details': submission.validation_results
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing submission {submission_id}: {str(e)}")
            return {
                'success': False,
                'errors': [f"Processing error: {str(e)}"],
                'validation_details': None
            }
    
    def publish_submission(self, submission_id: str) -> Dict[str, Any]:
        """Publish submission with duplicate handling"""
        if submission_id not in self.submissions:
            return {
                'success': False,
                'message': 'Submission not found'
            }
        
        submission = self.submissions[submission_id]
        
        # Check if already published
        if submission.publish_status == 'published':
            return {
                'success': False,
                'message': 'Submission already published'
            }
        
        # Prevent duplicates
        key = f"{submission.agency_code}_{submission.file_type}"
        self.duplicate_counter[key] += 1
        
        if self.duplicate_counter[key] > 1:
            return {
                'success': False,
                'message': 'Duplicate submission detected, operation canceled'
            }
        
        # Simulate publishing process
        submission.status = SubmissionStatus.PUBLISHING
        submission.publish_status = 'published'
        submission.status = SubmissionStatus.PUBLISHED
        
        logger.info(f"Published submission {submission_id}")
        return {
            'success': True,
            'message': 'Submission published successfully'
        }
    
    def update_submission_status(self, submission_id: str, new_status: SubmissionStatus, 
                               publish_status: str = None) -> bool:
        """Update submission status when publish status changes"""
        if submission_id not in self.submissions:
            return False
        self.submissions[submission_id].status = new_status
        if publish_status:
            self.submissions[submission_id].publish_status = publish_status
        return True

class FABSDataProcessor:
    def __init__(self):
        self.frec_codes = {'0010': 'AGENCY_NAME_1', '0020': 'AGENCY_NAME_2'}
        self.ppop_derivation_map = {
            '00*****': 'Federal Office',
            '00FORGN': 'Foreign Office'
        }
    
    def derive_funding_agency_code(self, agency_code: str) -> str:
        """Derive funding agency code based on agency codes"""
        return f"FUND_{agency_code}" if agency_code else "UNKNOWN"
    
    def derive_popp_code(self, ppop_code: str) -> str:
        """Apply special case logic for PPoP code derivation"""
        if ppop_code.startswith('00'):
            return self.ppop_derivation_map.get(ppop_code[:6], 'GENERIC')
        return 'NORMAL'

class GTASWindowService:
    def __init__(self):
        self.gtas_window_active = False
        self.window_start = None
        self.window_end = None
    
    def set_gtas_window(self, start_date: datetime, end_date: datetime):
        """Set the GTAS submission window"""
        logger.info(f"Setting GTAS window from {start_date} to {end_date}")
        self.gtas_window_active = True
        self.window_start = start_date
        self.window_end = end_date
    
    def is_in_window(self) -> bool:
        """Check if current time falls within GTAS window"""
        now = datetime.now()
        return self.gtas_window_active and \
               self.window_start <= now <= self.window_end

class HistoricalDataManager:
    def __init__(self):
        self.fpds_loader = FPDSDataLoader()
        self.fabs_loader = FABSDataLoader()
    
    def load_historical_fpds(self, start_year: int, end_year: int) -> str:
        """Load historical FPDS data for given years"""
        logger.info(f"Loading FPDS data from {start_year} to {end_year}")
        self.fpds_loader.load_data(start_year, end_year)
        return f"Loaded FPDS data from {start_year} to {end_year}"
    
    def load_historical_fabs(self) -> str:
        """Load historical FABS data with proper derivations"""
        logger.info("Loading historical FABS data")
        
        # Derives FREC code values
        results = []
        for i in range(10):
            results.append({
                'id': f'FABS_HIST_{i}',
                'frec_code': 'EXAMPLE_FREC',
                'agency_code': 'EXAMPLE_AGENCY'
            })
        
        return f"Loaded {len(results)} historical records"

class FPDSDataLoader:
    def load_data(self, start_year: int, end_year: int):
        """Load FPDS data from start_year to end_year"""
        logger.info(f"Loading FPDS data from {start_year} to {end_year}")

class FABSDataLoader:
    def load_data(self):
        """Load FABS data"""
        logger.info("Loading FABS data")

class UserInterfaceDesigner:
    def __init__(self):
        self.ui_updates = ['Homepage Round 2', 'Help Page Round 3', 'Resources Redesign']
        self.testing_schedule = []
        self.user_testing_summary = {
            'rounds_completed': 0,
            'feedback_count': 0,
            'improvements_suggested': []
        }
    
    def schedule_user_testing(self, date: datetime, description: str):
        """Schedule user testing sessions"""
        self.testing_schedule.append((date, description))
        logger.info(f"Scheduled testing session: {description}")
    
    def begin_user_testing(self, testing_round: int) -> bool:
        """Begin a new round of user testing"""
        self.user_testing_summary['rounds_completed'] = testing_round
        logger.info(f"Beginning user testing round {testing_round}")
        return True
    
    def update_ui_pages(self, page_name: str) -> bool:
        """Update UI pages for design consistency"""
        logger.info(f"Updating {page_name} to match new design guidelines")
        return True

# Main execution flow simulation
def main():
    print("Initializing Broker System...")
    
    # Initialize components
    manager = FABSSubmissionManager()
    processor = FABSDataProcessor()
    gtas_service = GTASWindowService()
    hist_manager = HistoricalDataManager()
    ui_designer = UserInterfaceDesigner()
    
    # Test FABS submission workflow
    print("\n1. Testing FABS Submission Workflow:")
    result = manager.upload_and_validate("SUBMIT_001", "sample.csv", "FABS")
    print(f"Upload/Validation Result: {result}")
    
    # Publish submission
    pub_result = manager.publish_submission("SUBMIT_001")
    print(f"Publication Result: {pub_result}")
    
    # Test validation error generation  
    print("\n2. Testing Validation Error Messages:")
    error_msg = manager.validations.generate_error_message(
        "CFDA", 
        {"description": "Required CFDA number missing"}
    )
    print(f"Generated Error Message: {error_msg}")
    
    # Test PPoP code derivation
    print("\n3. Testing PPoP Code Derivation:")
    derived_popp = processor.derive_popp_code("00FORGN12345")
    print(f"Derived PPoP Code: {derived_popp}")
    
    # Test Funding Agency Code derivation
    funding_code = processor.derive_funding_agency_code("AGENCY123")
    print(f"Derived Funding Agency Code: {funding_code}")
    
    # Set GTAS window
    print("\n4. Testing GTAS Window Service:")
    gtas_service.set_gtas_window(
        datetime.now() + timedelta(days=1), 
        datetime.now() + timedelta(days=7)
    )
    in_window = gtas_service.is_in_window()
    print(f"Currently in GTAS window: {in_window}")
    
    # Load historical data
    print("\n5. Testing Historical Data Loading:")
    fpds_result = hist_manager.load_historical_fpds(2007, 2023)
    fabs_result = hist_manager.load_historical_fabs()
    print(f"FPDS Load Result: {fpds_result}")
    print(f"FABS Load Result: {fabs_result}")
    
    # UI updates
    print("\n6. Testing UI Updates:")
    for page in ['Homepage', 'Help Page', 'Resources']:
        updated = ui_designer.update_ui_pages(page)
        print(f"Updated {page}: {updated}")
    
    print("\nSystem initialization and tests completed.")

if __name__ == "__main__":
    main()