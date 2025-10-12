import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Mock classes for simulation purposes
class Submission:
    def __init__(self, submission_id: str, status: str, submission_date: datetime, created_by: str):
        self.submission_id = submission_id
        self.status = status
        self.submission_date = submission_date
        self.created_by = created_by
        self.errors = []

class User:
    def __init__(self, user_id: str, name: str, role: str):
        self.user_id = user_id
        self.name = name
        self.role = role

class FABSFileData:
    def __init__(self):
        self.file_content = ""
        self.upload_date = datetime.utcnow()
        self.filename = ""

class FlexField:
    def __init__(self, field_name: str, value: str):
        self.field_name = field_name
        self.value = value

class BrokerDB:
    def __init__(self):
        self.submissions: Dict[str, Submission] = {}
        self.users: Dict[str, User] = {}
        self.fabs_files: List[FABSFileData] = []
        self.flex_fields: Dict[str, List[FlexField]] = {}

    def create_submission(self, submission_id: str, status: str, created_by: str) -> Submission:
        sub = Submission(submission_id, status, datetime.utcnow(), created_by)
        self.submissions[submission_id] = sub
        return sub

    def update_submission_status(self, submission_id: str, new_status: str):
        if submission_id in self.submissions:
            self.submissions[submission_id].status = new_status

    def get_submission_by_id(self, submission_id: str) -> Submission:
        return self.submissions.get(submission_id)

    def create_user(self, user_id: str, name: str, role: str) -> User:
        user = User(user_id, name, role)
        self.users[user_id] = user
        return user

    def get_user_by_id(self, user_id: str) -> User:
        return self.users.get(user_id)

    def store_fabs_file(self, file_data: FABSFileData):
        self.fabs_files.append(file_data)

    def add_flex_field(self, submission_id: str, flex_field: FlexField):
        if submission_id not in self.flex_fields:
            self.flex_fields[submission_id] = []
        self.flex_fields[submission_id].append(flex_field)

# Mock instance
db = BrokerDB()


# Implementing core features based on provided stories
class FABSManager:
    def __init__(self, db_instance: BrokerDB):
        self.db = db_instance

    def process_deletions_12192017(self):
        """As a Data user, I want to have the 12-19-2017 deletions processed"""
        print("Processing deletions from 12-19-2017...")
        # In a real system this would involve querying the database
        # and updating/deleting relevant records based on deletion logs.
        # Placeholder implementation
        return "Deletion processing completed."

    def generate_sample_file_link(self, submission_id: str) -> str:
        """FABS user wants link to correctly formatted sample file"""
        return f"https://usaspending.gov/sample_files/fabs_{submission_id}.csv"

    def validate_fabs_submission(self, submission_id: str) -> List[str]:
        """Validate FABS submission files with improved error reporting"""
        errors = []
        submission = self.db.get_submission_by_id(submission_id)
        if not submission:
            errors.append("Submission ID not found")
            return errors

        # Simulate validation checks
        if submission.status != "Validated":
            errors.append("Submission must be validated before publishing")

        return errors

    def handle_publish_button_state(self, submission_id: str):
        """User interaction - disabling publish button during derivation"""
        submission = self.db.get_submission_by_id(submission_id)
        if submission:
            if submission.status == "Publishing":  # Prevent double publishing
                return False  # Disable publish button
            submission.status = "Publishing"
            return True  # Enable publish button initially
        return False

    def derive_frec_data(self, submission_id: str):
        """Derive FREC codes to improve data consistency"""
        # Mock logic for FREC derivation
        print(f"Deriving FREC data for submission {submission_id}")
        return True

    def derive_office_names(self, office_codes: List[str]):
        """Data user wants to see office names derived from codes"""
        # Simple mapping example - would connect to actual lookup in real app
        office_map = {
            '0908': 'Bureau of Economic Analysis',
            '0201': 'Department of Commerce',
            '0601': 'Department of State'
        }
        office_names = [office_map.get(code, code) for code in office_codes]
        return office_names

    def load_historical_fabs_data(self, start_date: datetime, end_date: datetime):
        """Load historical FABS data including all necessary columns"""
        print(f"Loading historical FABS data from {start_date} to {end_date}")
        return ["Records loaded successfully"]

    def add_gtas_window_data(self):
        """Add GTAS window data to database"""
        print("Adding GTAS window data to ensure site lockdown during submission period")
        return True

    def get_file_with_errors(self, submission_id: str, file_type: str) -> str:
        """Get uploaded FABS file"""
        # In real implementation, this would pull from storage
        return f"Uploaded file for submission {submission_id}"

    def add_new_relief_data(self):
        """Integrate new relief data from external data sources"""
        print("Adding new relief data from external sources...")
        return True


class DABSManager:
    def __init__(self, db_instance: BrokerDB):
        self.db = db_instance

    def sync_d1_generation_with_fpds(self):
        """Sync D1 file generation with FPDS data load"""
        print("D1 file generation synchronized with FPDS data load")
        return True

    def update_error_messages(self):
        """Provide accurate error messages to users"""
        print("Updating error messages for clarity and accuracy")
        return True

    def set_readonly_access(self, user_id: str):
        """Set read-only access for DABS"""
        user = self.db.get_user_by_id(user_id)
        if user:
            if user.role == "FABS_USER":
                user.role = "RO_DABS_USER"
                print(f"Updated {user.name} to read-only access for DABS")
        return True


class UIUserInterface:
    def __init__(self, db_instance: BrokerDB):
        self.db = db_instance

    def redesign_resources_page(self):
        """Redesign resources page to match new broker design styles"""
        print("Resources page redesigned to match new Broker design styles")
        return True

    def schedule_user_testing(self, test_date: datetime, participants: List[str]):
        """Schedule user testing events"""
        print(f"Scheduled user testing on {test_date.strftime('%Y-%m-%d')} with participants: {', '.join(participants)}")
        return True

    def track_tech_thursday_issues(self, issue_log: Dict[str, str]):
        """Track issues raised in Tech Thursday meetings"""
        print("Tracking tech thursday issues:", issue_log)
        return True

    def report_user_testing_results(self, agency: str, findings: Dict[str, str]):
        """Report findings of user testing sessions"""
        print(f"Reporting user testing results for {agency}: {findings}")
        return True

    def create_ui_summary_report(self, summary_details: Dict[str, Any]):
        """Create a summary report of UI changes and improvements requested"""
        print("Creating comprehensive UI summary report")
        return True

    def update_help_page(self, page_number: int):
        """Update help page with latest design iterations"""
        print(f"Help page round {page_number} updated")
        return True

    def update_homepage(self, round_number: int):
        """Implement latest homepage design updates"""
        print(f"Homepage layout revision #{round_number} applied")
        return True

    def update_landing_page(self, page_type: str):
        """Update landing page navigation options"""
        print(f"Landing page for {page_type} updated")
        return True

    def display_updated_ui_changes(self, version_info: Dict[str, str]):
        """Display UI changes in a standardized format to stakeholders"""
        print("Showing updates for stakeholder review:", version_info)
        return True


class ValidationManager:
    def __init__(self, db_instance: BrokerDB):
        self.db = db_instance

    def apply_validation_rules_updates(self):
        """Apply DB-2213 rule updates to validation table"""
        print("Applying DB-2213 validation rule updates...")
        return True

    def validate_zip_plus_four(self, zip_code: str) -> bool:
        """Ensure PPoPZIP+4 behaves like Legal Entity ZIP"""
        # Simplified validation logic
        if "-" in zip_code:
            zip_parts = zip_code.split("-")
            return len(zip_parts[0]) == 5 and len(zip_parts[1]) == 4
        else:
            return len(zip_code) in [5, 9]

    def validate_duns_record(self, duns: str, action_type: str, registration_date: datetime) -> bool:
        """Validate DUNS records per specific rules"""
        # Placeholder validation logic
        if action_type in ['B', 'C', 'D']:
            # Accept even if expired, provided already registered
            return True
        elif registration_date < datetime.now():
            # Accept if after initial registration but before current date
            return True
        return False

    def ensure_correct_cfda_codes(self, error_code: str, record: Dict[str, Any]) -> str:
        """Clarify exactly what triggers CFDA error codes"""
        explanations = {
            "CFDA_ERROR_001": "Required CFDA number field is empty",
            "CFDA_ERROR_002": "Invalid CFDA number formatting",
            "CFDA_ERROR_003": "CFDA number not found in approved list"
        }
        return explanations.get(error_code, "Generic CFDA error")

    def validate_flexfield_required_elements(self, submission_id: str, flex_fields: List[FlexField]) -> List[str]:
        """Show warnings when only missing required elements exist"""
        errors = []
        for ff in flex_fields:
            if ff.value == "":
                errors.append(f"{ff.field_name} is required but empty")
        return errors

    def enforce_zero_padded_fields(self, fields: List[str]) -> List[str]:
        """Apply zero-padding consistently"""
        padded_fields = [f.zfill(10) for f in fields]
        return padded_fields

    def validate_legal_entity_address_line3_length(self, address_line3: str, max_length: int = 100) -> bool:
        """Validate legal entity address line 3 fits specification"""
        return len(address_line3) <= max_length

    def validate_loan_record_fields(self, is_loan_record: bool, field_values: Dict[str, str]) -> bool:
        """Accept zero/blank values for loan records"""
        for key, val in field_values.items():
            if val == "" or val == "0":
                continue
            elif not isinstance(val, (int, float)) or val != 0:
                return False
        return True

    def validate_poop_zip(self, pop_zip: str) -> bool:
        """Allow partial ZIP codes for cities"""
        if len(pop_zip) <= 5:
            return True
        return len(pop_zip) == 9 and pop_zip.isdigit()

    def validate_submission_periods(self, start_date: datetime, end_date: datetime):
        """Check if submission periods match expected dates"""
        now = datetime.utcnow()
        in_period = start_date <= now <= end_date
        return in_period

    def validate_file_structure(self, filename: str, expected_ext: str) -> bool:
        """Validate file type matches expected extension"""
        return filename.endswith(expected_ext)

    def get_submission_creator(self, submission_id: str) -> str:
        """Identify who created the submission"""
        submission = self.db.get_submission_by_id(submission_id)
        if submission:
            return submission.created_by
        return "Unknown"


class BackendDeveloper:
    def __init__(self, db_instance: BrokerDB):
        self.db = db_instance

    def improve_logging(self, submission_id: str):
        """Better logging for troubleshooting"""
        print(f"Enhanced logging for submission {submission_id}")
        return True

    def prevent_duplicates(self, submission_id: str):
        """Prevent duplicate publishing actions"""
        submission = self.db.get_submission_by_id(submission_id)
        # Check if already published
        return submission.status != "Published"

    def resolve_double_publishing_issue(self):
        """Handle refresh-based double publishing"""
        print("Handling issue preventing duplicate publications due to page refresh")
        return True

    def index_domain_models(self):
        """Index domain models for better query performance"""
        print("Optimizing DB indexes for validation queries")
        return True

    def fix_nonexistent_record_error_handling(self):
        """Fix handling of incorrect corrections"""
        print("Improved handling of attempts to modify non-existent records")
        return True

    def load_historical_data(self):
        """Historically load data for all systems"""
        print("Loading historical data for system consistency")
        return True

    def load_historical_fpds_data(self):
        """Load comprehensive FPDS historical data"""
        print("Loading combined historical FPDS data (extracted + feed)")
        return True

    def update_sql_codes(self):
        """Improve SQL code clarity"""
        print("Updated SQL codebase with clarifications")
        return True

    def validate_submission_derivation(self, submission_id: str) -> bool:
        """Ensure that submission data derives correctly"""
        submission = self.db.get_submission_by_id(submission_id)
        return submission is not None and len(submission.errors) == 0

    def optimize_d_file_generation_caching(self):
        """Manage and cache request for D Files"""
        print("Caching D file generation requests")
        return True


class FABSUser:
    def __init__(self, db_instance: BrokerDB):
        self.db = db_instance

    def submit_citywide_poop_zip(self, zip_code: str) -> bool:
        """Allow submission of city-wide ZIP codes"""
        return ValidationManager(self.db).validate_poop_zip(zip_code)

    def submit_with_quotes_around_fields(self, csv_row: str) -> str:
        """Ensure data fields are quoted like in spreadsheets"""
        # This simulates ensuring fields are wrapped quotes
        return csv_row.strip()


class DeveloperUser:
    def __init__(self, db_instance: BrokerDB):
        self.db = db_instance

    def access_broker_application_data(self):
        """Quickly access Broker application data"""
        print("Accessing Broker application data")
        return {"data": "Application logs retrieved"}

    def determine_best_fpds_load_approach(self):
        """Determine best strategy for loading FPDS data"""
        print("Evaluating strategies for loading FPDS history")
        return True

    def handle_fabs_groups_under_frec(self):
        """Establish support for FABS groupings under FREC paradigm"""
        print("FABS groups configured for FREC compliance")
        return True

    def validate_historical_data(self, submission_id: str):
        """Test that historical data includes required fields"""
        print(f"Validating historical submission {submission_id}")
        return True


class TestingEnvironment:
    def __init__(self, db_instance: BrokerDB):
        self.db = db_instance

    def provide_test_environment_access(self, env_name: str, user_role: str, enable_test_feature: bool = False):
        """Enable access in multiple environments for testing"""
        print(f"Provided access to '{env_name}' for {user_role} with features: {'Enabled' if enable_test_feature else 'Disabled'}")
        return True


class SystemOwner:
    def __init__(self, db_instance: BrokerDB):
        self.db = db_instance

    def reset_environment_permissions(self):
        """Reset to staging MAX permissions only"""
        print("Environment reset to Staging MAX permissions")
        return True

    def create_ui_sme_summary(self, ui_improvements_plan: Dict[str, str]):
        """Compile a summary based on UI SME's input"""
        print("Compiled UI improvements summary")
        return True

    def audit_ui_improvement_scope(self, proposal: Dict[str, Any]):
        """Audit scope of proposed UI enhancements"""
        print("Auditing UI enhancement scope")
        return True

    def design_ui_schedule(self, proposed_milestones: List[Dict[str, Any]]):
        """Define a project timeline for UI improvements"""
        print("UI improvement schedule designed")
        return True


# Implementation example usage
def demo_functionality():
    manager = FABSManager(db)
    ui = UIUserInterface(db)
    validation = ValidationManager(db)
    backend_dev = BackendDeveloper(db)
    
    # Create submission
    sub = db.create_submission(str(uuid.uuid4()), "New", "agency_user")
    
    # Test various functionalities
    message = manager.process_deletions_12192017()
    print(message)
    
    office_names = manager.derive_office_names(['0908', '0201'])
    print("Derived office names:", office_names)
    
    # Validate zip codes
    print("Zip validation:", validation.validate_zip_plus_four("12345-6789"))
    print("Incomplete ZIP OK:", validation.validate_poop_zip("123456"))
    
    # Schedule user test
    ui.schedule_user_testing(datetime.now(), ["agency1", "agency2"])
    
    # Test publishing logic
    result = manager.handle_publish_button_state(sub.submission_id)
    print("Publish button enabled:", result)
    
    # Logging
    backend_dev.improve_logging(sub.submission_id)
    
    # Report on user testing
    ui.report_user_testing_results("AgencyX", {"feedback": "Good UX", "errors": []})
    
    # Load historical data
    manager.load_historical_fabs_data(datetime(2020, 1, 1), datetime(2024, 1, 1))
    

if __name__ == "__main__":
    demo_functionality()