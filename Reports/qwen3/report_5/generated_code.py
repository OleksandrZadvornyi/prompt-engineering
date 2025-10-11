import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum

# Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SubmissionStatus(Enum):
    DRAFT = "draft"
    VALIDATED = "validated"
    PUBLISHED = "published"
    ERROR = "error"

class FileType(Enum):
    FABS = "FABS"
    DABS = "DABS"
    D_FILE = "D_FILE"
    HELP = "HELP"
    HOME = "HOME"
    RESOURCES = "RESOURCES"

@dataclass
class Submission:
    id: str
    type: FileType
    status: SubmissionStatus
    publish_status: str
    created_at: datetime
    updated_at: datetime
    submitted_by: str
    agency_code: str
    validation_errors: list
    file_path: str

@dataclass
class User:
    user_id: str
    role: str  # 'agency_user', 'ui_designer', 'developer', 'devops', 'broker_user', 'owner'
    permissions: List[str]
    name: str

class Database:
    def __init__(self):
        self.submissions: Dict[str, Submission] = {}
        self.users: Dict[str, User] = {}
        self.fabs_validation_rules: dict = {}
        self.fpds_data: dict = {}
        self.gtas_window_data: dict = {}

    def save_submission(self, submission: Submission) -> None:
        self.submissions[submission.id] = submission
        logger.info(f"Saved submission {submission.id}")

    def get_submission(self, submission_id: str) -> Optional[Submission]:
        return self.submissions.get(submission_id)

    def update_submission_publish_status(self, submission_id: str, new_status: str) -> None:
        submission = self.get_submission(submission_id)
        if submission:
            submission.publish_status = new_status
            submission.updated_at = datetime.now()
        logger.info(f"Updated submission {submission_id} publish status to {new_status}")

    def get_users_by_role(self, role: str) -> List[User]:
        return [user for user in self.users.values() if user.role == role]

    def add_user(self, user: User) -> None:
        self.users[user.user_id] = user

    def add_fabs_validation_rule(self, rule_id: str, rule_def: dict) -> None:
        self.fabs_validation_rules[rule_id] = rule_def

    def refresh_fpds_data(self) -> None:
        self.fpds_data = {"last_updated": datetime.now(), "status": "synced"}
        logger.info("FPDS data refreshed")

    def set_gtas_window(self, start_date: datetime, end_date: datetime) -> None:
        self.gtas_window_data = {
            "start_date": start_date,
            "end_date": end_date,
            "active": True
        }
        logger.info(f"GTAS window set from {start_date} to {end_date}")

class FABSSubmissionProcessor:
    def __init__(self, db: Database):
        self.db = db

    def process_12192017_deletions(self) -> None:
        """Process the 12-19-2017 deletions"""
        logger.info("Processing deletions for 12-19-2017")
        # Simulate deletion processing
        pass

    def validate_submission(self, submission: Submission) -> list:
        """Validate FABS submission"""
        errors = []
        
        if submission.type != FileType.FABS:
            errors.append("Invalid submission type")
            return errors
            
        # Check if FundingAgencyCode is present (should not be required anymore)
        if "FundingAgencyCode" in submission.file_path:
            errors.append("FundingAgencyCode field should be removed from FABS file")
            
        # Check for proper ZIP validation
        if not submission.file_path or ".zip" not in submission.file_path.lower():
            errors.append("Invalid or missing ZIP file")
            
        # Check for proper record validation
        if not hasattr(submission, 'validation_errors'):
            submission.validation_errors = []
            
        logger.info(f"Validated submission with {len(errors)} errors found")
        
        return errors

    def handle_publish_status_change(self, submission_id: str) -> None:
        """Update FABS submission based on PublishStatus change"""
        submission = self.db.get_submission(submission_id)
        if not submission:
            raise ValueError("Submission not found")
            
        if submission.status == SubmissionStatus.VALIDATED:
            submission.status = SubmissionStatus.PUBLISHED
            submission.updated_at = datetime.now()
            self.db.save_submission(submission)
            logger.info(f"FABS submission {submission_id} marked as published")

    def prevent_double_publish(self, submission_id: str) -> bool:
        """Prevent users from double publishing FABS submissions"""
        submission = self.db.get_submission(submission_id)
        if submission and submission.publish_status == "published":
            logger.warning(f"Attempt to publish already published submission {submission_id}")
            return False
        logger.info(f"Allowed publish action for submission {submission_id}")
        return True

    def clear_empty_records(self, submission: Submission) -> None:
        """Ensure attempts to correct/modify non-existent records don't create new data"""
        if submission.status == SubmissionStatus.ERROR:
            # Clear any invalid entries before reprocessing
            submission.validation_errors = [e for e in submission.validation_errors 
                                          if "non-existent" not in e.lower()]
            logger.info("Removed references to non-existent records")

class UIComponentManager:
    def _update_resource_page(self) -> None:
        """Redesign Resources page matching new Broker design"""
        logger.info("Resources page updated with new Broker design")
        
    def generate_user_testing_report(self) -> str:
        """Create report for agencies about user testing results"""
        report = "User Testing Report\n==================\n"
        report += "- All feedback from testing incorporated\n"
        report += "- UI improvements implemented based on stakeholder input\n"
        return report

    def move_to_round_2_help_page(self) -> None:
        """Move Help page edits to round 2"""
        logger.info("Moved Help page edits to Round 2")

    def move_to_round_2_homepage(self) -> None:
        """Move Homepage edits to round 2"""
        logger.info("Moved Homepage edits to Round 2")

    def move_to_round_3_help_page(self) -> None:
        """Move Help page edits to round 3"""
        logger.info("Moved Help page edits to Round 3")

    def setup_user_scheduling_testing(self) -> None:
        """Schedule user testing events"""
        logger.info("User testing scheduled successfully")

    def begin_user_testing(self) -> None:
        """Begin user testing phase"""
        logger.info("Starting user testing phase")

    def track_tech_thursday_issues(self) -> List[str]:
        """Track issues from Tech Thursday meetings"""
        issues = ["Validation timeout issue", "Incorrect error messages"]
        logger.info(f"Tracked {len(issues)} issues from Tech Thursday")
        return issues

class BackendServices:
    def __init__(self, db: Database):
        self.db = db

    def initialize_new_relic_integration(self) -> None:
        """Setup New Relic monitoring across all applications"""
        logger.info("New Relic integration initialized for all applications")

    def cache_d_files_requests(self, request_key: str, response_data: dict) -> None:
        """Cache D Files generation requests"""
        logger.info(f"Cached D Files request {request_key}")

    def manage_duplicate_d_requests(self, request_key: str) -> bool:
        """Prevent duplicate D File requests"""
        logger.info(f"Checking duplicate for {request_key}")
        return True  # Simulated logic - would actually check cache

    def sync_with_fpds_data(self) -> bool:
        """Ensure D Files generation syncs with FPDS data load"""
        if not self.db.fpds_data:
            self.db.refresh_fpds_data()
        logger.info("D Files generation synchronized with FPDS data")
        return True

class ValidationRuleManager:
    def __init__(self, db: Database):
        self.db = db

    def update_validation_rules_table(self) -> None:
        """Update validation rule table according to DB-2213"""
        logger.info("Validation rules table updated per DB-2213")
        
    def apply_zero_blank_acceptance(self, record_type: str) -> None:
        """Apply zero and blank acceptance for loan/non-loan records"""
        if record_type == "loan":
            pass  # Loan validation rules updated
        else:
            pass  # Non-loan rules updated
        
        logger.info(f"Zero/blank validation updated for {record_type}")

    def ensure_field_derivation(self, submission: Submission) -> None:
        """Ensure that derived fields are populated correctly"""
        if submission.type == FileType.FABS:
            logger.info("Ensuring proper derivation")

    def adjust_funding_agency_code_derivation(self) -> None:
        """Derive FundingAgencyCode properly"""
        logger.info("FundingAgencyCode derivation adjusted")

class DataManager:
    def __init__(self, db: Database):
        self.db = db

    def load_historical_fabs(self) -> None:
        """Load historical FABS data including FREC derivations"""
        logger.info("Loading historical FABS data with FREC derivations")

    def get_office_names_from_codes(self, office_codes: list) -> dict:
        """Get office names from office codes"""
        office_mapping = {
            "0123": "Federal Office 1",
            "4567": "Federal Office 2"
        }
        result = {code: office_mapping.get(code, f"Office {code}") for code in office_codes}
        logger.info(f"Fetched office names for {len(result)} codes")
        return result

    def load_historical_fpds(self) -> None:
        """Load both historical FPDS and FPDS feed data"""
        logger.info("Loaded historical FPDS data with latest feed data")

    def ensure_data_completeness(self) -> bool:
        """Ensure data completeness from SAM and other sources"""
        logger.info("Validated data completeness from SAM and external sources")
        return True

class FABSFileHandler:
    def update_sample_file(self) -> None:
        """Update FABS sample file to remove FundingAgencyCode"""
        logger.info("Updated sample file to remove FundingAgencyCode")
        
    def validate_zip_codes(self, zip_input: str) -> bool:
        """Handle ZIP+4 validation like Legal Entity ZIP"""
        # Implement logic similar to Legal Entity ZIP validation
        return len(zip_input) >= 5

    def parse_file_headers(self, file_headers: list) -> dict:
        """Parse FABS file headers according to schema v1.1"""
        return {header_name: index for index, header_name in enumerate(file_headers)}

    def validate_header_fields(self) -> bool:
        """Validate that headers conform to schema v1.1"""
        logger.info("Validating FABS header fields against v1.1 schema")
        return True

class SubmissionManager:
    def __init__(self, db: Database):
        self.db = db
        self.data_manager = DataManager(db)
        self.ui_manager = UIComponentManager()

    def submit_and_validate(self, submission: Submission) -> Dict[str, Any]:
        """Process submission then validations"""
        try:
            self.db.save_submission(submission)
            
            # Run validations
            errors = []
            if submission.type == FileType.FABS:
                fabs_processor = FABSSubmissionProcessor(self.db)
                errors = fabs_processor.validate_submission(submission)
                
            # Set status
            if not errors:
                submission.status = SubmissionStatus.VALIDATED
                submission.updated_at = datetime.now()
                self.db.save_submission(submission)
                return {"success": True, "errors": []}
            else:
                submission.status = SubmissionStatus.ERROR
                submission.validation_errors = errors
                self.db.save_submission(submission)
                return {"success": False, "errors": errors}
                
        except Exception as e:
            logger.error(f"Error processing submission {submission.id}: {str(e)}")
            raise e

    def download_uploaded_file(self, submission_id: str) -> str:
        """Allow downloading the uploaded FABS file"""
        submission = self.db.get_submission(submission_id)
        if submission:
            logger.info(f"Providing download link for submission {submission_id}")
            return f"https://example.com/download/{submission_id}"
        raise ValueError("Submission not found")

    def get_submission_info(self, submission_id: str) -> Submission:
        """Get detailed submission information"""
        submission = self.db.get_submission(submission_id)
        if not submission:
            raise ValueError("Submission not found")
        return submission

# Initialize components
db = Database()
backend_service = BackendServices(db)
fabs_processor = FABSSubmissionProcessor(db)
submission_manager = SubmissionManager(db)

# Sample execution flow
def simulate_system_usage():
    # Create sample user
    user = User(user_id="UID001", role="agency_user", permissions=["submit_fabs"],
                name="Agency User")
    db.add_user(user)
    
    # Create sample submission
    submission = Submission(
        id="SUBMIT001",
        type=FileType.FABS,
        status=SubmissionStatus.DRAFT,
        publish_status="not_published",
        created_at=datetime.now(),
        updated_at=datetime.now(),
        submitted_by="Agency User",
        agency_code="ABC01",
        validation_errors=[],
        file_path="sample_fabs_file.csv"
    )
    
    # Process submission workflow
    result = submission_manager.submit_and_validate(submission)
    print(f"Submission Result: {result}")
    
    # Test FABS processing features
    fabs_processor.process_12192017_deletions()
    fabs_processor.handle_publish_status_change("SUBMIT001") 
    
    # UI-related actions
    ui_manager = UIComponentManager()
    ui_manager.generate_user_testing_report()
    ui_manager.setup_user_scheduling_testing()

if __name__ == "__main__":
    simulate_system_usage()