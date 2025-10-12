import logging
import datetime
from typing import List, Dict, Optional
from enum import Enum

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('broker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PublishStatus(Enum):
    DRAFT = "draft"
    PUBLISHED = "published"
    VALIDATED = "validated"
    REJECTED = "rejected"

class FABSSubmission:
    def __init__(self, id: str, publish_status: PublishStatus):
        self.id = id
        self.publish_status = publish_status
        self.created_at = datetime.datetime.now()
        self.updated_at = datetime.datetime.now()
        self.file_content = None
        self.errors: List[str] = []
        self.warnings: List[str] = []
        
    def update_publish_status(self, new_status: PublishStatus):
        self.publish_status = new_status
        self.updated_at = datetime.datetime.now()
        logger.info(f"Submission {self.id} status changed to {new_status.value}")
        
    def validate(self):
        # Implementation of validation logic
        self.errors = []
        self.warnings = []
        
        if not self.file_content:
            self.errors.append("File content is empty")
            return False
            
        # Example validation checks
        if "FundingAgencyCode" not in self.file_content:
            self.errors.append("FundingAgencyCode is missing")
            
        if len(self.errors) == 0:
            self.publish_status = PublishStatus.VALIDATED
            return True
        else:
            self.publish_status = PublishStatus.REJECTED
            return False
            
    def publish(self):
        if self.publish_status != PublishStatus.VALIDATED:
            logger.error(f"Cannot publish submission {self.id} with status {self.publish_status.value}")
            return False
            
        # Prevent double publishing
        self.publish_status = PublishStatus.PUBLISHED
        logger.info(f"Submission {self.id} published successfully")
        return True

class BrokerDatabase:
    def __init__(self):
        self.submissions: Dict[str, FABSSubmission] = {}
        self.gtas_lock_period = None
        
    def add_submission(self, submission: FABSSubmission):
        if submission.id in self.submissions:
            logger.warning(f"Submission {submission.id} already exists")
            return False
            
        self.submissions[submission.id] = submission
        return True
        
    def get_submission(self, submission_id: str) -> Optional[FABSSubmission]:
        return self.submissions.get(submission_id)
        
    def set_gtas_window(self, start: datetime.datetime, end: datetime.datetime):
        self.gtas_lock_period = (start, end)
        logger.info(f"GTAS window set from {start} to {end}")
        
    def is_gtas_locked(self) -> bool:
        if not self.gtas_lock_period:
            return False
            
        now = datetime.datetime.now()
        return self.gtas_lock_period[0] <= now <= self.gtas_lock_period[1]

class FileGenerator:
    @staticmethod
    def generate_d_file(fabs_data: dict, fpds_data: dict) -> str:
        """Generate D file from FABS and FPDS data"""
        # Implementation of D file generation logic
        return "Generated D file content"
        
    @staticmethod
    def sync_with_fpds(fpds_update_time: datetime.datetime) -> bool:
        """Check if FPDS data is up to date"""
        return datetime.datetime.now() - fpds_update_time < datetime.timedelta(days=1)

class UserTestingManager:
    def __init__(self):
        self.testing_schedule = {}
        self.issues = []
        
    def schedule_testing(self, test_name: str, date: datetime.date, participants: list):
        self.testing_schedule[test_name] = {
            'date': date,
            'participants': participants,
            'completed': False
        }
        logger.info(f"Scheduled test '{test_name}' for {date}")
        
    def record_issue(self, issue: str, severity: str = "medium"):
        self.issues.append({
            'description': issue,
            'severity': severity,
            'date': datetime.datetime.now(),
            'resolved': False
        })
        
    def generate_test_summary(self) -> dict:
        return {
            'scheduled_tests': list(self.testing_schedule.keys()),
            'open_issues': [i for i in self.issues if not i['resolved']],
            'completed_tests': [t for t in self.testing_schedule if self.testing_schedule[t]['completed']]
        }

class ValidationRules:
    def __init__(self):
        self.rules = {
            'DUNS': self.validate_duns,
            'PPoPCode': self.validate_ppop_code,
            'LegalEntityAddressLine3': self.validate_address_line,
            # Add other validation rules
        }
        
    def validate_duns(self, value: str, action_type: str, registration_date: datetime.date) -> bool:
        """Validate DUNS according to specific rules"""
        if action_type in ['B', 'C', 'D']:
            return True
        if len(value) != 9 or not value.isdigit():
            return False
        return True
        
    def validate_ppop_code(self, value: str) -> bool:
        """Validate PPoPCode including special cases"""
        if value in ['00*****', '00FORGN']:
            return True
        return len(value) == 5 and value.isdigit()
        
    def validate_address_line(self, value: str) -> bool:
        """Validate address line length"""
        return len(value) <= 150 if value else True

class UIDesign:
    def __init__(self):
        self.page_designs = {
            'homepage': {'version': 1, 'approved': False},
            'resources': {'version': 1, 'approved': False},
            'help': {'version': 1, 'approved': False},
            'dabs_landing': {'version': 1, 'approved': False},
            'fabs_landing': {'version': 1, 'approved': False}
        }
        
    def update_design(self, page: str, version: int):
        if page in self.page_designs:
            self.page_designs[page]['version'] = version
            self.page_designs[page]['approved'] = False
            logger.info(f"Updated {page} to version {version}")
            
    def approve_design(self, page: str):
        if page in self.page_designs:
            self.page_designs[page]['approved'] = True
            logger.info(f"Approved {page} version {self.page_designs[page]['version']}")

class HistoricalDataLoader:
    def load_fabs_history(self, data: list, include_frec_derivations: bool = True):
        """Load historical FABS data with optional FREC derivations"""
        logger.info(f"Loading {len(data)} historical FABS records")
        if include_frec_derivations:
            logger.info("Including FREC derivations")
        # Implementation would process each record and insert into database
            
    def load_fpds_history(self, data: list, include_feed_data: bool = True):
        """Load historical FPDS data"""
        logger.info(f"Loading {len(data)} historical FPDS records")
        if include_feed_data:
            logger.info("Including FPDS feed data")
        # Implementation would process each record and insert into database

class NewRelicMonitor:
    def __init__(self):
        self.metrics = {}
        
    def track_metric(self, app_name: str, metric_name: str, value: float):
        if app_name not in self.metrics:
            self.metrics[app_name] = {}
        self.metrics[app_name][metric_name] = value
        logger.debug(f"Tracked {metric_name} for {app_name}: {value}")
        
    def get_metrics(self) -> dict:
        return self.metrics

class PermissionManager:
    def __init__(self):
        self.permissions = {
            'staging': {'max': True, 'fabs_testers': True},
            'production': {'max': False, 'fabs_testers': False}
        }
        
    def reset_environment(self, env: str):
        """Reset permissions to only allow Staging MAX access"""
        if env in self.permissions:
            self.permissions[env] = {'max': env == 'staging', 'fabs_testers': False}
            logger.info(f"Reset {env} permissions to staging defaults")

class ErrorHandler:
    def __init__(self):
        self.error_messages = {
            'invalid_extension': "The file extension is invalid. Please upload a file with the correct extension.",
            'missing_header': "Required header '{header}' is missing from the file.",
            'validation_failed': "Validation failed with {count} errors and {warnings} warnings.",
            'duns_validation': "DUNS validation failed. Please check the DUNS number and try again."
        }
        
    def get_error_message(self, error_code: str, **kwargs) -> str:
        msg = self.error_messages.get(error_code, "An unknown error occurred")
        return msg.format(**kwargs)