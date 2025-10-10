import datetime
import time
import threading
import logging
from collections import defaultdict
import hashlib

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')

# MOCK DATA STRUCTURES AND DATABASE SIMULATIONS

class SubmissionStatus:
    PENDING = "Pending"
    VALIDATING = "Validating"
    VALIDATED = "Validated"
    PUBLISHING = "Publishing"
    PUBLISHED = "Published"
    FAILED = "Failed"

class SubmissionType:
    FABS = "FABS"
    DABS = "DABS"


class Submission:
    _id_counter = 1
    
    def __init__(self, user, submission_type, data, published=False):
        self.id = Submission._id_counter
        Submission._id_counter += 1
        self.user = user
        self.submission_type = submission_type
        self.data = data
        self.status = SubmissionStatus.PENDING
        self.errors = []
        self.warnings = []
        self.published = published
        self.publish_status_change_timestamp = None
        self.created_at = datetime.datetime.now()
        self.updated_at = datetime.datetime.now()
        self.derived_fields = {}
        self.submission_hash = self._compute_hash()  # to prevent duplicates
        self.locked_publish = False  # disables publish button if derivations happening
    
    def _compute_hash(self):
        # hash of submission data dictionary for duplicate detection
        return hashlib.sha256(str(sorted(self.data.items())).encode('utf-8')).hexdigest()
    
    def update_status(self, new_status):
        self.status = new_status
        self.updated_at = datetime.datetime.now()
        if new_status == SubmissionStatus.PUBLISHED:
            self.publish_status_change_timestamp = self.updated_at
    
    def add_error(self, error_msg):
        self.errors.append(error_msg)
        self.updated_at = datetime.datetime.now()
    
    def add_warning(self, warning_msg):
        self.warnings.append(warning_msg)
        self.updated_at = datetime.datetime.now()
    
    def derive_fields(self):
        # Simulate derivation logic: e.g. office names, codes, PPoPCode modifications
        # Add FundingAgencyCode derivation if missing
        if 'FundingAgencyCode' not in self.data or not self.data['FundingAgencyCode']:
            self.derived_fields['FundingAgencyCode'] = self.data.get('AgencyCode', 'UnknownAgency')
        # PPoPCode special cases:
        ppop_code = self.data.get('PPoPCode')
        if ppop_code in [None, '00*****', '00FORGN']:
            self.derived_fields['PPoPCode'] = 'DerivedSpecialCode'
        else:
            self.derived_fields['PPoPCode'] = ppop_code
        
        # Derive office names from office codes
        office_code = self.data.get('OfficeCode')
        if office_code:
            # Mock office names lookup
            office_names = {'001': 'Office of Management', '002': 'Office of Finance'}
            self.derived_fields['OfficeName'] = office_names.get(office_code, 'Unknown Office')
        self.updated_at = datetime.datetime.now()

    def validate(self):
        self.errors.clear()
        self.warnings.clear()
        # Validate based on sample rules in stories:
        # Validate file extension if provided
        filename = self.data.get('filename')
        if filename and not filename.endswith('.csv'):
            self.add_error("File extension invalid - must be .csv")
        
        # Validate DUNS logic: Accept ActionTypes B,C,D if DUNS registered even if expired
        action_type = self.data.get('ActionType')
        duns_registered = self.data.get('DUNS_registered', False)
        if action_type in ['B', 'C', 'D'] and not duns_registered:
            self.add_error("DUNS validation failed: registration required for ActionTypes B,C,D")
        
        # Loan records accept zero or blank for specific fields
        is_loan = self.data.get('RecordType') == 'Loan'
        loan_field = self.data.get('LoanAmount')
        if is_loan and (loan_field is None or loan_field == 0):
            self.add_warning("Loan amount is zero or blank - acceptable for loan records.")
        
        # ZIP validations: Accept if missing last 4 digits or citywide PPoPZIP
        zip_code = self.data.get('PPoPZIP')
        legal_zip = self.data.get('LegalEntityZIP')
        if zip_code and (len(zip_code) == 5 or len(zip_code) == 9):
            pass  # okay
        else:
            self.add_error("PPoPZIP invalid length")
        if legal_zip and (len(legal_zip) == 5 or len(legal_zip) == 9):
            pass
        else:
            self.add_error("Legal Entity ZIP invalid length")
        
        # Required elements check with flexfields included in errors/warnings
        missing_required = []
        required_fields = ['AgencyCode', 'SubmissionType', 'PPoPCode']
        for rf in required_fields:
            if not self.data.get(rf):
                missing_required.append(rf)
        if missing_required:
            self.add_error(f"Missing required fields: {', '.join(missing_required)}. Flexfields provided in file will appear in warnings/errors.")
        
        # Prevent double publishing same submission
        if self.published:
            self.add_error("Submission already published, duplicate publishing prevented.")
        
        # Validate with updates on DB-2213 (mock)
        # Assuming new validation rules for 'RuleX'
        if self.data.get('RuleX', 0) < 0:
            self.add_error("RuleX must be non-negative.")
        
        # Check for empty or zero for non-loan records for FABS rules
        if not is_loan and (loan_field is None or loan_field == 0):
            self.add_warning("LoanAmount is zero or blank for non-loan record (allowed).")
        
        # Validate CFDA error codes description
        cfda = self.data.get('CFDA')
        if cfda and not isinstance(cfda, str):
            self.add_error("CFDA error: must be string describing the program code.")
        
    def publish(self):
        if self.locked_publish:
            return "Publish button is deactivated - derivations are in progress."
        if self.status != SubmissionStatus.VALIDATED:
            return "Cannot publish submission not validated successfully."
        if self.published:
            return "Submission is already published."
        # Simulate publishing process
        # Prevent double publishing by checking recent published submissions with same hash
        global SUBMISSIONS_DB
        for sub in SUBMISSIONS_DB:
            if sub.submission_hash == self.submission_hash and sub.published:
                return "Duplicate submission detected - publishing prevented."
        self.update_status(SubmissionStatus.PUBLISHING)
        time.sleep(0.1)  # simulate publishing delay
        self.published = True
        self.update_status(SubmissionStatus.PUBLISHED)
        # Update publishStatus changes timestamp
        self.publish_status_change_timestamp = datetime.datetime.now()
        return "Submission successfully published."
    
    def lock_publish_button(self):
        self.locked_publish = True
    
    def unlock_publish_button(self):
        self.locked_publish = False
    
    def get_error_messages(self):
        # Return errors including flexfields info when validation error is missing required element only
        if len(self.errors) == 1 and "Missing required fields" in self.errors[0]:
            flexfields = {k:v for k,v in self.data.items() if k.startswith('flexfield')}
            if flexfields:
                flex_msg = f" Flexfields included: {', '.join(flexfields.keys())}."
                return self.errors[0] + flex_msg
        return " | ".join(self.errors)
    
    def download_uploaded_file(self):
        # Return filename and pretend to return file contents
        filename = self.data.get('filename')
        if filename:
            return f"Downloading file: {filename}"
        else:
            return "No uploaded file found."


SUBMISSIONS_DB = []

# Prevent duplicate submissions and batch caching for D file generation requests
class DFileGenerationManager:
    def __init__(self):
        # cache keyed by (date_of_data_load)
        self.lock = threading.Lock()
        self.cache = {}
    
    def request_generation(self, data_load_date):
        with self.lock:
            if data_load_date in self.cache:
                return f"D File for {data_load_date} already generated. Using cached file."
            # Simulate file generation processing
            self.cache[data_load_date] = f"D_File_{data_load_date}.dat"
            time.sleep(0.1)  # simulate delay
            return f"D File for {data_load_date} generated."
    
    def clear_cache(self):
        with self.lock:
            self.cache.clear()

# Logging improvements
class Logger:
    @staticmethod
    def log_submission(submission):
        logging.info(f"Submission ID: {submission.id} Status: {submission.status} Errors: {len(submission.errors)} Warnings: {len(submission.warnings)} Updated: {submission.updated_at}")

    @staticmethod
    def log_error(message):
        logging.error(message)

    @staticmethod
    def log_info(message):
        logging.info(message)

# User testing and UI scheduling/tracking
class UserTestingManager:
    def __init__(self):
        self.scheduled_tests = []  # (date, description)
        self.test_reports = []
        self.issue_tracker = defaultdict(list)  # mapping from tech_thursday_date to issues
    
    def schedule_test(self, test_date, description):
        self.scheduled_tests.append((test_date, description))
    
    def report_issues(self, tech_thursday_date, issues):
        self.issue_tracker[tech_thursday_date].extend(issues)
    
    def submit_testing_report(self, summary, reporter):
        self.test_reports.append({"summary": summary, "reporter": reporter, "date": datetime.datetime.now()})
    
    def get_tests(self):
        return self.scheduled_tests
    
    def get_issues(self):
        return dict(self.issue_tracker)

# Environment access control
class EnvironmentManager:
    def __init__(self):
        self.allowed_permissions = {"production": set(), "staging": {"MAX"}, "development": {"MAX", "TESTER"}}
        self.current_env = "staging"
    
    def reset_to_staging_max_permissions(self):
        self.current_env = "staging"
        self.allowed_permissions[self.current_env] = {"MAX"}
        Logger.log_info("Environment reset to Staging MAX permissions.")
    
    def add_permission(self, env, perm):
        self.allowed_permissions.setdefault(env, set()).add(perm)
    
    def can_access(self, env, permission):
        return permission in self.allowed_permissions.get(env, set())
    
    def set_environment(self, env):
        self.current_env = env

# Data loading and derivation
class HistoricalDataLoader:
    def __init__(self):
        self.historical_fabs_loaded = False
        self.historical_fpds_loaded = False
    
    def load_historical_fabs(self):
        # simulate loading and deriving all fields
        self.historical_fabs_loaded = True
        Logger.log_info("Historical FABS data loaded with all necessary columns and FREC derivations.")
    
    def load_historical_fpds(self):
        # load extracted historical data and FPDS feed data since 2007
        self.historical_fpds_loaded = True
        Logger.log_info("Historical FPDS data loaded including extracted and feed data since 2007.")
    
# Broker resources updates
def update_broker_resources_and_validations():
    Logger.log_info("Broker resources, validations and P&P pages updated for FABS and DAIMS v1.1 launch.")

# DUNS validation for specific conditions
def validate_duns(action_type, duns_status, action_date, registration_date, initial_registration_date):
    # action_type: one of submission data action types
    # duns_status: bool, is DUNS registered?
    # action_date, registration_date, initial_registration_date: datetime dates
    if action_type in ['B', 'C', 'D'] and not duns_status:
        return False, "DUNS required for action types B, C, D."
    if action_date and registration_date:
        if not (initial_registration_date <= action_date <= registration_date):
            return False, "Action date outside DUNS registration period."
    return True, "DUNS validation passed."


# Demo functional capabilities implementations

dfile_manager = DFileGenerationManager()
env_manager = EnvironmentManager()
user_testing_mgr = UserTestingManager()
historical_data_loader = HistoricalDataLoader()


def submit_fabs_data(user, data, filename=None):
    # Enforce schema v1.1 headers (mock check)
    required_headers = {'AgencyCode', 'SubmissionType', 'PPoPCode', 'FundingAgencyCode'}
    if not required_headers.issubset(set(data.keys())):
        raise ValueError("Submission missing required headers per schema v1.1")
    data['filename'] = filename
    submission = Submission(user, SubmissionType.FABS, data)
    # Derive fields
    submission.derive_fields()
    submission.validate()
    SUBMISSIONS_DB.append(submission)
    Logger.log_submission(submission)
    return submission

def publish_submission(submission_id):
    submission = next((s for s in SUBMISSIONS_DB if s.id == submission_id), None)
    if not submission:
        return "Submission not found."
    # Lock publish button while derivations run
    submission.lock_publish_button()
    submission.derive_fields()
    # Unlock publish button after derivation finishes
    submission.unlock_publish_button()
    # Update publishStatus on change
    result = submission.publish()
    Logger.log_submission(submission)
    return result

def generate_d_file_for_date(data_load_date):
    # Sync with FPDS data load (simulate)
    result = dfile_manager.request_generation(data_load_date)
    return result

def get_published_fabs_files():
    # Return all published FABS file names (simulate)
    files = [sub.data.get('filename') for sub in SUBMISSIONS_DB if sub.published and sub.submission_type == SubmissionType.FABS]
    return [f for f in files if f]

def report_user_testing_to_agencies(summary, reporter):
    user_testing_mgr.submit_testing_report(summary, reporter)
    Logger.log_info(f"User testing summary submitted by {reporter}")

def schedule_user_testing(test_date, description):
    user_testing_mgr.schedule_test(test_date, description)
    Logger.log_info(f"User testing scheduled on {test_date} - {description}")

def track_tech_thursday_issues(tech_date, issues_list):
    user_testing_mgr.report_issues(tech_date, issues_list)
    Logger.log_info(f"Tech Thursday issues tracked for {tech_date}")

def reset_environment_permissions():
    env_manager.reset_to_staging_max_permissions()

def lookup_office_name_by_code(code):
    offices = {'001': 'Office of Management', '002': 'Office of Finance', '003': 'Office of Audit'}
    return offices.get(code, 'Unknown Office')

def clarify_cfda_error(cfda_code):
    # Provide detailed explanation on CFDA error trigger
    if not cfda_code:
        return "CFDA code missing - required for validation."
    if not isinstance(cfda_code, str):
        return "CFDA code must be string format."
    # Mock responses
    known_cfda_codes = ['10.001', '20.002', '30.003']
    if cfda_code not in known_cfda_codes:
        return f"CFDA code {cfda_code} not recognized."
    return "CFDA code valid."

def simulate_fabs_submission_workflow():
    # Example of a FABS user submitting a file
    
    data = {
        'AgencyCode': 'AGY123',
        'SubmissionType': 'FABS',
        'PPoPCode': None,  # Test derivation
        'FundingAgencyCode': '',
        'OfficeCode': '002',
        'RecordType': 'Loan',
        'LoanAmount': 0,
        'ActionType': 'B',
        'DUNS_registered': True,
        'filename': 'submission_fabs.csv',
        'RuleX': 1,
        'CFDA': '10.001',
        'PPoPZIP': '12345',
        'LegalEntityZIP': '12345-6789',
        'flexfield1': 'Some flex data',
        'flexfield2': 'More flex data',
    }
    submission = submit_fabs_data("user1", data, filename='submission_fabs.csv')
    # Validate DUNS with dates
    valid_duns, msg = validate_duns(data['ActionType'], data['DUNS_registered'], datetime.datetime.now(), datetime.datetime.now()+datetime.timedelta(days=1), datetime.datetime.now()-datetime.timedelta(days=30))
    if not valid_duns:
        submission.add_error(msg)
    # Publish
    publish_msg = publish_submission(submission.id)
    return publish_msg, submission.get_error_messages(), submission.derived_fields


# Run demonstration of core flow (this simulates many user stories)
if __name__ == "__main__":
    # Load historical data
    historical_data_loader.load_historical_fabs()
    historical_data_loader.load_historical_fpds()
    
    # Submit and publish FABS
    publish_msg, errors, derived = simulate_fabs_submission_workflow()
    print(publish_msg)
    print("Errors:", errors)
    print("Derived fields:", derived)
    
    # Generate D file
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    print(generate_d_file_for_date(today_str))
    print(generate_d_file_for_date(today_str))  # Should use cache
    
    # Reset environment permissions
    reset_environment_permissions()
    
    # Schedule and report user testing
    test_date = datetime.datetime.now() + datetime.timedelta(days=7)
    schedule_user_testing(test_date, "Round 2 of Homepage edits user testing")
    report_user_testing_to_agencies("User testing completed with key feedback info.", "UI Designer")
    
    # Track Tech Thursday issues
    tech_thursday_date = datetime.datetime.now().date()
    track_tech_thursday_issues(tech_thursday_date, ["Issue A", "Issue B"])
    
    # Access published FABS files
    files = get_published_fabs_files()
    print("Published FABS files:", files)
    
    # Clarify CFDA error example
    print(clarify_cfda_error("99.999"))
    print(clarify_cfda_error("10.001"))
    
    # Logging demonstration
    for s in SUBMISSIONS_DB:
        Logger.log_submission(s)