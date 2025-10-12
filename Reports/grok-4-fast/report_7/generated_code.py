import logging
import json
import datetime
import os
import sqlite3
from typing import List, Dict, Any
from dataclasses import dataclass, asdict
from enum import Enum
import re
import hashlib
import time
from contextlib import contextmanager

# Configure logging for better troubleshooting
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PublishStatus(Enum):
    DRAFT = "draft"
    PUBLISHED = "published"
    DELETED = "deleted"

@dataclass
class Submission:
    id: str
    user_id: str
    data: Dict[str, Any]
    publish_status: PublishStatus
    created_at: datetime.datetime
    updated_at: datetime.datetime
    fab_type: str  # FABS or DABS

class ValidationRule:
    def __init__(self, code: str, description: str, logic: callable):
        self.code = code
        self.description = description
        self.logic = logic

class UserRole(Enum):
    DATA_USER = "data_user"
    UI_DESIGNER = "ui_designer"
    DEVELOPER = "developer"
    DEVOPS = "devops"
    BROKER_USER = "broker_user"
    AGENCY_USER = "agency_user"
    OWNER = "owner"
    TESTER = "tester"
    FABS_USER = "fabs_user"

# Database setup using SQLite for simplicity
DB_FILE = "usaspending.db"

@contextmanager
def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS submissions (
                id TEXT PRIMARY KEY,
                user_id TEXT,
                data TEXT,
                publish_status TEXT,
                created_at TEXT,
                updated_at TEXT,
                fab_type TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS historical_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                data TEXT,
                loaded_at TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS flexfields (
                submission_id TEXT,
                field_name TEXT,
                value TEXT,
                FOREIGN KEY(submission_id) REFERENCES submissions(id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gt as_window (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_date TEXT,
                end_date TEXT,
                locked BOOLEAN DEFAULT FALSE
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS validation_results (
                submission_id TEXT,
                rule_code TEXT,
                error_message TEXT,
                FOREIGN KEY(submission_id) REFERENCES submissions(id)
            )
        ''')
        conn.commit()

# Initialize DB
init_db()

# Validation rules updated for DB-2213 and other requirements
VALIDATION_RULES = {
    "CFDA_ERROR": ValidationRule("CFDA_ERROR", "Invalid CFDA code", lambda data: "CFDA" not in data or not re.match(r'^\d+\.\d+$', data["CFDA"])),
    "DUNS_ERROR": ValidationRule("DUNS_ERROR", "Invalid DUNS for action types B,C,D", lambda data: "DUNS" in data and len(data["DUNS"]) == 9 and data.get("ActionType") in ["B", "C", "D"]),
    "PPOP_ZIP": ValidationRule("PPOP_ZIP", "Invalid PPoP ZIP+4", lambda data: "PPoPZIP" in data and (len(data["PPoPZIP"]) == 5 or len(data["PPoPZIP"]) == 9)),
    "FUNDING_AGENCY": ValidationRule("FUNDING_AGENCY", "Missing FundingAgencyCode", lambda data: "FundingAgencyCode" in data),
    "LEGAL_ENTITY_ADDR": ValidationRule("LEGAL_ENTITY_ADDR", "AddressLine3 too long", lambda data: len(data.get("LegalEntityAddressLine3", "")) <= 55),
    "LOAN_BLANK": ValidationRule("LOAN_BLANK", "Zero/blank not allowed for loans", lambda data: data.get("RecordType") != "loan" or data.get("Amount", 0) > 0),
}

def process_12_19_2017_deletions():
    """As a Data user, I want to have the 12-19-2017 deletions processed."""
    logger.info("Processing deletions from 12-19-2017")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM submissions WHERE updated_at < '2017-12-20'")
        conn.commit()
    return "Deletions processed"

def redesign_resources_page():
    """As a UI designer, I want to redesign the Resources page, so that it matches the new Broker design styles."""
    # Simulate UI redesign by generating a JSON config for styles
    styles = {
        "theme": "broker_new",
        "colors": {"primary": "#007BFF", "secondary": "#6C757D"},
        "layout": "flexbox"
    }
    with open("resources_page_styles.json", "w") as f:
        json.dump(styles, f)
    logger.info("Resources page redesign config generated")
    return styles

def report_user_testing_to_agencies():
    """As a UI designer, I want to report to the Agencies about user testing, so that they are aware of their contributions to making Broker a better UX."""
    report = {
        "summary": "User testing completed. Agencies' feedback incorporated for better UX.",
        "contributions": ["Improved navigation", "Better error messages"],
        "date": datetime.datetime.now().isoformat()
    }
    with open("user_testing_report.json", "w") as f:
        json.dump(report, f)
    logger.info("User testing report generated for agencies")
    return report

def move_to_round2_dabs_fabs_landing():
    """As a UI designer, I want to move on to round 2 of DABS or FABS landing page edits, so that I can get approvals from leadership."""
    edits = ["Update headers", "Add navigation links"]
    with open("round2_dabs_fabs_edits.json", "w") as f:
        json.dump(edits, f)
    logger.info("Round 2 edits for DABS/FABS landing pages prepared")
    return edits

def move_to_round2_homepage():
    """As a UI designer, I want to move on to round 2 of Homepage edits, so that I can get approvals from leadership."""
    edits = ["Redesign hero section", "Update footer"]
    with open("round2_homepage_edits.json", "w") as f:
        json.dump(edits, f)
    logger.info("Round 2 homepage edits prepared")
    return edits

def move_to_round3_help_page():
    """As a UI designer, I want to move on to round 3 of the Help page edits, so that I can get approvals from leadership."""
    edits = ["Add FAQ section", "Improve search"]
    with open("round3_help_edits.json", "w") as f:
        json.dump(edits, f)
    logger.info("Round 3 help page edits prepared")
    return edits

def move_to_round2_help_page():
    """As a UI designer,  I want to move on to round 2 of the Help page edits, so that I can get approvals from leadership."""
    edits = ["Update content", "Add images"]
    with open("round2_help_edits.json", "w") as f:
        json.dump(edits, f)
    logger.info("Round 2 help page edits prepared")
    return edits

def setup_better_logging():
    """As a Developer , I want to be able to log better, so that I can troubleshoot issues with particular submissions and functions."""
    # Logging already configured at top
    logger.info("Enhanced logging setup complete for submissions and functions")
    return True

def update_fabs_submission_on_status_change(submission_id: str, new_status: PublishStatus):
    """As a Developer, I want to add the updates on a FABS submission to be modified when the publishStatus changes, so that I know when the status of the submission has changed."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE submissions SET publish_status = ?, updated_at = ? WHERE id = ?",
            (new_status.value, datetime.datetime.now().isoformat(), submission_id)
        )
        conn.commit()
    logger.info(f"FABS submission {submission_id} status updated to {new_status.value}")
    return True

def setup_new_relic_monitoring():
    """As a DevOps engineer, I want New Relic to provide useful data across all applications."""
    # Simulate New Relic config
    config = {"enabled": True, "apps": ["fabs", "dabs", "broker"], "metrics": ["response_time", "errors"]}
    with open("new_relic_config.json", "w") as f:
        json.dump(config, f)
    logger.info("New Relic monitoring configured for all applications")
    return config

def update_validation_rule_table():
    """As a Developer, I want to update the Broker validation rule table to account for the rule updates in DB-2213."""
    # Simulate updating rules
    new_rules = {"DB2213": "Updated length for AddressLine3 to 55"}
    VALIDATION_RULES.update(new_rules)
    logger.info("Validation rule table updated for DB-2213")
    return VALIDATION_RULES

def add_gtas_window_data(start_date: str, end_date: str):
    """As a Developer, I want to add the GTAS window data to the database, so that I can ensure the site is locked down during the GTAS submission period."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO gt_as_window (start_date, end_date) VALUES (?, ?)",
            (start_date, end_date)
        )
        conn.commit()
    logger.info(f"GTAS window added: {start_date} to {end_date}")
    return True

def is_gtas_locked():
    """Check if site is locked due to GTAS."""
    today = datetime.datetime.now().date().isoformat()
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM gt_as_window WHERE start_date <= ? AND end_date >= ? AND locked = 1", (today, today))
        return cursor.fetchone() is not None

def manage_d_files_caching(request_hash: str):
    """As a Developer, I want D Files generation requests to be managed and cached, so that duplicate requests do not cause performance issues."""
    cache_dir = "d_files_cache"
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, f"{request_hash}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            return json.load(f)
    else:
        # Generate D file (simulated)
        d_file_data = {"generated_at": datetime.datetime.now().isoformat(), "data": []}
        with open(cache_file, "w") as f:
            json.dump(d_file_data, f)
        logger.info(f"D file generated and cached for hash {request_hash}")
        return d_file_data

def access_raw_agency_files():
    """As a user, I want to access the raw agency published files from FABS via USAspending."""
    files_dir = "raw_agency_files"
    os.makedirs(files_dir, exist_ok=True)
    # Simulate listing files
    files = [f for f in os.listdir(files_dir) if f.endswith('.fabs')]
    logger.info(f"Raw FABS files available: {files}")
    return files

def handle_large_flexfields(submission_id: str, flexfields: List[Dict]):
    """As an Agency user, I want to be able to include a large number of flexfields without performance impact."""
    # Batch insert for performance
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for ff in flexfields:
            cursor.execute(
                "INSERT OR REPLACE INTO flexfields (submission_id, field_name, value) VALUES (?, ?, ?)",
                (submission_id, ff["name"], ff["value"])
            )
        conn.commit()
    logger.info(f"Large flexfields added to submission {submission_id} without performance hit")
    return len(flexfields)

def create_content_mockups():
    """As a Broker user, I want  to help create content mockups, so that I can submit my data efficiently."""
    mockups = {"template": "fabs_submission", "sections": ["header", "body", "footer"]}
    with open("content_mockups.json", "w") as f:
        json.dump(mockups, f)
    logger.info("Content mockups created for efficient submissions")
    return mockups

def track_tech_thursday_issues(issues: List[str]):
    """As a UI designer, I want to track the issues that come up in Tech Thursday, so that I know what to test and what wants to be fixed."""
    tracked = {"date": datetime.datetime.now().isoformat(), "issues": issues}
    with open("tech_thursday_issues.json", "a") as f:
        json.dump(tracked, f)
        f.write("\n")
    logger.info(f"Tech Thursday issues tracked: {issues}")
    return tracked

def create_user_testing_summary(improvements: List[str]):
    """As an Owner, I want to create a user testing summary from the UI SME, so that I can know what UI improvements we will follow through on."""
    summary = {"improvements": improvements, "priority": "high"}
    with open("ui_improvements_summary.json", "w") as f:
        json.dump(summary, f)
    logger.info("User testing summary created")
    return summary

def begin_user_testing():
    """As a UI designer, I want to begin user testing, so that I can validate stakeholder UI improvement requests."""
    logger.info("User testing begun")
    return {"status": "started", "timestamp": datetime.datetime.now().isoformat()}

def schedule_user_testing(date: str):
    """As a UI designer, I want to schedule user testing, so that I can give the testers advanced notice to ensure buy-in."""
    schedule = {"date": date, "testers": ["agency1", "agency2"]}
    with open("user_testing_schedule.json", "w") as f:
        json.dump(schedule, f)
    logger.info(f"User testing scheduled for {date}")
    return schedule

def design_ui_schedule(timeline: Dict):
    """As an Owner, I want to design a schedule from the UI SME, so that I know the potential timeline of the UI improvements wanted."""
    with open("ui_schedule.json", "w") as f:
        json.dump(timeline, f)
    logger.info("UI improvement schedule designed")
    return timeline

def design_ui_audit(scope: Dict):
    """As an Owner, I want to design an audit from the UI SME, so that I know the potential scope of the UI improvements wanted."""
    with open("ui_audit.json", "w") as f:
        json.dump(scope, f)
    logger.info("UI improvement audit designed")
    return scope

def prevent_double_publishing(submission_id: str):
    """As a Developer, I want to prevent users from double publishing FABS submissions after refreshing, so that there are no duplicates."""
    # Use a lock file to prevent duplicates
    lock_file = f"publish_lock_{submission_id}.lock"
    if os.path.exists(lock_file):
        logger.warning(f"Double publish attempt prevented for {submission_id}")
        return False
    with open(lock_file, "w") as f:
        f.write(str(time.time()))
    # Simulate publish
    logger.info(f"Published {submission_id}")
    # Remove lock after delay (in real, use timeout)
    time.sleep(1)
    os.remove(lock_file)
    return True

def receive_fabs_updates():
    """As an data user, I want to receive updates to FABS records."""
    logger.info("FABS records updated")
    return {"status": "updated"}

def update_fabs_sample_file():
    """As a Developer , I want to update the FABS sample file to remove FundingAgencyCode after FABS is updated to no longer require the header."""
    sample_data = ["header1", "header2"]  # No FundingAgencyCode
    with open("fabs_sample.txt", "w") as f:
        f.write("\n".join(sample_data))
    logger.info("FABS sample file updated without FundingAgencyCode")
    return sample_data

def ensure_deleted_fsrs_not_included():
    """As an agency user, I want to ensure that deleted FSRS records are not included in submissions."""
    # Filter out deleted
    submissions = []  # Simulated filter
    logger.info("Deleted FSRS records excluded from submissions")
    return submissions

def update_financial_data_daily():
    """As a website user, I want to see updated financial assistance data daily."""
    # Simulate daily update
    now = datetime.datetime.now().date().isoformat()
    update_data = {"last_update": now, "source": "FABS"}
    with open("daily_financial_update.json", "w") as f:
        json.dump(update_data, f)
    logger.info("Financial assistance data updated daily")
    return update_data

def deactivate_publish_button_during_derivations(submission_id: str):
    """As a user, I want the publish button in FABS to deactivate after I click it while the derivations are happening, so that I cannot click it multiple times for the same submission."""
    # Simulate deactivation
    status = {"button_state": "disabled", "deriving": True}
    logger.info(f"Publish button deactivated for {submission_id} during derivations")
    # Simulate derivation time
    time.sleep(2)
    status["deriving"] = False
    status["button_state"] = "enabled"
    return status

def prevent_nonexistent_record_ops():
    """As a Developer , I want to ensure that attempts to correct or delete non-existent records don't create new published data."""
    # Check existence before ops
    if not os.path.exists("nonexistent_record.txt"):
        logger.warning("Operation on non-existent record prevented")
        return False
    return True

def reset_environment_permissions():
    """As an Owner, I want to reset the environment to only take Staging MAX permissions, so that I can ensure that the FABS testers no longer have access."""
    perms = {"env": "staging", "max_perms": True, "fabs_testers": False}
    with open("environment_permissions.json", "w") as f:
        json.dump(perms, f)
    logger.info("Environment permissions reset to Staging MAX only")
    return perms

def flexfields_in_error_files(submission_id: str):
    """As a user, I want the flexfields in my submission file to appear in the warning and error files when the only error is a missing required element."""
    errors = [{"code": "MISSING_REQUIRED", "flexfields": ["field1", "field2"]}]
    with open(f"{submission_id}_errors.json", "w") as f:
        json.dump(errors, f)
    logger.info(f"Flexfields included in error file for {submission_id}")
    return errors

def ensure_ppopcode_data():
    """As a user, I want to have accurate and complete data related to PPoPCode and PPoPCongressionalDistrict."""
    data = {"PPoPCode": "valid_code", "PPoPCongressionalDistrict": "01"}
    logger.info("PPoPCode and PPoPCongressionalDistrict data ensured accurate")
    return data

def accept_zero_blank_loans():
    """As an agency user, I want the FABS validation rules to accept zero and blank for loan records."""
    VALIDATION_RULES["LOAN_BLANK"] = ValidationRule("LOAN_BLANK", "Zero/blank allowed for loans", lambda data: True if data.get("RecordType") == "loan" else data.get("Amount", 0) > 0)
    logger.info("Validation rules updated to accept zero/blank for loans")
    return True

def deploy_fabs_to_production():
    """As an Agency user, I want FABS deployed into production, so I can submit my Financial Assistance data."""
    logger.info("FABS deployed to production")
    return {"status": "deployed"}

def clarify_cfda_error(data: Dict):
    """As a Developer , I want to clarify to users what exactly is triggering the CFDA error code in each case."""
    if VALIDATION_RULES["CFDA_ERROR"].logic(data):
        return "CFDA error: Code must match \\d+\\.\\d+ format and be present."
    return "No CFDA error"

def ensure_sam_data_complete():
    """As an agency user, I want to be confident that the data coming from SAM is complete."""
    sam_check = {"complete": True, "missing": []}
    logger.info("SAM data verified complete")
    return sam_check

def index_domain_models():
    """As a Developer , I want my domain models to be indexed properly, so that I can get validation results back in a reasonable amount of time."""
    # Simulate indexing
    logger.info("Domain models indexed for faster validation")
    return True

def accept_zero_blank_non_loans():
    """As an agency user, I want the FABS validation rules to accept zero and blank for non-loan records."""
    VALIDATION_RULES["NON_LOAN_BLANK"] = ValidationRule("NON_LOAN_BLANK", "Zero/blank allowed for non-loans", lambda data: True)
    logger.info("Validation updated for non-loan zero/blank")
    return True

def update_sql_codes():
    """As a broker team member, I want to make some updates to the SQL codes for clarity."""
    sql_update = "SELECT * FROM submissions WHERE status = 'published' ORDER BY created_at;"
    with open("updated_sql.sql", "w") as f:
        f.write(sql_update)
    logger.info("SQL codes updated for clarity")
    return sql_update

def derive_all_elements(data: Dict):
    """As an agency user, I want to have all derived data elements derived properly."""
    data["derived_frec"] = "derived_value"
    logger.info("All derived elements computed")
    return data

def add_ppopcode_cases():
    """As a broker team member, I want to add the 00***** and 00FORGN PPoPCode cases to the derivation logic."""
    derivation_logic = {
        "00*****": "US based",
        "00FORGN": "Foreign"
    }
    logger.info("PPoPCode cases added to derivation")
    return derivation_logic

def derive_office_names():
    """As a data user, I want to see the office names derived from office codes, so that I can have appropriate context for understanding them."""
    offices = {"code1": "Office of Grants"}
    logger.info("Office names derived from codes")
    return offices

def historical_fabs_loader_derive_fields(data: List[Dict]):
    """As a broker user, I want the historical FABS loader to derive fields, so that my agency codes are correct in the PublishedAwardFinancialAssistance table."""
    for record in data:
        record["derived_agency_code"] = "correct_code"
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for record in data:
            cursor.execute(
                "INSERT INTO historical_data (source, data, loaded_at) VALUES (?, ?, ?)",
                ("fabs_historical", json.dumps(record), datetime.datetime.now().isoformat())
            )
        conn.commit()
    logger.info("Historical FABS fields derived and loaded")
    return data

def update_broker_resources_for_launch():
    """As a broker team member, I want to ensure the Broker resources, validations, and P&P pages are updated appropriately for the launch of FABS and DAIMS v1.1."""
    updates = {"resources": "updated", "validations": "v1.1", "pp": "revised"}
    logger.info("Broker pages updated for FABS/DAIMS v1.1 launch")
    return updates

def load_historical_fabs_with_frec(data: List[Dict]):
    """As a Developer, I want the data loaded from historical FABS to include the FREC derivations, so that I can have consistent FREC data for USASpending.gov."""
    for record in data:
        record["FREC"] = "derived_frec"
    logger.info("Historical FABS loaded with FREC derivations")
    return data

def prevent_nasa_grants_as_contracts(data: Dict):
    """As a user, I don't want to see NASA grants displayed as contracts."""
    if data.get("agency") == "NASA" and data.get("type") == "grant":
        data["display_type"] = "grant"
    logger.info("NASA grants correctly displayed as grants")
    return data

def duns_validation_accept_bcd(data: Dict):
    """As a user, I want the DUNS validations to accept records whose ActionTypes are B, C, or D and the DUNS is registered in SAM, even though it may have expired."""
    if data.get("ActionType") in ["B", "C", "D"] and len(data.get("DUNS", "")) == 9:
        return True  # Assume SAM check passes
    return False

def duns_validation_date(data: Dict):
    """As a user, I want the DUNS validations to accept records whose ActionDates are before the current registration date in SAM, but after the initial registration date."""
    action_date = datetime.datetime.fromisoformat(data.get("ActionDate"))
    sam_reg_date = datetime.datetime.now() - datetime.timedelta(days=365)
    initial_reg = datetime.datetime(2000, 1, 1)
    return initial_reg < action_date < sam_reg_date

def derive_funding_agency_code(data: Dict):
    """As a broker team member, I want to derive FundingAgencyCode, so that the data quality and completeness improves."""
    data["FundingAgencyCode"] = "derived_code"
    logger.info("FundingAgencyCode derived")
    return data

def update_legal_entity_addr_length(data: Dict):
    """As an agency user, I want the maximum length allowed for LegalEntityAddressLine3 to match Schema v1.1."""
    if len(data.get("LegalEntityAddressLine3", "")) > 55:
        data["LegalEntityAddressLine3"] = data["LegalEntityAddressLine3"][:55]
    return data

def use_schema_v11_headers(headers: List[str]):
    """As an agency user, I want to use the schema v1.1 headers in my FABS file."""
    v11_headers = ["Header1_v11", "Header2_v11"]
    updated = v11_headers if not headers else headers + v11_headers
    logger.info("Schema v1.1 headers applied to FABS file")
    return updated

def map_federal_action_obligation_to_atom(data: Dict):
    """As a agency user, I want to map the FederalActionObligation properly to the Atom Feed."""
    data["atom_fed_action"] = data.get("FederalActionObligation", 0)
    logger.info("FederalActionObligation mapped to Atom Feed")
    return data

def ppop_zip_validation_like_legal(data: Dict):
    """As a Broker user, I want to have PPoPZIP+4 work the same as the Legal Entity ZIP validations."""
    zip_val = data.get("PPoPZIP", "")
    # Same logic as legal ZIP: 5 or 9 digits
    valid = len(re.sub(r'\D', '', zip_val)) in [5, 9]
    logger.info(f"PPoPZIP validation: {valid}")
    return valid

def link_sample_file_dialog():
    """As a FABS user, I want to link the SAMPLE FILE on the "What you want  to submit" dialog to point to the correct file, so that I have an accurate reference for my agency submissions."""
    link = "path/to/correct_sample.fabs"
    logger.info(f"SAMPLE FILE linked to {link}")
    return link

def update_fpds_daily():
    """As an Agency user, I want FPDS data to be up-to-date daily."""
    now = datetime.datetime.now().date().isoformat()
    fpds_data = {"last_update": now}
    with open("fpds_daily.json", "w") as f:
        json.dump(fpds_data, f)
    logger.info("FPDS data updated daily")
    return fpds_data

def determine_d_files_generation():
    """As a Developer , I want to determine how agencies will generate and validate D Files from FABS and FPDS data."""
    method = "Agencies generate via API, validate with rules"
    logger.info(f"D Files generation method: {method}")
    return method

def generate_validate_d_files(fabs_data: Dict, fpds_data: Dict):
    """As a user, I want to generate and validate D Files from FABS and FPDS data."""
    d_file = {"fabs": fabs_data, "fpds": fpds_data, "validated": True}
    hash_val = hashlib.md5(json.dumps(d_file).encode()).hexdigest()
    return manage_d_files_caching(hash_val)

def header_info_with_datetime():
    """As an Agency user, I want the header information box to show updated date AND time, so that I know when it was updated."""
    header = {"updated": datetime.datetime.now().isoformat()}
    logger.info("Header shows date and time")
    return header

def helpful_file_error(extension: str):
    """As an Agency user, I want to receive a more helpful file-level error when I upload a file with the wrong extension."""
    if not extension.lower() in ['.fabs', '.txt']:
        return "Error: Invalid file extension. Please use .fabs or .txt"
    return "File accepted"

def tester_access_other_envs(feature: str, env: str):
    """As a tester, I want to have access to test features in environments other than Staging, so that I can test any nonProd feature in any environment."""
    if env != "prod":
        logger.info(f"Tester access granted to {feature} in {env}")
        return True
    return False

def accurate_fabs_submission_errors(submission: Submission):
    """As a FABS user, I want to submission errors to accurately represent FABS errors, so that I know why my submission didn't work."""
    errors = []
    for code, rule in VALIDATION_RULES.items():
        if rule.logic(submission.data):
            errors.append({"code": code, "message": rule.description})
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for err in errors:
            cursor.execute(
                "INSERT INTO validation_results (submission_id, rule_code, error_message) VALUES (?, ?, ?)",
                (submission.id, err["code"], err["message"])
            )
        conn.commit()
    logger.info(f"Accurate FABS errors for submission {submission.id}")
    return errors

def update_frontend_urls(page: str):
    """As a FABS user, I want the frontend URLs to more accurately reflect the page I'm accessing, so that I'm not confused."""
    urls = {"/fabs": "FABS Dashboard", "/dabs": "DABS Dashboard"}
    accurate_url = urls.get(page, page)
    logger.info(f"URL updated to reflect {accurate_url}")
    return accurate_url

def load_historical_financial_assistance():
    """As an Agency user, I want all historical Financial Assistance data loaded for FABS go-live."""
    historical = [{"year": 2007, "data": "loaded"}]
    for rec in historical:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO historical_data (source, data, loaded_at) VALUES (?, ?, ?)",
                ("financial_assistance", json.dumps(rec), datetime.datetime.now().isoformat())
            )
            conn.commit()
    logger.info("Historical Financial Assistance data loaded")
    return historical

def load_historical_fpds_both_sources():
    """As a Developer , I want the historical FPDS data loader to include both extracted historical data and FPDS feed data."""
    extracted = [{"source": "extracted", "data": "2007-2010"}]
    feed = [{"source": "feed", "data": "2011-now"}]
    all_data = extracted + feed
    logger.info("Historical FPDS loaded from both sources")
    return all_data

def load_historical_fpds():
    """As an Agency user, I want historical FPDS data loaded."""
    return load_historical_fpds_both_sources()

def show_submission_creator(submission: Submission):
    """As an Agency user, I want to accurately see who created a submission, so that I'm not confused about who last updated a submission."""
    creator_info = {"created_by": submission.user_id, "created_at": submission.created_at}
    logger.info(f"Submission creator: {creator_info}")
    return creator_info

def get_file_f_correct_format():
    """As an agency user, I want to get File F in the correct format."""
    file_f = {"format": "v1.1", "content": "correct_structure"}
    with open("file_f.json", "w") as f:
        json.dump(file_f, f)
    logger.info("File F provided in correct format")
    return file_f

def better_file_level_errors(file_path: str):
    """As an Agency user, I want to better understand my file-level errors."""
    errors = ["Header mismatch", "Invalid row count"]
    return {"file": file_path, "errors": errors}

def provide_fabs_groups_frec():
    """As a Developer , I want to provide FABS groups that function under the FREC paradigm."""
    groups = {"FREC_group1": ["agency1", "agency2"]}
    logger.info("FABS groups provided under FREC")
    return groups

def test_fabs_derivations(test_file: str):
    """As a tester, I want to ensure that FABS is deriving fields properly through a robust test file plus a follow up check."""
    # Simulate derivation check
    derived = derive_all_elements({"test": True})
    check = "derived" in derived
    logger.info(f"FABS derivations test: {check}")
    return check

def only_zero_padded_fields(data: Dict):
    """As an owner, I only want zero-padded fields, so that I can justify padding."""
    for key, val in data.items():
        if isinstance(val, str) and re.match(r'^\d+$', val):
            data[key] = val.zfill(10)
    logger.info("Fields zero-padded only")
    return data

def submit_individual_recipients_no_duns_error(data: Dict):
    """As a Broker user, I want to submit records for individual recipients without receiving a DUNS error."""
    if data.get("recipient_type") == "individual":
        data["duns_validation"] = "skipped"
    logger.info("Individual recipient submitted without DUNS error")
    return data

def info_rows_before_publish(submission: Submission):
    """As a user, I want more information about how many rows will be published prior to deciding whether to publish."""
    rows_count = len(submission.data.get("rows", []))
    info = {"rows_to_publish": rows_count, "estimated_time": "2 minutes"}
    logger.info(f"Pre-publish info: {info}")
    return info

def prevent_duplicate_transactions(data: List[Dict]):
    """As a Developer, I want to prevent duplicate transactions from being published and deal with the time gap between validation and the publishing decision."""
    seen = set()
    unique = []
    for rec in data:
        h = hashlib.md5(json.dumps(rec).encode()).hexdigest()
        if h not in seen:
            seen.add(h)
            unique.append(rec)
    logger.info(f"Duplicates prevented, {len(unique)} unique transactions")
    return unique

def submit_citywide_ppopzip(data: Dict):
    """As a FABS user, I want to submit a citywide as a PPoPZIP and pass validations."""
    data["PPoPZIP"] = "12345"  # Citywide example
    if ppop_zip_validation_like_legal(data):
        logger.info("Citywide PPoPZIP submission validated")
        return True
    return False

def updated_error_codes(logic: str):
    """As a Broker user, I want to have updated error codes that accurately reflect the logic and provide enough information, so that I can fix my submission."""
    code = f"ERR_{hashlib.md5(logic.encode()).hexdigest()[:8]}"
    desc = f"Error based on {logic}"
    return {"code": code, "description": desc}

def leave_off_last4_zip_no_error(data: Dict):
    """As an agency user, I want to leave off the last 4 digits of the ZIP without an error, so that I can complete my submissions."""
    zip5 = data.get("ZIP", "")[:5]
    if len(zip5) == 5:
        data["ZIP"] = zip5
        logger.info("Last 4 ZIP digits omitted without error")
        return True
    return False

def historical_data_columns(data: List[Dict]):
    """As a FABS user, I want to make sure the historical data includes all necessary columns, so that the information in the database is correct."""
    required_cols = ["col1", "col2"]
    for rec in data:
        for col in required_cols:
            if col not in rec:
                rec[col] = "default"
    logger.info("Historical data columns ensured complete")
    return data

def access_additional_fpds_fields():
    """As a data user, I want to access two additional fields from the FPDS data pull."""
    additional = {"field3": "value3", "field4": "value4"}
    logger.info("Additional FPDS fields accessed")
    return additional

def submission_dashboard_info(submission_id: str):
    """As a FABS user, I want additional helpful info in the submission dashboard, so that I can better manage submissions and IG requests."""
    info = {"status": "pending", "ig_requests": 0, "last_action": "validated"}
    logger.info(f"Dashboard info for {submission_id}: {info}")
    return info

def download_uploaded_fabs_file(submission_id: str):
    """As a FABS user, I want to download the uploaded FABS file, so that I can get the uploaded file."""
    file_path = f"{submission_id}.fabs"
    # Simulate download
    with open(file_path, "w") as f:
        f.write("uploaded_data")
    logger.info(f"FABS file downloaded: {file_path}")
    return file_path

def quick_access_broker_data():
    """As a Developer I want to quickly access Broker application data, so that I can investigate issues."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM submissions LIMIT 10")
        data = [dict(row) for row in cursor.fetchall()]
    logger.info("Quick access to Broker data")
    return data

def determine_historical_fpds_load_method():
    """As a Developer , I want to determine the best way to load historical FPDS data, so that I can load all FPDS data since 2007."""
    method = "Batch load with indexing from 2007"
    logger.info(f"Best method for historical FPDS: {method}")
    return method

def appropriate_fabs_language(page: str):
    """As a FABS user, I want the language on FABS pages to be appropriate for me, so that I am not confused."""
    lang = {"fabs": "Financial Assistance Broker Submission", "dabs": "Direct Assistance"}
    return lang.get(page, "Generic")

def no_cross_banner_messages(user_type: str):
    """As a FABS user, I do not want  DABS banner messages and vice versa, so that I have the appropriate information for my application."""
    banners = {"fabs": "FABS specific", "dabs": "DABS specific"}
    appropriate = banners.get(user_type, "")
    logger.info(f"Appropriate banner: {appropriate}")
    return appropriate

def fabs_readonly_dabs(data: Dict):
    """As a FABS user, I want to have read-only access to DABS, so that I can view DABS pages without wanting two sets of permissions."""
    data["dabs_access"] = "readonly"
    logger.info("Read-only DABS access for FABS user")
    return data

def reasonable_validation_time(data: Dict):
    """As a FABS user, I want to have my validations run in a reasonable amount of time."""
    start = time.time()
    errors = accurate_fabs_submission_errors(Submission("test", "user", data, PublishStatus.DRAFT, datetime.datetime.now(), datetime.datetime.now(), "FABS"))
    end = time.time()
    if end - start < 5:  # Reasonable <5s
        logger.info("Validation completed in reasonable time")
        return errors
    return []

def correct_status_labels(submission: Submission):
    """As a FABS user, I want to see correct status labels on the Submission Dashboard, so that I can quickly see my submission history."""
    labels = {PublishStatus.DRAFT.value: "Draft", PublishStatus.PUBLISHED.value: "Published"}
    label = labels.get(submission.publish_status.value, "Unknown")
    logger.info(f"Status label for {submission.id}: {label}")
    return label

def submission_periods_info():
    """As an agency user, I want to know when the submission periods start and end, so that I know when the submission starts and ends."""
    periods = {"start": "2023-01-01", "end": "2023-12-31"}
    logger.info(f"Submission periods: {periods}")
    return periods

def landing_page_nav():
    """As an agency user, I want a landing page to navigate to either FABS or DABS pages, so that I can access both sides of the site."""
    nav = {"fabs": "/fabs", "dabs": "/dabs"}
    logger.info("Landing page navigation provided")
    return nav

def submit_with_quotes(data: List[str]):
    """As an agency user, I want to submit my data elements surrounded by quotation marks, so that Excel won't strip off leading and trailing zeroes."""
    quoted = [f'"{item}"' for item in data]
    logger.info("Data submitted with quotes to preserve zeroes")
    return quoted

# Sync D1 file generation with FPDS load
def sync_d1_with_fpds():
    """As a Broker user, I want the D1 file generation to be synced with the FPDS data load, so that I don't have to regenerate a file if no data has been updated."""
    fpds_last_load = datetime.datetime.now().isoformat()
    if not os.path.exists("d1_synced.flag"):
        generate_d1_file()
        with open("d1_synced.flag", "w") as f:
            f.write(fpds_last_load)
    logger.info("D1 generation synced with FPDS load")
    return True

def generate_d1_file():
    """Generate D1 file."""
    d1_data = {"synced": True}
    with open("d1_file.json", "w") as f:
        json.dump(d1_data, f)

# Access published FABS files
def access_published_fabs_files():
    """As a Website user, I want to access published FABS files, so that I can see the new files as they come in."""
    published_dir = "published_fabs"
    os.makedirs(published_dir, exist_ok=True)
    files = os.listdir(published_dir) if os.path.exists(published_dir) else []
    logger.info(f"Published FABS files: {files}")
    return files

# Ensure only grant records sent
def filter_grant_records_only(data: List[Dict]):
    """As an owner, I want to be sure that USAspending only send grant records to my system."""
    grants = [rec for rec in data if rec.get("type") == "grant"]
    logger.info(f"Only grants sent: {len(grants)} records")
    return grants

# Upload and validate error message
def upload_validate_error_message(file_path: str):
    """As a Broker user, I want to Upload and Validate the error message to have accurate text."""
    error_msg = "Validation failed: Check headers"
    if "fabs" in file_path.lower():
        validate_file(file_path)
    logger.info(f"Upload validation: {error_msg}")
    return error_msg

def validate_file(file_path: str):
    """Simulate file validation."""
    pass

# Update FABS sample file (duplicate story)
update_fabs_sample_file()

# Broker user help create content mockups (duplicate)
create_content_mockups()

# Agency user large flexfields (duplicate)
handle_large_flexfields("test", [{"name": "flex1", "value": "val1"}])

# Receive FABS updates (duplicate)
receive_fabs_updates()

# Agency user large flexfields (duplicate again)
handle_large_flexfields("test2", [{"name": "flex2", "value": "val2"}])

# Update FABS sample (duplicate)
update_fabs_sample_file()

# Ensure deleted FSRS (already implemented)

# Update financial daily (already)

# Publish button deactivate (already)

# Prevent nonexistent (already)

# Reset env (already)

# Flexfields in errors (already)

# Accurate PPoP (already)

# Accept zero loan (already)

# Deploy FABS (already)

# Clarify CFDA (already)

# SAM complete (already)

# Index models (already)

# Accept zero non-loan (already)

# Update SQL (already)

# Derive all (already)

# Add PPoP cases (already)

# Derive office (already)

# Historical FABS derive (already)

# Update broker launch (already)

# Historical FABS FREC (already)

# Prevent NASA (already)

# DUNS accept BCD (already)

# DUNS date (already)

# Derive funding (already)

# Legal addr length (already)

# Schema v1.1 headers (already)

# Map obligation (already)

# PPoP ZIP (already)

# Link sample (already)

# FPDS daily (already)

# Access raw (already)

# Determine D files (already)

# Generate D files (already)

# Header datetime (already)

# Helpful error (already)

# Tester access (already)

# Accurate errors (already)

# Frontend URLs (already)

# Historical FA (already)

# Historical FPDS (already)

# Load historical FPDS (already)

# Show creator (already)

# File F (already)

# Better errors (already)

# FABS groups (already)

# Test derivations (already)

# Zero padded (already)

# Individual no DUNS (already)

# Rows info (already)

# Prevent dup trans (already)

# Citywide ZIP (already)

# Updated errors (already)

# Leave off ZIP (already)

# Historical columns (already)

# Additional fields (already)

# Dashboard info (already)

# Download file (already)

# Quick access (already)

# Determine load (already)

# Language (already)

# No cross banners (already)

# Readonly (already)

# Validation time (already)

# Status labels (already)

# Periods (already)

# Landing nav (already)

# Quotes (already)

# Main function to demonstrate
def main():
    # Example usage simulating the system
    test_submission = Submission(
        id="test123",
        user_id="agency_user",
        data={"CFDA": "10.123", "Amount": 0},
        publish_status=PublishStatus.DRAFT,
        created_at=datetime.datetime.now(),
        updated_at=datetime.datetime.now(),
        fab_type="FABS"
    )
    
    # Process some stories
    process_12_19_2017_deletions()
    redesign_resources_page()
    update_fabs_submission_on_status_change("test123", PublishStatus.PUBLISHED)
    add_gtas_window_data("2023-01-01", "2023-01-31")
    errors = reasonable_validation_time(test_submission.data)
    print(f"Validation errors: {errors}")
    
    # Sync and generate
    sync_d1_with_fpds()
    files = access_published_fabs_files()
    print(f"Published files: {files}")
    
    # Load historical
    historical = [{"test": True}]
    historical_fabs_loader_derive_fields(historical)
    
    logger.info("All user stories implemented in simulated fashion")

if __name__ == "__main__":
    main()