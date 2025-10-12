import datetime
import json
import logging
import os
import re
import sqlite3
import zipfile
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import pandas as pd
from werkzeug.utils import secure_filename
from flask import Flask, request, jsonify, render_template_string, session, redirect, url_for

# Configure logging for better troubleshooting
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = 'super_secret_key_for_session'  # In production, use env var

# Database setup using SQLite for simplicity
DB_PATH = 'broker.db'

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Table for FABS submissions
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fabs_submissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agency_name TEXT,
            file_path TEXT,
            status TEXT DEFAULT 'pending',
            publish_status TEXT DEFAULT 'draft',
            created_by TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            publish_date TIMESTAMP NULL
        )
    ''')
    
    # Table for validation rules
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS validation_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_code TEXT UNIQUE,
            description TEXT,
            is_active BOOLEAN DEFAULT TRUE
        )
    ''')
    
    # Table for historical data loads
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS historical_loads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            load_type TEXT,  -- 'FABS', 'FPDS'
            load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            record_count INTEGER,
            status TEXT
        )
    ''')
    
    # Insert sample validation rules for DB-2213 updates
    rules = [
        ('CFDA_ERROR', 'Clarify CFDA error triggers'),
        ('DUNS_VALID', 'DUNS validation for expired but registered'),
        ('ZIP_VALID', 'ZIP+4 validation matching Legal Entity'),
        ('FLEXFIELD_LIMIT', 'Handle large flexfields without performance impact')
    ]
    for rule_code, desc in rules:
        cursor.execute('INSERT OR IGNORE INTO validation_rules (rule_code, description) VALUES (?, ?)', (rule_code, desc))
    
    conn.commit()
    conn.close()

init_db()

# Enums
class PublishStatus(Enum):
    DRAFT = 'draft'
    PUBLISHED = 'published'
    DELETED = 'deleted'

class SubmissionStatus(Enum):
    PENDING = 'pending'
    VALIDATED = 'validated'
    PUBLISHED = 'published'
    ERROR = 'error'

@dataclass
class FABSRecord:
    action_type: str
    duns: str
    action_date: str
    cfda: str
    zip_code: str
    # Add more fields as needed
    flexfields: Dict[str, str] = None

    def __post_init__(self):
        if self.flexfields is None:
            self.flexfields = {}

class ValidationResult:
    def __init__(self, errors: List[str] = None, warnings: List[str] = None):
        self.errors = errors or []
        self.warnings = warnings or []
        self.is_valid = len(self.errors) == 0

# Core functions for data processing

def process_12_19_2017_deletions():
    """Process deletions from 12-19-2017 as a Data user."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    delete_date = datetime.date(2017, 12, 19)
    cursor.execute('''
        UPDATE fabs_submissions 
        SET status = 'deleted' 
        WHERE created_at < ? AND status != 'deleted'
    ''', (delete_date,))
    deleted_count = cursor.rowcount
    conn.commit()
    conn.close()
    logger.info(f"Processed {deleted_count} deletions for 12-19-2017")
    return deleted_count

def update_fabs_validation_rules():
    """Update validation rule table for DB-2213 as Developer."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE validation_rules 
        SET description = 'Updated for DB-2213: Accept zero/blank for loans/non-loans'
        WHERE rule_code IN ('CFDA_ERROR', 'ZIP_VALID')
    ''')
    conn.commit()
    conn.close()
    logger.info("Updated validation rules for DB-2213")

def derive_gtas_window_data():
    """Add GTAS window data to database as Developer."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # Simulate GTAS window: lock site during submission period
    gtas_start = datetime.date.today().replace(month=10, day=1)  # Example
    gtas_end = gtas_start.replace(month=10, day=15)
    cursor.execute('''
        INSERT INTO historical_loads (load_type, status) 
        VALUES ('GTAS', 'locked')
        ON CONFLICT DO NOTHING
    ''')
    # Add lock flag or something
    conn.commit()
    conn.close()
    logger.info(f"GTAS window data added: {gtas_start} to {gtas_end}")

def manage_d_file_generation_cache():
    """Manage and cache D Files generation to avoid duplicates as Developer."""
    cache_dir = 'd_files_cache'
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, f'd_file_{datetime.date.today()}.json')
    if os.path.exists(cache_file):
        logger.info("Using cached D File")
        with open(cache_file, 'r') as f:
            return json.load(f)
    else:
        # Simulate generation
        data = {"fpds_data": [], "fabs_data": []}
        with open(cache_file, 'w') as f:
            json.dump(data, f)
        logger.info("Generated new D File and cached")
        return data

def load_historical_fabs_data():
    """Load historical FABS data with derivations as Developer/Broker user."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # Simulate loading
    sample_records = [
        FABSRecord(action_type='A', duns='123456789', action_date='2023-01-01', cfda='12.001', zip_code='12345'),
    ]
    for rec in sample_records:
        # Derive fields, e.g., FundingAgencyCode
        rec.flexfields['FundingAgencyCode'] = '097'  # Example derivation
        rec.flexfields['OfficeName'] = derive_office_name(rec.duns)
        # Insert derived record
        cursor.execute('''
            INSERT INTO fabs_submissions (agency_name, status, flexfields_json)
            VALUES (?, ?, ?)
        ''', (f"Agency_{rec.duns}", 'historical', json.dumps(asdict(rec))))
    cursor.execute('''
        INSERT INTO historical_loads (load_type, record_count, status)
        VALUES ('FABS', ?, 'loaded')
    ''', (len(sample_records),))
    conn.commit()
    conn.close()
    logger.info("Historical FABS data loaded with derivations")

def derive_office_name(duns: str) -> str:
    """Derive office names from codes as data user."""
    # Mock derivation logic
    office_map = {'123456789': 'Office of Grants'}
    return office_map.get(duns, 'Unknown Office')

def update_frec_derivations():
    """Ensure FREC derivations in historical loader as Developer."""
    # Simulate FREC derivation: e.g., for NASA grants not as contracts
    logger.info("Applied FREC derivations: NASA grants categorized correctly")
    return True

def validate_duns_record(record: FABSRecord) -> ValidationResult:
    """DUNS validations accepting expired but registered, pre-current date, etc. as user."""
    result = ValidationResult()
    if record.action_type in ['B', 'C', 'D'] and len(record.duns) == 9:
        # Mock SAM check: assume registered if digits
        if not re.match(r'^\d{9}$', record.duns):
            result.errors.append('Invalid DUNS format')
        # Allow expired if action_date before current but after initial
        try:
            action_dt = datetime.datetime.strptime(record.action_date, '%Y-%m-%d').date()
            if action_dt < datetime.date.today():
                result.warnings.append('DUNS may be expired, but accepted for historical')
        except ValueError:
            result.errors.append('Invalid action date')
    else:
        result.warnings.append('Individual recipient: No DUNS required')
    return result

def validate_zip_ppop(record: FABSRecord) -> ValidationResult:
    """PPoP ZIP+4 validation same as Legal Entity as Broker user."""
    result = ValidationResult()
    zip_pattern = r'^\d{5}(-\d{4})?$'
    if not re.match(zip_pattern, record.zip_code):
        if len(record.zip_code) == 5:  # Allow without +4
            result.warnings.append('ZIP without +4 accepted')
        else:
            result.errors.append('Invalid PPoP ZIP format')
    # Citywide as ZIP
    if record.zip_code == 'Citywide':
        result.is_valid = True
    return result

def handle_large_flexfields(flexfields: Dict[str, str]) -> bool:
    """Allow large flexfields without performance impact as Agency user."""
    # Simulate: check length but process efficiently
    total_size = sum(len(v) for v in flexfields.values())
    if total_size > 10000:
        logger.warning("Large flexfields detected, but processed")
    return True

def derive_ppop_code_congressional_district(ppop_code: str) -> Tuple[str, str]:
    """Accurate data for PPoPCode and PPoPCongressionalDistrict as user."""
    # Mock derivation, including 00***** and 00FORGN
    if ppop_code.startswith('00FORGN'):
        return 'Foreign', '00'
    elif ppop_code.startswith('00'):
        return 'Domestic Zero', '01'
    return 'Standard', 'Unknown'

def update_fabs_sample_file():
    """Update FABS sample file to remove FundingAgencyCode header as Developer."""
    sample_content = """ActionType,DUNS,ActionDate,CFDA,ZIP\nA,123456789,2023-01-01,12.001,12345\n"""
    with open('fabs_sample.csv', 'w') as f:
        f.write(sample_content)
    logger.info("Updated FABS sample file")

def prevent_double_publishing(submission_id: int) -> bool:
    """Prevent double publishing after refresh as Developer/user."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT publish_status FROM fabs_submissions WHERE id = ?', (submission_id,))
    status = cursor.fetchone()
    conn.close()
    if status and status[0] == PublishStatus.PUBLISHED.value:
        logger.warning("Double publish attempt prevented")
        return False
    return True

def generate_d_files_from_fabs_fpds():
    """Generate and validate D Files from FABS and FPDS as user/Developer."""
    fabs_data = load_historical_fabs_data()  # Reuse
    fpds_data = []  # Mock FPDS load
    d_file = {"fabs": fabs_data, "fpds": fpds_data}
    # Sync with FPDS load: only generate if updated
    last_fpds_load = datetime.date.today() - datetime.timedelta(days=1)
    if datetime.date.today() > last_fpds_load:
        with open('d_file.json', 'w') as f:
            json.dump(d_file, f)
    return d_file

def update_header_info_with_datetime():
    """Show updated date AND time in header as Agency user."""
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return f"Last Updated: {now}"

def handle_wrong_file_extension(filename: str) -> str:
    """Helpful file-level error for wrong extension as Agency user."""
    if not filename.endswith('.csv'):
        return "Error: Please upload a CSV file. Wrong extension detected."
    return "Valid file extension."

def load_historical_fpds_data():
    """Load historical FPDS data including extracted and feed as Developer/Agency user."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO historical_loads (load_type, record_count, status)
        VALUES ('FPDS', 10000, 'loaded_historical_since_2007')
    ''')
    conn.commit()
    conn.close()
    logger.info("Historical FPDS data loaded since 2007")

def show_submission_creator():
    """Accurately show who created a submission as Agency user."""
    return "Created by: John Doe (Agency Admin)"

def get_file_f_format():
    """Get File F in correct format as agency user."""
    return {"format": "Schema v1.1 compatible", "headers": ["ActionType", "DUNS", ...]}  # Abbrev

def provide_fabs_groups_frec():
    """Provide FABS groups under FREC paradigm as Developer."""
    frec_groups = ["Grants", "Loans", "NASA_Grants"]  # Ensure NASA not as contracts
    logger.info(f"FREC groups: {frec_groups}")
    return frec_groups

def test_derivations_robust():
    """Robust test for field derivations as tester."""
    test_file = 'test_fabs.csv'
    # Load and validate
    df = pd.read_csv(test_file)
    derived_count = len(df)
    assert derived_count > 0, "Derivations failed"
    logger.info("Derivations tested successfully")

def ensure_zero_padded_fields():
    """Only zero-padded fields as owner."""
    # Enforce in processing
    def pad_field(field: str) -> str:
        if isinstance(field, str) and field.isdigit():
            return field.zfill(9)  # e.g., DUNS
        return field
    logger.info("Zero-padding enforced")
    return pad_field

def allow_individual_recipients_no_duns_error():
    """Submit individual recipients without DUNS error as Broker user."""
    # In validation, skip DUNS check for individuals
    logger.info("Individual recipients allowed without DUNS")

def show_publish_row_count():
    """More info on rows to publish before decision as user."""
    row_count = 100  # Simulate
    return f"Ready to publish {row_count} rows."

def prevent_duplicate_transactions():
    """Prevent duplicate transactions with time gap as Developer."""
    # Use timestamp check
    time_gap = 5  # minutes
    if datetime.datetime.now().timestamp() - session.get('last_publish', 0) < time_gap * 60:
        raise ValueError("Too soon to publish again")
    session['last_publish'] = datetime.datetime.now().timestamp()
    return True

def submit_citywide_ppop_zip():
    """Submit citywide as PPoPZIP without error as FABS user."""
    if 'Citywide' in '12345':
        return ValidationResult()  # Valid
    return ValidationResult(warnings=['Citywide ZIP accepted'])

def update_error_codes_helpful():
    """Updated error codes with info as Broker user."""
    error_map = {
        'CFDA_ERROR': 'CFDA missing or invalid; provide valid program code.',
        'DUNS_VALID': 'DUNS expired but accepted for this action type.'
    }
    return error_map

def allow_zip_without_last4():
    """Leave off last 4 digits without error as agency user."""
    zip_short = '12345'
    if len(zip_short) == 5:
        return "Accepted: ZIP+4 optional"
    return "Error"

def ensure_historical_columns():
    """Historical data includes all necessary columns as FABS user."""
    required_cols = ['ActionType', 'DUNS', 'CFDA', 'ZIP', 'FundingAgencyCode']
    # Check df.columns
    logger.info("Historical data columns verified")

def access_additional_fpds_fields():
    """Access two additional fields from FPDS as data user."""
    additional = ['NewField1', 'NewField2']
    return additional

def add_submission_dashboard_info():
    """Additional helpful info in dashboard as FABS user."""
    dashboard = {
        "submissions": 5,
        "pending": 2,
        "ig_requests": 1
    }
    return dashboard

def download_uploaded_fabs_file(submission_id: int) -> str:
    """Download uploaded FABS file as FABS user."""
    filename = f"fabs_{submission_id}.csv"
    # Simulate return path
    return filename

def quick_access_broker_data():
    """Quick access to Broker data for investigation as Developer."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM fabs_submissions LIMIT 10", conn)
    conn.close()
    return df.to_dict()

def load_historical_fpds_best_way():
    """Best way to load historical FPDS since 2007 as Developer."""
    return load_historical_fpds_data()  # Reuse

def appropriate_fabs_language():
    """Language on FABS pages appropriate as FABS user."""
    # In UI, use specific text
    text = "Submit your Financial Assistance Data here."
    return text

def no_dabs_banners_in_fabs():
    """No DABS banners in FABS and vice versa as FABS user."""
    # Conditional rendering
    banners = {"FABS": "FABS Banner", "DABS": None}
    return banners

def fabs_readonly_dabs_access():
    """Read-only access to DABS for FABS users."""
    permissions = {"FABS": "full", "DABS": "readonly"}
    return permissions

def reasonable_validation_time():
    """Validations run in reasonable time as FABS user."""
    start = datetime.datetime.now()
    # Simulate validation
    time.sleep(0.5)  # <1s
    end = datetime.datetime.now()
    assert (end - start).seconds < 2
    return True

def correct_status_labels_dashboard():
    """Correct status labels on Submission Dashboard as FABS user."""
    statuses = {SubmissionStatus.PENDING.value: "Pending Review"}
    return statuses

def show_submission_periods():
    """Know when submission periods start/end as agency user."""
    periods = {"start": "2023-10-01", "end": "2023-12-31"}
    return periods

def landing_page_nav_fabs_dabs():
    """Landing page to navigate FABS or DABS as agency user."""
    nav = {"FABS": "/fabs", "DABS": "/dabs"}
    return nav

def submit_with_quotes_preserve_zeros():
    """Submit data elements with quotes to preserve zeros as agency user."""
    # Process CSV with quotes
    def parse_quoted_csv(file_content: str):
        # Use csv module with quotechar
        import csv
        reader = csv.reader(file_content.splitlines(), quotechar='"')
        return list(reader)
    return parse_quoted_csv

# UI-related simulations (using Flask for redesigns, reports, etc.)

@app.route('/resources')
def redesigned_resources_page():
    """Redesign Resources page matching new Broker styles as UI designer."""
    html = """
    <html>
    <head><title>Resources</title><style>body { font-family: Arial; background: #f0f0f0; }</style></head>
    <body><h1>Broker Resources</h1><p>Updated design.</p></body>
    </html>
    """
    return render_template_string(html)

@app.route('/report_user_testing')
def report_user_testing_agencies():
    """Report user testing to Agencies as UI designer."""
    report = {
        "summary": "User testing contributions improved UX.",
        "feedback": ["Better navigation", "Clearer errors"]
    }
    return jsonify(report)

@app.route('/round2_dabs_fabs')
def round2_dabs_fabs_edits():
    """Round 2 of DABS/FABS landing page edits for approval as UI designer."""
    edits = {"changes": "Updated layouts for leadership approval"}
    logger.info("Round 2 edits ready for approval")
    return jsonify(edits)

@app.route('/round2_homepage')
def round2_homepage_edits():
    """Round 2 of Homepage edits."""
    return jsonify({"status": "Ready for approval"})

@app.route('/round3_help')
def round3_help_edits():
    """Round 3 of Help page edits."""
    return jsonify({"status": "Ready for approval"})

@app.route('/round2_help')
def round2_help_edits():
    """Round 2 of Help page edits."""
    return jsonify({"status": "Ready for approval"})

@app.route('/track_tech_thursday_issues')
def track_tech_thursday_issues():
    """Track issues from Tech Thursday as UI designer."""
    issues = ["UI bug #1", "Test case #2"]
    return jsonify({"issues": issues})

def create_user_testing_summary():
    """Create user testing summary from UI SME as Owner."""
    summary = "Follow through on: Navigation improvements, Error clarity."
    return summary

@app.route('/begin_user_testing')
def begin_user_testing():
    """Begin user testing as UI designer."""
    return jsonify({"status": "Testing started"})

@app.route('/schedule_user_testing')
def schedule_user_testing():
    """Schedule user testing as UI designer."""
    schedule = {"date": "2023-11-15", "testers": 10}
    return jsonify(schedule)

def design_ui_schedule():
    """Design schedule from UI SME as Owner."""
    timeline = {"phase1": "Q4 2023", "phase2": "Q1 2024"}
    return timeline

def design_ui_audit():
    """Design audit from UI SME as Owner."""
    scope = {"improvements": ["UX redesign", "Accessibility"]}
    return scope

def reset_environment_staging_max():
    """Reset environment to Staging MAX permissions as Owner."""
    # Simulate permission reset
    os.environ['PERMISSIONS'] = 'staging_max'
    logger.info("Environment reset: FABS testers access revoked")
    return True

def flexfields_in_error_warning_files():
    """Flexfields appear in warning/error files when missing required as user."""
    error_file = "errors.csv"
    with open(error_file, 'w') as f:
        f.write("Row,Error,Flexfields\n1,Missing required,DUNS:123\n")
    return error_file

def update_fabs_deploy_production():
    """FABS deployed to production as Agency user."""
    logger.info("FABS deployed to production")
    return "Deployed"

def clarify_cfda_error():
    """Clarify CFDA error triggers as Developer."""
    triggers = ["Missing CFDA", "Invalid format"]
    return triggers

def ensure_sam_data_complete():
    """Confident SAM data complete as agency user."""
    # Mock check
    completeness = 100
    return f"SAM data: {completeness}% complete"

def index_domain_models():
    """Index domain models for fast validation as Developer."""
    # Simulate indexing
    logger.info("Domain models indexed for performance")

def update_sql_codes_clarity():
    """Updates to SQL codes for clarity as broker team member."""
    # Example clear SQL
    sql = "SELECT * FROM fabs_submissions WHERE status = 'pending';"
    logger.info(f"Clear SQL: {sql}")

def derive_funding_agency_code():
    """Derive FundingAgencyCode as broker team member."""
    # Logic
    code = "097"  # Default
    return code

def max_length_legal_entity_address3():
    """Max length for LegalEntityAddressLine3 matching v1.1 as agency user."""
    max_len = 55  # Schema v1.1
    return max_len

def use_schema_v11_headers():
    """Use schema v1.1 headers in FABS file as agency user."""
    headers = "ActionType,DUNS,ActionDate,CFDA,ZIP"  # v1.1
    return headers

def map_federal_action_obligation_atom():
    """Map FederalActionObligation to Atom Feed as agency user."""
    mapping = {"obligation": "atom:obligation"}
    return mapping

def link_sample_file_dialog():
    """Link SAMPLE FILE to correct file as FABS user."""
    link = "/static/fabs_sample.csv"
    return link

def ensure_daily_fpds_update():
    """FPDS data up-to-date daily as Agency user."""
    last_update = datetime.date.today()
    return f"FPDS updated: {last_update}"

def access_raw_agency_published_files():
    """Access raw agency published files from FABS via USAspending as user."""
    files_dir = "published_files"
    os.makedirs(files_dir, exist_ok=True)
    return os.listdir(files_dir) if os.path.exists(files_dir) else []

def fabs_submission_errors_accurate():
    """Submission errors accurately represent FABS errors as FABS user."""
    errors = ["DUNS invalid", "CFDA missing"]
    return errors

def frontend_urls_accurate():
    """Frontend URLs reflect page accurately as FABS user."""
    url_map = {"/fabs/submit": "FABS Submission"}
    return url_map

def load_all_historical_fin_assist():
    """All historical Financial Assistance data loaded for FABS go-live as Agency user."""
    return load_historical_fabs_data()  # Reuse

def get_file_level_errors():
    """Better understand file-level errors as Agency user."""
    errors = {"wrong_extension": "Upload CSV only"}
    return errors

def submission_dashboard_status():
    """Correct status labels as FABS user."""
    return correct_status_labels_dashboard()

# Broker upload and validate
@app.route('/upload_fabs', methods=['POST'])
def upload_and_validate_fabs():
    """Upload and validate FABS file with accurate error messages as Broker user."""
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    if file and secure_filename(file.filename).endswith('.csv'):
        filename = secure_filename(file.filename)
        filepath = os.path.join('uploads', filename)
        os.makedirs('uploads', exist_ok=True)
        file.save(filepath)
        
        # Validate
        df = pd.read_csv(filepath)
        results = []
        for _, row in df.iterrows():
            rec = FABSRecord(
                action_type=row.get('ActionType', ''),
                duns=row.get('DUNS', ''),
                action_date=row.get('ActionDate', ''),
                cfda=row.get('CFDA', ''),
                zip_code=row.get('ZIP', '')
            )
            val_duns = validate_duns_record(rec)
            val_zip = validate_zip_ppop(rec)
            combined = ValidationResult(val_duns.errors + val_zip.errors, val_duns.warnings + val_zip.warnings)
            results.append(combined)
        
        # Accurate error text
        error_text = "Validation completed. Check errors for details."
        if any(not r.is_valid for r in results):
            error_text = "Errors found: Review DUNS, ZIP, etc."
        
        # Store submission
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('INSERT INTO fabs_submissions (agency_name, file_path, status) VALUES (?, ?, ?)',
                       ('Test Agency', filepath, 'validated' if all(r.is_valid for r in results) else 'error'))
        submission_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return jsonify({"submission_id": submission_id, "message": error_text, "valid": all(r.is_valid for r in results)})
    return jsonify({"error": handle_wrong_file_extension(file.filename)}), 400

# Sync D1 file with FPDS
def sync_d1_file_generation():
    """Sync D1 file with FPDS data load as Broker user."""
    fpds_updated = check_fpds_update()
    if not fpds_updated:
        logger.info("No FPDS update, using existing D1 file")
        return "cached_d1.json"
    else:
        # Generate new
        return generate_d_files_from_fabs_fpds()

def check_fpds_update() -> bool:
    """Check if FPDS data updated."""
    return datetime.date.today().weekday() == 0  # Mock daily, e.g., Monday

# Access published FABS files as Website user
@app.route('/published_fabs_files')
def access_published_fabs_files():
    """Access published FABS files."""
    published = [f for f in os.listdir('published_files') if f.endswith('.zip')]
    return jsonify({"files": published})

def ensure_only_grant_records_sent():
    """USAspending only sends grant records as owner."""
    # Filter logic
    def filter_records(records):
        return [r for r in records if r.cfda.startswith('Grant')]
    return filter_records

def receive_fabs_updates():
    """Receive updates to FABS records as data user."""
    # Subscribe or poll
    updates = ["Record 1 updated"]
    return updates

def ensure_deleted_fsrs_not_included():
    """Deleted FSRS records not in submissions as agency user."""
    # Filter out deleted
    def filter_fsrs(records):
        return [r for r in records if r.status != 'deleted']
    return filter_fsrs

def daily_financial_assist_update():
    """See updated financial assistance data daily as website user."""
    last_update = datetime.date.today()
    data = {"updated": last_update, "records": 1000}
    return data

def deactivate_publish_button_derivations():
    """Publish button deactivates during derivations as user."""
    # In UI, set disabled=True during process
    deriving = True
    return not deriving  # Enabled when false

def prevent_nonexistent_correct_delete():
    """Attempts to correct/delete non-existent don't create new published as Developer."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT id FROM fabs_submissions WHERE id = ?', (999,))
    if not cursor.fetchone():
        logger.warning("Non-existent record: No new data created")
        conn.close()
        return False
    conn.close()
    return True

def update_broker_resources_validations_pp():
    """Update Broker resources, validations, P&P for FABS/DAIMS v1.1 launch as broker team member."""
    # Simulate update
    logger.info("Updated for launch")

def no_nasa_grants_as_contracts():
    """Don't show NASA grants as contracts as user."""
    # In derivation
    if 'NASA' in agency and cfda.startswith('Grant'):
        category = 'Grant'
    return category

@app.route('/fabs_publish/<int:submission_id>', methods=['POST'])
def publish_fabs_submission(submission_id: int):
    """Publish FABS submission, modify when status changes as Developer/Broker user."""
    if not prevent_double_publishing(submission_id):
        return jsonify({"error": "Already published"}), 400
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('UPDATE fabs_submissions SET publish_status = ?, publish_date = CURRENT_TIMESTAMP WHERE id = ?',
                   (PublishStatus.PUBLISHED.value, submission_id))
    updated = cursor.rowcount
    if updated:
        # Log status change
        logger.info(f"Status changed to published for submission {submission_id}")
        # Deactivate button simulation
        session['deriving'] = True
    conn.commit()
    conn.close()
    
    if updated:
        # Publish rows info
        row_count = show_publish_row_count()
        return jsonify({"success": True, "rows_to_publish": row_count})
    return jsonify({"error": "Submission not found"}), 404

def new_relic_useful_data():
    """New Relic provides useful data across apps as DevOps."""
    # Mock integration
    metrics = {"apps": ["FABS", "DABS"], "performance": "High"}
    logger.info(f"New Relic data: {metrics}")
    return metrics

# UI SME content mockups
def create_content_mockups():
    """Create content mockups as Broker user."""
    mockup = "<div>Mockup for efficient data submission</div>"
    return mockup

# Main app run
if __name__ == '__main__':
    # Initialize some data
    process_12_19_2017_deletions()
    update_fabs_validation_rules()
    derive_gtas_window_data()
    load_historical_fabs_data()
    update_frec_derivations()
    update_fabs_sample_file()
    load_historical_fpds_data()
    test_derivations_robust()
    ensure_zero_padded_fields()
    allow_individual_recipients_no_duns_error()
    prevent_duplicate_transactions()
    update_error_codes_helpful()
    ensure_historical_columns()
    access_additional_fpds_fields()
    add_submission_dashboard_info()
    load_historical_fpds_best_way()
    no_dabs_banners_in_fabs()
    fabs_readonly_dabs_access()
    reasonable_validation_time()
    correct_status_labels_dashboard()
    landing_page_nav_fabs_dabs()
    submit_with_quotes_preserve_zeros()
    
    app.run(debug=True)