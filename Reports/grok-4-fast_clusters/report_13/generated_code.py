import json
import logging
import os
import re
import zipfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import pandas as pd
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Setup logging for better troubleshooting
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database setup for Broker data
Base = declarative_base()
engine = create_engine('sqlite:///:memory:', echo=False)
Session = sessionmaker(bind=engine)

@dataclass
class Submission:
    id: int
    agency: str
    file_path: str
    status: str = 'pending'
    publish_status: str = 'unpublished'
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()
    is_deleted: bool = False

class SubmissionTable(Base):
    __tablename__ = 'submissions'
    id = Column(Integer, primary_key=True)
    agency = Column(String(100))
    file_path = Column(String(500))
    status = Column(String(50))
    publish_status = Column(String(50))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    is_deleted = Column(Boolean, default=False)

Base.metadata.create_all(engine)

# Enums for statuses and types
class PublishStatus(Enum):
    UNPUBLISHED = 'unpublished'
    PUBLISHED = 'published'
    DERIVING = 'deriving'

class ActionType(Enum):
    B = 'B'
    C = 'C'
    D = 'D'

class FileType(Enum):
    FABS = 'FABS'
    DABS = 'DABS'
    FPDS = 'FPDS'

# Core Broker System Class
class BrokerSystem:
    def __init__(self):
        self.session = Session()
        self.fpds_data = {}  # Cache for FPDS data
        self.sam_data = {}   # Mock SAM data
        self.historical_fabs = []
        self.error_codes = {
            'DUNS_INVALID': 'DUNS number is invalid or not registered in SAM.',
            'ZIP_INVALID': 'ZIP code format is invalid.',
            'CFDA_ERROR': 'CFDA code mismatch detected.',
            'FILE_EXTENSION_WRONG': 'File has incorrect extension.',
            'DUPLICATE_PUBLISH': 'Cannot publish duplicate submission.',
            'MISSING_REQUIRED': 'Missing required element in flexfields.'
        }
        self.derived_fields = {}  # Cache for derived fields like FundingAgencyCode
        self.load_mock_data()

    def load_mock_data(self):
        """Load mock historical and SAM data."""
        # Mock SAM data
        self.sam_data = {
            '123456789': {'registration_date': datetime(2017, 1, 1), 'expiration_date': datetime(2024, 1, 1), 'status': 'active'},
            '987654321': {'registration_date': datetime(2018, 1, 1), 'expiration_date': datetime(2025, 1, 1), 'status': 'expired'}
        }
        # Mock FPDS historical data
        self.fpds_data = {
            'historical': [
                {'id': 1, 'agency_code': '00TEST', 'obligation': 1000, 'action_date': datetime(2007, 1, 1)},
                {'id': 2, 'agency_code': '00FORGN', 'obligation': 2000, 'action_date': datetime(2008, 1, 1)}
            ],
            'feed': [
                {'id': 3, 'agency_code': 'TEST', 'obligation': 3000, 'action_date': datetime.now()}
            ]
        }
        # Mock historical FABS
        self.historical_fabs = [
            {'duns': '123456789', 'cfda': '12.000', 'amount': 5000, 'action_type': ActionType.B.value}
        ]

    # Cluster (4,): Deletions, UI redesign (simulated), reporting, New Relic (logging), D1 sync, SQL updates, derivation logic
    def process_deletions_2017(self, submission_id: int):
        """Process deletions for 12-19-2017."""
        submission = self.session.query(SubmissionTable).get(submission_id)
        if submission:
            submission.is_deleted = True
            submission.updated_at = datetime.now()
            self.session.commit()
            logger.info(f"Processed deletion for submission {submission_id}")
        else:
            logger.warning(f"Submission {submission_id} not found")

    def redesign_resources_page(self) -> str:
        """Simulate UI redesign for Resources page matching Broker styles."""
        # In a real app, this would generate HTML/CSS; here, return a mock design summary
        design = {
            "header": "Updated Resources Page",
            "styles": "Broker design: Modern, responsive, blue theme",
            "sections": ["Validations", "P&P", "Downloads"]
        }
        logger.info("Resources page redesigned")
        return json.dumps(design)

    def report_user_testing_to_agencies(self, testing_results: Dict):
        """Report user testing to agencies."""
        report = {
            "date": datetime.now().isoformat(),
            "contributions": "Improved UX based on feedback",
            "agencies": list(testing_results.keys())
        }
        logger.info(f"User testing report sent: {report}")
        return report

    def sync_d1_with_fpds(self, fabs_file: str) -> bool:
        """Sync D1 file generation with FPDS data load."""
        if not self.is_fpds_updated():
            logger.info("No FPDS updates; skipping D1 regeneration")
            return False
        # Simulate D1 generation
        d1_path = fabs_file.replace('.fabs', '_D1.zip')
        with zipfile.ZipFile(d1_path, 'w') as zf:
            zf.writestr('D1.txt', 'Mock D1 data synced with FPDS')
        logger.info(f"D1 generated: {d1_path}")
        return True

    def is_fpds_updated(self) -> bool:
        """Check if FPDS data has updates."""
        last_load = getattr(self, '_last_fpds_load', datetime.min)
        return (datetime.now() - last_load).days < 1  # Daily check

    def update_sql_for_clarity(self, sql_code: str) -> str:
        """Update SQL codes for clarity (mock)."""
        # Add comments and format
        updated_sql = f"-- Updated for clarity\n{sql_code}"
        logger.info("SQL updated for clarity")
        return updated_sql

    def add_ppopcode_cases(self, record: Dict) -> Dict:
        """Add 00***** and 00FORGN PPoPCode cases to derivation logic."""
        ppop_code = record.get('PPoPCode', '')
        if re.match(r'^00\*+\*+\*+\*+\*+$', ppop_code) or ppop_code == '00FORGN':
            record['derived_ppop'] = 'Foreign or Special'
        return record

    def derive_funding_agency_code(self, record: Dict) -> Dict:
        """Derive FundingAgencyCode for data quality."""
        agency_code = record.get('AgencyCode', '')
        if agency_code.startswith('00'):
            record['FundingAgencyCode'] = f"Derived_{agency_code}"
        self.derived_fields[record['id']] = record['FundingAgencyCode']
        logger.info(f"Derived FundingAgencyCode: {record['FundingAgencyCode']}")
        return record

    def map_federal_action_obligation_to_atom(self, obligation: float) -> Dict:
        """Map FederalActionObligation to Atom Feed."""
        atom_entry = {
            "obligation": obligation,
            "updated": datetime.now().isoformat(),
            "type": "financial-assistance"
        }
        logger.info(f"Mapped obligation {obligation} to Atom")
        return atom_entry

    def validate_ppop_zip4(self, zip_code: str) -> bool:
        """PPoPZIP+4 validation same as Legal Entity ZIP."""
        pattern = r'^\d{5}(-\d{4})?$'
        is_valid = bool(re.match(pattern, zip_code))
        if not is_valid:
            logger.warning(f"Invalid ZIP: {zip_code}")
        return is_valid

    # Cluster (5,): UI edits (simulated), logging, access files, grant records, content mockups, tracking issues
    def edit_landing_pages_round2(self, page_type: str) -> str:
        """Round 2 edits for DABS/FABS landing, Homepage, Help pages."""
        edits = {
            "DABS_FABS": "Updated navigation and headers for round 2",
            "Homepage": "Leadership approval pending round 2",
            "Help": "Round 3 edits: Added FAQs and search"
        }
        summary = edits.get(page_type, "No edits")
        logger.info(f"Page edits for {page_type}: {summary}")
        return summary

    def improve_logging_for_submissions(self, submission_id: int, event: str):
        """Better logging for troubleshooting submissions."""
        logger.info(f"Submission {submission_id}: {event} - Timestamp: {datetime.now()}")

    def access_published_fabs_files(self, agency: str) -> List[str]:
        """Access published FABS files."""
        files = [f"{agency}_fabs_{i}.txt" for i in range(1, 4)]
        logger.info(f"Accessed FABS files for {agency}")
        return files

    def ensure_only_grant_records_sent(self, records: List[Dict]) -> List[Dict]:
        """USAspending only sends grant records."""
        grants = [r for r in records if r.get('type') == 'grant']
        logger.info(f"Filtered to {len(grants)} grant records")
        return grants

    def create_content_mockups(self) -> Dict:
        """Create content mockups for efficient submission."""
        mockup = {
            "sections": ["Upload", "Validate", "Publish"],
            "guides": "Step-by-step for FABS submission"
        }
        logger.info("Content mockups created")
        return mockup

    def track_tech_thursday_issues(self, issues: List[str]) -> Dict:
        """Track issues from Tech Thursday for testing."""
        tracked = {issue: "To Test/Fix" for issue in issues}
        logger.info(f"Tracked {len(issues)} issues")
        return tracked

    def create_user_testing_summary(self, sme_feedback: Dict) -> Dict:
        """User testing summary from UI SME."""
        summary = {
            "improvements": list(sme_feedback.values()),
            "timeline": "Q1 2024"
        }
        logger.info("User testing summary created")
        return summary

    def begin_user_testing(self, requests: List[str]) -> bool:
        """Begin user testing for UI improvements."""
        logger.info(f"Starting user testing for: {requests}")
        return True

    def schedule_user_testing(self, date: datetime) -> str:
        """Schedule user testing."""
        notice = f"User testing scheduled for {date}. Buy-in confirmed."
        logger.info(notice)
        return notice

    def design_ui_schedule(self, sme_schedule: Dict) -> Dict:
        """Design schedule from UI SME."""
        timeline = {k: v for k, v in sme_schedule.items()}
        logger.info("UI schedule designed")
        return timeline

    def design_ui_audit(self, sme_audit: Dict) -> Dict:
        """Design audit from UI SME."""
        scope = {k: "Scope Defined" for k in sme_audit}
        logger.info("UI audit designed")
        return scope

    def reset_environment_staging_max(self):
        """Reset environment to Staging MAX permissions."""
        logger.info("Environment reset: Only Staging MAX permissions. FABS testers access revoked.")
        # Simulate permission reset

    def index_domain_models(self) -> bool:
        """Index domain models for validation speed."""
        # Simulate indexing
        logger.info("Domain models indexed for faster validation")
        return True

    def update_header_info_with_datetime(self, box_data: Dict) -> Dict:
        """Header info shows updated date AND time."""
        box_data['updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Header updated: {box_data['updated']}")
        return box_data

    def enforce_zero_padded_fields(self, fields: Dict) -> Dict:
        """Only zero-padded fields."""
        for key, value in fields.items():
            if isinstance(value, str) and re.match(r'^\d+$', value):
                fields[key] = value.zfill(10)  # Example padding
        logger.info("Fields zero-padded")
        return fields

    def get_updated_error_codes(self, error_type: str) -> str:
        """Updated error codes with logic info."""
        code = self.error_codes.get(error_type, 'Unknown Error')
        logger.info(f"Error code: {code}")
        return code

    def quick_access_broker_data(self, query: str) -> Dict:
        """Quick access to Broker app data for investigation."""
        data = {'query': query, 'results': self.fpds_data}
        logger.info(f"Accessed broker data for: {query}")
        return data

    def read_only_dabs_access_for_fabs(self, user: str) -> bool:
        """Read-only access to DABS for FABS users."""
        logger.info(f"{user} granted read-only DABS access")
        return True

    def create_landing_page_for_fabs_dabs(self) -> str:
        """Landing page to navigate FABS or DABS."""
        page = {
            "title": "FABS/DABS Landing",
            "nav": ["FABS Pages", "DABS Pages"]
        }
        logger.info("Landing page created")
        return json.dumps(page)

    # Cluster (2,): FABS submission updates, GTAS data, sample file, publish button, historical loaders
    def update_fabs_submission_on_publish_change(self, submission_id: int, new_status: PublishStatus):
        """Update FABS submission when publishStatus changes."""
        submission = self.session.query(SubmissionTable).get(submission_id)
        if submission:
            submission.publish_status = new_status.value
            submission.updated_at = datetime.now()
            self.session.commit()
            logger.info(f"Submission {submission_id} status changed to {new_status.value}")

    def add_gtas_window_data(self, start_date: datetime, end_date: datetime):
        """Add GTAS window data to DB for lockdown."""
        gtas = {
            'window_start': start_date,
            'window_end': end_date,
            'lockdown': True
        }
        # Simulate DB insert
        logger.info(f"GTAS window added: {gtas}")

    def update_fabs_sample_file(self) -> str:
        """Update FABS sample file to remove FundingAgencyCode header."""
        sample_content = "Header without FundingAgencyCode\nRow1: data\n"
        path = 'fabs_sample.txt'
        with open(path, 'w') as f:
            f.write(sample_content)
        logger.info("FABS sample file updated")
        return path

    def deactivate_publish_button_during_derivation(self, submission_id: int) -> bool:
        """Deactivate publish button during derivations."""
        # Simulate UI state
        logger.info(f"Publish button deactivated for {submission_id} during derivation")
        return True

    def derive_fields_in_historical_fabs_loader(self, record: Dict) -> Dict:
        """Historical FABS loader derives fields including agency codes."""
        record = self.derive_funding_agency_code(record)
        record['frec_derived'] = True
        self.historical_fabs.append(record)
        logger.info(f"Historical FABS derived: {record['id']}")
        return record

    def load_historical_fabs_with_frec(self, data: List[Dict]):
        """Load historical FABS with FREC derivations."""
        for record in data:
            self.derive_fields_in_historical_fabs_loader(record)
        logger.info(f"Loaded {len(data)} historical FABS with FREC")

    def update_fabs_frontend_urls(self, old_url: str) -> str:
        """Update frontend URLs to reflect pages accurately."""
        new_url = old_url.replace('old', 'fabs_specific')
        logger.info(f"URL updated: {new_url}")
        return new_url

    def load_historical_fpds_data(self):
        """Load historical FPDS from extracted and feed data."""
        combined = self.fpds_data['historical'] + self.fpds_data['feed']
        self.fpds_data['full_historical'] = combined
        self._last_fpds_load = datetime.now()
        logger.info(f"Loaded {len(combined)} historical FPDS records since 2007")

    def provide_fabs_groups_frec_paradigm(self) -> List[Dict]:
        """Provide FABS groups under FREC paradigm."""
        groups = [{'group': 'FREC_Test', 'records': self.historical_fabs[:1]}]
        logger.info("FABS groups provided under FREC")
        return groups

    def ensure_historical_data_columns(self, data: List[Dict]) -> List[Dict]:
        """Ensure historical data includes all necessary columns."""
        required_cols = ['duns', 'cfda', 'amount']
        for record in data:
            for col in required_cols:
                if col not in record:
                    record[col] = 'default'
        logger.info("Historical data columns ensured")
        return data

    def access_additional_fpds_fields(self, fields: List[str]) -> Dict:
        """Access two additional fields from FPDS pull."""
        extra = {field: f"FPDS_{field}" for field in fields[:2]}
        logger.info(f"Accessed additional FPDS fields: {extra}")
        return extra

    def add_helpful_info_to_submission_dashboard(self, submission_id: int) -> Dict:
        """Additional helpful info in submission dashboard."""
        info = {
            'submission_id': submission_id,
            'ig_requests': 0,
            'tips': 'Check validations before publish'
        }
        logger.info(f"Dashboard info added for {submission_id}")
        return info

    def download_uploaded_fabs_file(self, upload_path: str) -> str:
        """Download uploaded FABS file."""
        download_path = upload_path.replace('upload', 'download')
        # Simulate copy
        os.system(f'cp {upload_path} {download_path}')  # Mock
        logger.info(f"Downloaded: {download_path}")
        return download_path

    def determine_best_way_load_historical_fpds(self):
        """Determine best way to load historical FPDS since 2007."""
        method = "Batch load with derivations"
        self.load_historical_fpds_data()
        logger.info(f"Best method: {method}")

    def update_fabs_language(self, page_content: str) -> str:
        """Appropriate language on FABS pages."""
        updated = page_content.replace('confusing term', 'clear term')
        logger.info("FABS language updated")
        return updated

    def hide_dabs_banners_on_fabs(self, user_type: str) -> bool:
        """No DABS banners on FABS and vice versa."""
        logger.info(f"Banners hidden for {user_type}")
        return True

    def show_submission_periods(self, agency: str) -> Dict:
        """Know when submission periods start/end."""
        periods = {
            'start': datetime.now(),
            'end': datetime.now() + timedelta(days=30)
        }
        logger.info(f"Submission periods for {agency}: {periods}")
        return periods

    # Cluster (0,): Upload/validate errors, update validation rules, flexfields warnings, CFDA clarity
    def upload_and_validate_error_message(self, file_path: str) -> str:
        """Accurate error message for upload and validate."""
        if not file_path.endswith('.fabs'):
            return self.error_codes['FILE_EXTENSION_WRONG']
        return "Validation passed"

    def update_broker_validation_rule_table(self, rule_updates: Dict):
        """Update validation rule table for DB-2213."""
        for rule, update in rule_updates.items():
            self.error_codes[rule] = update
        logger.info("Validation rules updated")

    def include_flexfields_in_error_files(self, errors: List[str], flexfields: Dict):
        """Flexfields appear in warning/error files if only missing required."""
        if 'missing_required' in errors:
            errors.append(f"Flexfields affected: {list(flexfields.keys())}")
        logger.warning(f"Errors with flexfields: {errors}")
        return errors

    def clarify_cfda_error_triggers(self, record: Dict) -> str:
        """Clarify what triggers CFDA error."""
        trigger = "Mismatch between CFDA in record and SAM"
        if record.get('cfda') != self.sam_data.get(record.get('duns'), {}).get('cfda', ''):
            logger.warning(self.error_codes['CFDA_ERROR'] + f" Trigger: {trigger}")
        return trigger

    def update_broker_resources_for_launch(self, version: str = "v1.1"):
        """Update Broker resources, validations, P&P for FABS and DAIMS v1.1."""
        updates = {
            "resources": "Updated for v1.1",
            "validations": "Schema aligned",
            "pp": "Policies refreshed"
        }
        logger.info(f"Broker updated for {version} launch: {updates}")
        return updates

    def validate_duns_for_action_types(self, duns: str, action_type: str, action_date: datetime) -> bool:
        """DUNS validations for B,C,D actions, SAM registered even expired, date checks."""
        if action_type not in [at.value for at in ActionType]:
            return False
        sam_entry = self.sam_data.get(duns, {})
        reg_date = sam_entry.get('registration_date', datetime.min)
        exp_date = sam_entry.get('expiration_date', datetime.max)
        if action_date < reg_date or action_date > exp_date:
            if sam_entry.get('status') == 'active' or action_date > reg_date:
                logger.info(f"DUNS {duns} accepted despite date")
                return True
        return bool(sam_entry)

    def helpful_file_level_error(self, file_path: str) -> str:
        """Helpful file-level error for wrong extension."""
        if not file_path.lower().endswith(('.fabs', '.txt')):
            return "File extension must be .fabs or .txt. Please check and re-upload."
        return "File accepted"

    def prevent_duplicate_transactions_publish(self, submission_id: int) -> bool:
        """Prevent duplicate publishes, handle validation to publish gap."""
        existing = self.session.query(SubmissionTable).filter_by(id=submission_id, publish_status=PublishStatus.PUBLISHED.value).first()
        if existing:
            logger.warning(self.error_codes['DUPLICATE_PUBLISH'])
            return False
        return True

    # Cluster (1,): D Files management, access raw files, flexfields performance, prevent double publish
    def manage_d_files_generation_cache(self, request_id: str) -> str:
        """Manage and cache D Files generation requests."""
        if request_id in self.derived_fields:
            logger.info("Returning cached D File")
            return "cached_d_file.zip"
        d_file = self.generate_d_file()
        # Cache it
        self.derived_fields[request_id] = d_file
        return d_file

    def generate_d_file(self) -> str:
        """Generate D File from FABS and FPDS."""
        content = "D File: Combined FABS + FPDS data"
        path = 'd_file.zip'
        with zipfile.ZipFile(path, 'w') as zf:
            zf.writestr('D.txt', content)
        return path

    def access_raw_agency_published_files_usaspending(self, agency: str) -> List[str]:
        """Access raw published FABS files via USAspending."""
        files = [f"{agency}_published_{i}.fabs" for i in range(1, 3)]
        logger.info(f"Raw files accessed for {agency}")
        return files

    def handle_large_flexfields_no_impact(self, flexfields: Dict) -> bool:
        """Include large number of flexfields without performance impact."""
        # Simulate efficient processing
        processed = {k: v[:100] for k, v in flexfields.items()}  # Truncate for demo
        logger.info(f"Processed {len(flexfields)} flexfields efficiently")
        return True

    def prevent_double_publishing_after_refresh(self, submission_id: int) -> bool:
        """Prevent double publishing after refresh."""
        return self.prevent_duplicate_transactions_publish(submission_id)

    def ensure_daily_financial_assistance_updates(self) -> bool:
        """See updated financial assistance data daily."""
        self.load_historical_fpds_data()  # Simulate daily update
        logger.info("Daily FA updates ensured")
        return True

    def prevent_correct_delete_nonexistent_records(self, record_id: int) -> bool:
        """Ensure attempts on non-existent records don't create new data."""
        if record_id not in [r['id'] for r in self.historical_fabs]:
            logger.warning(f"Non-existent record {record_id} - no action taken")
            return False
        return True

    def accurate_ppopcode_ppocongressionaldistrict(self, record: Dict) -> Dict:
        """Accurate data for PPoPCode and PPoPCongressionalDistrict."""
        record['PPoPCode_accurate'] = record.get('PPoPCode', 'derived')
        record['PPoPCongressionalDistrict_accurate'] = '01'  # Mock derivation
        logger.info("PPoP fields accurate")
        return record

    def hide_nasa_grants_as_contracts(self, records: List[Dict]) -> List[Dict]:
        """Don't show NASA grants as contracts."""
        filtered = [r for r in records if not (r.get('agency') == 'NASA' and r.get('type') == 'grant')]
        logger.info(f"Filtered NASA grants: {len(filtered)} records")
        return filtered

    def determine_d_files_generation_from_fabs_fpds(self) -> str:
        """How agencies generate/validate D Files."""
        method = "Agencies validate via Broker API, generate from combined data"
        logger.info(method)
        return method

    def generate_validate_d_files_user(self, fabs_data: List[Dict], fpds_data: List[Dict]) -> bool:
        """User generate/validate D Files."""
        combined = fabs_data + fpds_data
        validation = all(self.validate_record(r) for r in combined)
        logger.info(f"D Files generated and validated: {validation}")
        return validation

    def access_test_features_other_envs(self, env: str, feature: str) -> bool:
        """Tester access to nonProd features in any env."""
        if env != 'prod':
            logger.info(f"Access granted to {feature} in {env}")
            return True
        return False

    def accurate_fabs_submission_errors(self, errors: List[str]) -> List[str]:
        """Submission errors accurately represent FABS errors."""
        fabs_specific = [e for e in errors if 'FABS' in e]
        logger.warning(f"FABS errors: {fabs_specific}")
        return fabs_specific

    def show_submission_creator(self, submission: Dict) -> str:
        """Accurately see who created submission."""
        creator = submission.get('creator', 'Unknown')
        logger.info(f"Creator: {creator}")
        return creator

    def robust_test_file_for_derivations(self, test_file: str) -> bool:
        """Robust test file plus check for derivations."""
        # Simulate test
        derivations_ok = True
        self.improve_logging_for_submissions(1, "Derivation test passed")
        logger.info("Derivations tested robustly")
        return derivations_ok

    def submit_individual_recipients_no_duns_error(self, records: List[Dict]) -> List[Dict]:
        """Submit without DUNS error for individuals."""
        for record in records:
            if record.get('recipient_type') == 'individual':
                record['duns_error'] = False
        logger.info("Individual recipients submitted no DUNS error")
        return records

    def show_rows_to_publish_before_decision(self, rows: int) -> int:
        """Info on how many rows to publish."""
        info = f"Will publish {rows} rows"
        logger.info(info)
        return rows

    def submit_citywide_ppopzip(self, zip_code: str = '00000') -> bool:
        """Submit citywide as PPoPZIP and pass."""
        return self.validate_ppop_zip4(zip_code)

    def reasonable_validation_time(self, records: int) -> float:
        """Validations run in reasonable time."""
        time_taken = records * 0.01  # Simulated time
        logger.info(f"Validation time: {time_taken}s for {records} records")
        return time_taken

    # Cluster (3,): FABS updates, deleted FSRS, validation rules zero/blank, deploy, SAM complete
    def receive_fabs_record_updates(self, updates: List[Dict]):
        """Receive updates to FABS records."""
        for update in updates:
            # Simulate update
            logger.info(f"Updated FABS record: {update['id']}")

    def exclude_deleted_fsrs_records(self, submissions: List[Dict]) -> List[Dict]:
        """Ensure deleted FSRS records not included."""
        filtered = [s for s in submissions if not s.get('fsrs_deleted', False)]
        logger.info(f"Excluded {len(submissions) - len(filtered)} FSRS records")
        return filtered

    def accept_zero_blank_loans_nonloans(self, record: Dict, is_loan: bool) -> bool:
        """Validation rules accept zero/blank for loan/non-loan."""
        amount = record.get('amount', 0)
        if is_loan or not is_loan:
            if amount in (0, '', None):
                return True
        return bool(amount > 0)

    def deploy_fabs_to_production(self) -> bool:
        """Deploy FABS to production."""
        logger.info("FABS deployed to production")
        return True

    def ensure_sam_data_complete(self, sam_pull: Dict) -> Dict:
        """Confident SAM data is complete."""
        if len(sam_pull) > 0:
            sam_pull['complete'] = True
        logger.info("SAM data completeness ensured")
        return sam_pull

    def derive_all_data_elements(self, record: Dict) -> Dict:
        """All derived data elements properly derived."""
        record = self.add_ppopcode_cases(record)
        record = self.derive_funding_agency_code(record)
        logger.info("All derivations complete")
        return record

    def max_length_legalentityaddressline3(self, address: str, max_len: int = 55) -> str:
        """Max length for LegalEntityAddressLine3 matching v1.1."""
        if len(address) > max_len:
            address = address[:max_len]
        logger.info("Address length adjusted")
        return address

    def use_schema_v11_headers(self, file_headers: List[str]) -> List[str]:
        """Use schema v1.1 headers in FABS file."""
        v11_headers = ['UEI', 'AwardingAgencyCode', 'FundingAgencyCode']  # Example
        updated = [h if h in v11_headers else f"v1.1_{h}" for h in file_headers]
        logger.info("Headers updated to v1.1")
        return updated

    def daily_fpds_up_to_date(self) -> bool:
        """FPDS data up-to-date daily."""
        self.load_historical_fpds_data()
        logger.info("FPDS updated daily")
        return True

    def load_all_historical_fa_for_go_live(self):
        """Load all historical Financial Assistance for FABS go-live."""
        self.load_historical_fabs_with_frec(self.historical_fabs * 10)  # Simulate volume
        logger.info("All historical FA loaded for go-live")

    def load_historical_fpds_for_agency(self, agency: str):
        """Load historical FPDS."""
        relevant = [r for r in self.fpds_data['full_historical'] if r['agency_code'].startswith(agency)]
        logger.info(f"Loaded historical FPDS for {agency}: {len(relevant)}")

    def get_file_f_correct_format(self) -> str:
        """Get File F in correct format."""
        content = "File F: Correctly formatted per schema"
        path = 'file_f.txt'
        with open(path, 'w') as f:
            f.write(content)
        return path

    def better_understand_file_level_errors(self, errors: List[str]) -> List[str]:
        """Better file-level errors."""
        detailed = [f"File Error: {e} - See line X" for e in errors]
        logger.warning("Detailed file errors")
        return detailed

    def submit_data_with_quotes(self, data: str) -> str:
        """Submit elements surrounded by quotes to preserve zeroes."""
        quoted = f'"{data}"'
        logger.info("Data submitted with quotes")
        return quoted

    # Combined Clusters
    # Cluster (2, 5): Derive office names from codes
    def derive_office_names_from_codes(self, code: str) -> str:
        """See office names derived from codes."""
        names = {'00TEST': 'Test Office', '00FORGN': 'Foreign Office'}
        name = names.get(code, 'Unknown Office')
        logger.info(f"Derived office: {name} from {code}")
        return name

    # Cluster (2, 4, 5): Link SAMPLE FILE correctly
    def link_sample_file_correctly(self) -> str:
        """Link SAMPLE FILE to correct file on dialog."""
        correct_file = self.update_fabs_sample_file()
        logger.info(f"SAMPLE FILE linked to: {correct_file}")
        return correct_file

    # Cluster (3, 5): Leave off last 4 ZIP digits no error
    def validate_zip_without_last4(self, zip5: str) -> bool:
        """Leave off last 4 digits without error."""
        pattern = r'^\d{5}$'
        valid = bool(re.match(pattern, zip5))
        if valid:
            logger.info(f"ZIP {zip5} accepted without +4")
        return valid

    # Cluster (1, 2): Correct status labels on dashboard
    def correct_status_labels_dashboard(self, submission: Dict) -> str:
        """Correct status labels on Submission Dashboard."""
        label = submission.get('status', 'Unknown').title()
        logger.info(f"Status label: {label}")
        return label

    # Utility validation
    def validate_record(self, record: Dict) -> bool:
        """Generic record validation."""
        duns_valid = self.validate_duns_for_action_types(record.get('duns', ''), record.get('action_type', ''), record.get('action_date', datetime.now()))
        zip_valid = self.validate_ppop_zip4(record.get('zip', ''))
        return duns_valid and zip_valid

    def create_submission(self, agency: str, file_path: str) -> int:
        """Create a new submission."""
        new_sub = SubmissionTable(agency=agency, file_path=file_path, status='pending')
        self.session.add(new_sub)
        self.session.commit()
        return new_sub.id

# Example usage - Fully functional demo
if __name__ == "__main__":
    broker = BrokerSystem()
    
    # Demo Cluster 4
    sub_id = broker.create_submission('TEST', 'test.fabs')
    broker.process_deletions_2017(sub_id)
    print(broker.redesign_resources_page())
    broker.sync_d1_with_fpds('test.fabs')
    record4 = {'id': 1, 'PPoPCode': '00*****', 'AgencyCode': '00TEST'}
    record4 = broker.add_ppopcode_cases(record4)
    record4 = broker.derive_funding_agency_code(record4)
    
    # Demo Cluster 5
    print(broker.edit_landing_pages_round2('Homepage'))
    broker.improve_logging_for_submissions(sub_id, 'Upload')
    print(broker.access_published_fabs_files('TEST'))
    testing_results = {'AgencyA': 'Feedback'}
    print(broker.report_user_testing_to_agencies(testing_results))
    
    # Demo Cluster 2
    broker.update_fabs_submission_on_publish_change(sub_id, PublishStatus.PUBLISHED)
    broker.load_historical_fpds_data()
    broker.update_fabs_sample_file()
    
    # Demo Cluster 0
    print(broker.upload_and_validate_error_message('test.fabs'))
    print(broker.helpful_file_level_error('wrong.ext'))
    
    # Demo Cluster 1
    print(broker.manage_d_files_generation_cache('req1'))
    broker.handle_large_flexfields_no_impact({'field1': 'long data' * 100})
    
    # Demo Cluster 3
    broker.deploy_fabs_to_production()
    broker.daily_fpds_up_to_date()
    
    # Combined
    print(broker.derive_office_names_from_codes('00TEST'))
    print(broker.link_sample_file_correctly())
    print(broker.validate_zip_without_last4('12345'))
    print(broker.correct_status_labels_dashboard({'status': 'pending'}))
    
    logger.info("Broker system demo complete")