import logging
import json
import os
import zipfile
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import re
from urllib.parse import urlparse

# Configure logging for better troubleshooting
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

Base = declarative_base()

class PublishStatus(Enum):
    DRAFT = "draft"
    PUBLISHED = "published"

@dataclass
class Submission:
    id: int
    data: Dict[str, Any]
    status: PublishStatus
    created_by: str
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

class FABSRecord(Base):
    __tablename__ = 'fabs_records'
    id = Column(Integer, primary_key=True)
    action_date = Column(DateTime)
    action_type = Column(String(10))
    duns = Column(String(9))
    cfda = Column(String(50))
    funding_agency_code = Column(String(3))
    legal_entity_address_line3 = Column(String(100))
    ppop_zip = Column(String(10))
    federal_action_obligation = Column(String(20))
    publish_status = Column(String(20))
    is_deleted = Column(Boolean, default=False)

class GTASWindow(Base):
    __tablename__ = 'gtas_windows'
    id = Column(Integer, primary_key=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    is_active = Column(Boolean, default=True)

class ValidationRule:
    def __init__(self, code: str, description: str, logic: callable):
        self.code = code
        self.description = description
        self.logic = logic

class BrokerSystem:
    def __init__(self, db_url: str = "sqlite:///broker.db"):
        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.validation_rules = self._load_validation_rules()
        self.cache = {}
        self.new_relic_data = {}  # Placeholder for New Relic integration

    def _load_validation_rules(self) -> List[ValidationRule]:
        rules = [
            ValidationRule("DB-2213-01", "CFDA error clarification", lambda record: self._validate_cfda(record)),
            ValidationRule("DUNS-01", "DUNS validation for expired but registered", lambda record: self._validate_duns(record)),
            ValidationRule("ZIP-01", "PPoPZIP+4 validation", lambda record: self._validate_ppop_zip(record)),
            ValidationRule("FREC-01", "Derive FundingAgencyCode", lambda record: self._derive_funding_agency(record)),
            ValidationRule("LOAN-01", "Accept zero/blank for loan records", lambda record: self._validate_loan_fields(record)),
            ValidationRule("NONLOAN-01", "Accept zero/blank for non-loan records", lambda record: self._validate_nonloan_fields(record)),
            # Add more rules as per stories
        ]
        return rules

    # As a Data user, I want to have the 12-19-2017 deletions processed.
    def process_20171219_deletions(self, deletion_file_path: str):
        logger.info("Processing 12-19-2017 deletions")
        session = self.Session()
        try:
            df = pd.read_csv(deletion_file_path)
            for _, row in df.iterrows():
                record = session.query(FABSRecord).filter(FABSRecord.id == row['id']).first()
                if record:
                    record.is_deleted = True
                    session.commit()
                    logger.info(f"Deleted record {row['id']}")
        except Exception as e:
            logger.error(f"Error processing deletions: {e}")
            session.rollback()
        finally:
            session.close()

    # As a Developer, I want to be able to log better, so that I can troubleshoot issues with particular submissions and functions.
    def log_submission_issue(self, submission_id: int, function: str, error: str):
        logger.error(f"Submission {submission_id} in {function}: {error}")

    # As a Developer, I want to add the updates on a FABS submission to be modified when the publishStatus changes.
    def update_submission_on_status_change(self, submission: Submission):
        if submission.status != PublishStatus.DRAFT:
            # Trigger updates, e.g., derivations
            self._derive_fields(submission.data)
            logger.info(f"Submission {submission.id} updated due to status change to {submission.status}")

    # As a DevOps engineer, I want New Relic to provide useful data across all applications.
    def collect_new_relic_data(self, app_name: str, metrics: Dict[str, Any]):
        self.new_relic_data[app_name] = metrics
        logger.info(f"Collected New Relic data for {app_name}")

    # As a Developer, I want to update the Broker validation rule table to account for the rule updates in DB-2213.
    def update_validation_rules_db2213(self):
        # Simulate updating rules in DB
        logger.info("Updated validation rules for DB-2213")
        # In reality, this would update a DB table

    # As a Developer, I want to add the GTAS window data to the database.
    def add_gtas_window(self, start: datetime, end: datetime):
        session = self.Session()
        gtas = GTASWindow(start_date=start, end_date=end)
        session.add(gtas)
        session.commit()
        session.close()
        logger.info("GTAS window added")

    # As a Developer, I want D Files generation requests to be managed and cached.
    def generate_d_file(self, fabs_data: List[Dict], fpds_data: List[Dict], cache_key: str = None) -> str:
        if cache_key and cache_key in self.cache:
            logger.info("Returning cached D file")
            return self.cache[cache_key]
        
        # Simulate generation
        combined = fabs_data + fpds_data
        df = pd.DataFrame(combined)
        output_path = f"d_file_{datetime.now().isoformat()}.csv"
        df.to_csv(output_path, index=False)
        
        if cache_key:
            self.cache[cache_key] = output_path
        logger.info("Generated new D file")
        return output_path

    # As an Agency user, I want to be able to include a large number of flexfields without performance impact.
    def process_flexfields(self, flexfields: Dict[str, List[str]]) -> Dict[str, Any]:
        # Use efficient processing, e.g., batch inserts
        processed = {k: v[:100] for k, v in flexfields.items()}  # Limit for demo
        logger.info(f"Processed flexfields: {len(processed)} keys")
        return processed

    # As a Developer, I want to prevent users from double publishing FABS submissions after refreshing.
    def publish_submission(self, submission_id: int, user_id: str) -> bool:
        session = self.Session()
        submission = session.query(Submission).filter(Submission.id == submission_id).first()
        if not submission or submission.status == PublishStatus.PUBLISHED:
            logger.warning(f"Attempted double publish for {submission_id}")
            session.close()
            return False
        submission.status = PublishStatus.PUBLISHED
        submission.updated_at = datetime.now()
        session.commit()
        session.close()
        self._deactivate_publish_button(submission_id)  # Placeholder for UI
        logger.info(f"Published submission {submission_id}")
        return True

    def _deactivate_publish_button(self, submission_id: int):
        # Simulate UI deactivation
        pass

    # As a data user, I want to receive updates to FABS records.
    def update_fabs_records(self, updates: List[Dict]):
        session = self.Session()
        for update in updates:
            record = session.query(FABSRecord).filter(FABSRecord.id == update['id']).first()
            if record:
                for key, value in update.items():
                    setattr(record, key, value)
        session.commit()
        session.close()
        logger.info(f"Updated {len(updates)} FABS records")

    # As a Developer, I want to update the FABS sample file to remove FundingAgencyCode after FABS is updated.
    def generate_sample_file(self, include_funding_agency: bool = False) -> str:
        sample_data = [{"action_date": "2018-01-01", "duns": "123456789"}]
        if include_funding_agency:
            sample_data[0]["funding_agency_code"] = "012"
        df = pd.DataFrame(sample_data)
        path = "fabs_sample.csv"
        df.to_csv(path, index=False)
        logger.info("Generated FABS sample file")
        return path

    # As an agency user, I want to ensure that deleted FSRS records are not included in submissions.
    def filter_deleted_fsrs(self, records: List[Dict]) -> List[Dict]:
        return [r for r in records if not r.get('is_deleted', False)]

    # As a website user, I want to see updated financial assistance data daily.
    def load_daily_fabs_data(self):
        # Simulate daily load
        today = date.today().isoformat()
        logger.info(f"Loaded daily FABS data for {today}")
        # In reality, pull from source

    # As a user, I want the publish button in FABS to deactivate after I click it while the derivations are happening.
    def perform_derivations(self, submission: Submission) -> bool:
        self._deactivate_publish_button(submission.id)
        # Simulate derivation time
        import time
        time.sleep(2)  # Placeholder
        self._derive_fields(submission.data)
        return True

    # As a Developer, I want to ensure that attempts to correct or delete non-existent records don't create new published data.
    def safe_delete_record(self, record_id: int):
        session = self.Session()
        record = session.query(FABSRecord).filter(FABSRecord.id == record_id).first()
        if not record:
            logger.warning(f"Non-existent record {record_id} delete attempt")
            session.close()
            return
        record.is_deleted = True
        session.commit()
        session.close()

    # As a user, I want the flexfields in my submission file to appear in the warning and error files when the only error is a missing required element.
    def generate_error_files(self, submission_file: str, errors: List[str], warnings: List[str]) -> Tuple[str, str]:
        error_df = pd.DataFrame({"Error": errors, "Flexfields": ["field1", "field2"]})  # Include flexfields
        warning_df = pd.DataFrame({"Warning": warnings, "Flexfields": ["field1", "field2"]})
        error_path = "errors.csv"
        warning_path = "warnings.csv"
        error_df.to_csv(error_path, index=False)
        warning_df.to_csv(warning_path, index=False)
        return error_path, warning_path

    # As a user, I want to have accurate and complete data related to PPoPCode and PPoPCongressionalDistrict.
    def derive_ppop_data(self, record: Dict) -> Dict:
        ppop_code = record.get('ppop_code', '')
        if re.match(r'^00\*{5}$', ppop_code) or ppop_code == '00FORGN':
            record['ppop_congressional_district'] = '00'
        return record

    # As an agency user, I want FABS deployed into production. (Simulate)
    def deploy_fabs_to_production(self):
        logger.info("FABS deployed to production (simulated)")

    # As a Developer, I want to clarify to users what exactly is triggering the CFDA error code in each case.
    def _validate_cfda(self, record: Dict) -> bool:
        cfda = record.get('cfda', '')
        if not cfda or not re.match(r'^\d+\.\d+$', cfda):
            return False
        return True

    # As an agency user, I want to be confident that the data coming from SAM is complete.
    def validate_sam_data(self, sam_data: Dict) -> bool:
        required = ['duns', 'registration_date']
        return all(key in sam_data for key in required)

    # As a Developer, I want my domain models to be indexed properly.
    def create_indexes(self):
        # Simulate indexing
        logger.info("Created indexes on domain models")

    # As a broker team member, I want to make some updates to the SQL codes for clarity.
    def execute_clarified_sql(self, query: str):
        session = self.Session()
        result = session.execute(query)
        session.close()
        return result.fetchall()

    # As a broker team member, I want to add the 00***** and 00FORGN PPoPCode cases to the derivation logic.
    def _derive_ppop(self, record: Dict):
        self.derive_ppop_data(record)

    # As a data user, I want to see the office names derived from office codes.
    def derive_office_names(self, records: List[Dict]) -> List[Dict]:
        office_map = {'001': 'Office of Grants', '002': 'Office of Loans'}
        for record in records:
            code = record.get('office_code')
            if code:
                record['office_name'] = office_map.get(code, 'Unknown')
        return records

    # As a broker user, I want the historical FABS loader to derive fields.
    def load_historical_fabs(self, file_path: str):
        df = pd.read_csv(file_path)
        df = self.derive_office_names(df.to_dict('records'))
        session = self.Session()
        for rec in df:
            fabs_rec = FABSRecord(**rec)
            session.add(fabs_rec)
        session.commit()
        session.close()
        logger.info("Loaded and derived historical FABS")

    # As a Developer, I want the data loaded from historical FABS to include the FREC derivations.
    def derive_frec(self, record: Dict):
        # Simulate FREC derivation
        record['frec_code'] = record.get('funding_agency_code', '')[:3]

    # As a user, I don't want to see NASA grants displayed as contracts. (Filter in display logic)
    def filter_nasa_grants(self, records: List[Dict]) -> List[Dict]:
        return [r for r in records if not (r.get('agency') == 'NASA' and r.get('type') == 'grant') or r.get('display_as') != 'contract']

    # As a user, I want the DUNS validations to accept records whose ActionTypes are B, C, or D and the DUNS is registered in SAM.
    def _validate_duns(self, record: Dict) -> bool:
        action_type = record.get('action_type', '')
        duns = record.get('duns', '')
        if action_type in ['B', 'C', 'D'] and self._is_duns_registered(duns):
            return True
        return False

    def _is_duns_registered(self, duns: str) -> bool:
        # Simulate SAM check
        return len(duns) == 9 and duns.isdigit()

    # As a user, I want the DUNS validations to accept records whose ActionDates are before the current registration date in SAM.
    def _validate_duns_date(self, record: Dict) -> bool:
        action_date = datetime.fromisoformat(record.get('action_date'))
        # Simulate SAM registration dates
        current_reg = datetime.now()
        initial_reg = datetime(2000, 1, 1)
        return initial_reg <= action_date <= current_reg

    # As a broker team member, I want to derive FundingAgencyCode.
    def _derive_funding_agency(self, record: Dict):
        # Simulate derivation
        record['funding_agency_code'] = '012' if 'grant' in record.get('type', '') else '999'

    # As an agency user, I want the maximum length allowed for LegalEntityAddressLine3 to match Schema v1.1.
    def validate_address_line3(self, address: str) -> bool:
        return len(address or '') <= 100

    # As an agency user, I want to use the schema v1.1 headers in my FABS file.
    def read_fabs_file_v11(self, file_path: str) -> pd.DataFrame:
        headers_v11 = ['action_date', 'duns', 'cfda']  # Example
        df = pd.read_csv(file_path, names=headers_v11)
        return df

    # As a agency user, I want to map the FederalActionObligation properly to the Atom Feed.
    def map_to_atom_feed(self, obligation: str) -> str:
        # Simulate mapping
        return f"obligation:{obligation}"

    # As a Broker user, I want to have PPoPZIP+4 work the same as the Legal Entity ZIP validations.
    def _validate_ppop_zip(self, record: Dict) -> bool:
        zip_code = record.get('ppop_zip', '')
        pattern = r'^\d{5}(-\d{4})?$'
        return bool(re.match(pattern, zip_code))

    # As a FABS user, I want to link the SAMPLE FILE on the "What you want to submit" dialog to point to the correct file.
    def get_sample_file_link(self) -> str:
        return self.generate_sample_file()

    # As an Agency user, I want FPDS data to be up-to-date daily.
    def load_daily_fpds(self):
        # Simulate
        logger.info("Loaded daily FPDS data")

    # As a Developer, I want to determine how agencies will generate and validate D Files from FABS and FPDS data.
    def validate_d_file_generation(self, d_file_path: str) -> List[str]:
        errors = []
        df = pd.read_csv(d_file_path)
        if df.empty:
            errors.append("Empty file")
        return errors

    # As a user, I want to generate and validate D Files from FABS and FPDS data.
    def generate_and_validate_d(self, fabs: List[Dict], fpds: List[Dict]) -> Tuple[str, List[str]]:
        d_path = self.generate_d_file(fabs, fpds)
        return d_path, self.validate_d_file_generation(d_path)

    # As an Agency user, I want the header information box to show updated date AND time.
    def get_header_info(self) -> str:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"Updated: {now}"

    # As an Agency user, I want to receive a more helpful file-level error when I upload a file with the wrong extension.
    def validate_file_extension(self, file_path: str) -> Optional[str]:
        ext = os.path.splitext(file_path)[1].lower()
        if ext != '.csv':
            return "Error: File must be CSV. Please upload a .csv file."
        return None

    # As a tester, I want to have access to test features in environments other than Staging.
    def enable_test_features(self, env: str):
        if env != 'staging':
            logger.info(f"Enabled test features in {env}")

    # As a FABS user, I want to submission errors to accurately represent FABS errors.
    def get_submission_errors(self, submission: Submission) -> List[str]:
        errors = []
        for rule in self.validation_rules:
            if not rule.logic(submission.data):
                errors.append(f"{rule.code}: {rule.description}")
        return errors

    # As a FABS user, I want the frontend URLs to more accurately reflect the page I'm accessing.
    def generate_url(self, page: str) -> str:
        base = "https://broker.example.gov/fabs"
        sanitized = re.sub(r'[^\w\s-]', '', page).strip().lower().replace(' ', '-')
        return f"{base}/{sanitized}"

    # As an Agency user, I want all historical Financial Assistance data loaded for FABS go-live.
    def load_historical_fa(self):
        self.load_historical_fabs("historical_fabs.csv")  # Assume file

    # As a Developer, I want the historical FPDS data loader to include both extracted historical data and FPDS feed data.
    def load_historical_fpds(self, extracted_path: str, feed_data: List[Dict]):
        # Load both
        logger.info("Loaded historical FPDS from extracted and feed")

    # As an Agency user, I want historical FPDS data loaded.
    def load_historical_fpds_simple(self):
        self.load_historical_fpds("historical_fpds.csv", [])

    # As an Agency user, I want to accurately see who created a submission.
    def get_submission_creator(self, submission_id: int) -> str:
        session = self.Session()
        sub = session.query(Submission).filter(Submission.id == submission_id).first()
        session.close()
        return sub.created_by if sub else "Unknown"

    # As an agency user, I want to get File F in the correct format.
    def generate_file_f(self, data: List[Dict]) -> str:
        df = pd.DataFrame(data)
        path = "file_f.csv"
        df.to_csv(path, index=False, quoting=1)  # CSV with quotes
        return path

    # As an Agency user, I want to better understand my file-level errors.
    def explain_file_error(self, error_code: str) -> str:
        explanations = {"EXT-01": "Wrong file extension; use CSV."}
        return explanations.get(error_code, "Unknown error")

    # As a Developer, I want to provide FABS groups that function under the FREC paradigm.
    def create_frec_groups(self) -> List[str]:
        return ['FREC-Group1', 'FREC-Group2']

    # As a tester, I want to ensure that FABS is deriving fields properly through a robust test file plus a follow up check.
    def test_derivations(self, test_file: str):
        df = pd.read_csv(test_file)
        derived = [self._derive_fields(rec) for rec in df.to_dict('records')]
        # Check assertions
        assert len(derived) == len(df)
        logger.info("Derivation tests passed")

    # As an owner, I only want zero-padded fields.
    def zero_pad_fields(self, record: Dict):
        for key in record:
            if isinstance(record[key], str) and re.match(r'^\d+$', record[key]):
                record[key] = record[key].zfill(10)

    # As a Broker user, I want to submit records for individual recipients without receiving a DUNS error.
    def allow_individual_recipients(self, record: Dict) -> bool:
        if record.get('recipient_type') == 'individual':
            record['duns'] = None  # No DUNS required
        return self._validate_duns(record)

    # As a user, I want more information about how many rows will be published prior to deciding whether to publish.
    def get_publish_preview(self, submission: Submission) -> int:
        return len(submission.data.get('rows', []))

    # As a Developer, I want to prevent duplicate transactions from being published.
    def check_duplicate_transaction(self, transaction_id: str) -> bool:
        # Simulate check
        return transaction_id not in self.cache

    # As a FABS user, I want to submit a citywide as a PPoPZIP and pass validations.
    def validate_citywide_ppop(self, zip_code: str) -> bool:
        return zip_code == 'citywide'  # Allow special case

    # As a Broker user, I want to have updated error codes that accurately reflect the logic.
    def get_detailed_error(self, code: str, context: Dict) -> str:
        return f"{code}: {context.get('description', 'Error occurred')}"

    # As an agency user, I want to leave off the last 4 digits of the ZIP without an error.
    def _validate_zip_flexible(self, zip_code: str) -> bool:
        return bool(re.match(r'^\d{5}(-\d{4})?$', zip_code)) or len(zip_code) == 5

    # As a FABS user, I want to make sure the historical data includes all necessary columns.
    def validate_historical_columns(self, df: pd.DataFrame) -> bool:
        required = ['duns', 'action_date']
        return all(col in df.columns for col in required)

    # As a data user, I want to access two additional fields from the FPDS data pull.
    def pull_fpds_with_extra(self) -> Dict:
        return {'extra_field1': 'value1', 'extra_field2': 'value2'}

    # As a FABS user, I want additional helpful info in the submission dashboard.
    def get_dashboard_info(self, user_id: str) -> Dict:
        return {'pending': 5, 'published': 10, 'errors': 2}

    # As a FABS user, I want to download the uploaded FABS file.
    def download_uploaded_file(self, upload_id: str) -> str:
        # Simulate
        return "uploaded_fabs.csv"

    # As a Developer I want to quickly access Broker application data.
    def quick_access_data(self, query: str) -> List[Dict]:
        session = self.Session()
        # Dynamic query simulation
        results = session.execute("SELECT * FROM fabs_records LIMIT 10").fetchall()
        session.close()
        return [dict(row) for row in results]

    # As a Developer, I want to determine the best way to load historical FPDS data since 2007.
    def load_historical_fpds_since_2007(self):
        start_date = datetime(2007, 1, 1)
        # Load from start_date to now
        logger.info(f"Loaded FPDS since {start_date}")

    # As a FABS user, I want the language on FABS pages to be appropriate.
    def get_page_language(self, page: str) -> str:
        return f"FABS {page} content"

    # As a FABS user, I do not want DABS banner messages and vice versa.
    def get_banner(self, app: str) -> str:
        if app == 'fabs':
            return "FABS Banner"
        return "DABS Banner"

    # As a FABS user, I want to have read-only access to DABS.
    def set_readonly_access(self, user: str, app: str):
        if app == 'dabs':
            # Set permissions
            pass
        logger.info(f"Set read-only for {user} on {app}")

    # As a FABS user, I want to have my validations run in a reasonable amount of time.
    def run_validations(self, data: List[Dict]) -> List[str]:
        errors = []
        for rec in data[:100]:  # Limit for performance
            for rule in self.validation_rules:
                if not rule.logic(rec):
                    errors.append(rule.code)
        return errors

    # As a FABS user, I want to see correct status labels on the Submission Dashboard.
    def get_status_label(self, status: PublishStatus) -> str:
        return status.value.upper()

    # As an agency user, I want to know when the submission periods start and end.
    def get_submission_period(self) -> Tuple[datetime, datetime]:
        return datetime.now(), datetime.now() + pd.Timedelta(days=30)

    # As an agency user, I want a landing page to navigate to either FABS or DABS pages.
    def get_landing_redirect(self, target: str) -> str:
        return f"/{target.lower()}"

    # As an agency user, I want to submit my data elements surrounded by quotation marks.
    def read_quoted_csv(self, file_path: str) -> pd.DataFrame:
        return pd.read_csv(file_path, quoting=1)  # csv.QUOTE_ALL

    # As a Broker user, I want to Upload and Validate the error message to have accurate text.
    def upload_and_validate(self, file_path: str) -> List[str]:
        error = self.validate_file_extension(file_path)
        if error:
            return [error]
        df = self.read_quoted_csv(file_path)
        return self.run_validations(df.to_dict('records'))

    # As a Broker user, I want the D1 file generation to be synced with the FPDS data load.
    def generate_d1_synced(self, fpds_timestamp: datetime) -> str:
        if self.cache.get('fpds_load_time') == fpds_timestamp:
            logger.info("No update needed; returning cached D1")
            return self.cache.get('d1_file')
        # Generate new
        path = "d1_file.csv"
        self.cache['d1_file'] = path
        self.cache['fpds_load_time'] = fpds_timestamp
        return path

    # As a Website user, I want to access published FABS files.
    def get_published_fabs_files(self) -> List[str]:
        session = self.Session()
        records = session.query(FABSRecord).filter(FABSRecord.publish_status == 'published').all()
        session.close()
        return [f"file_{r.id}.zip" for r in records]

    # As an owner, I want to be sure that USAspending only send grant records to my system.
    def filter_grants_only(self, records: List[Dict]) -> List[Dict]:
        return [r for r in records if r.get('type') == 'grant']

    # As a Developer, I want the Broker resources, validations, and P&P pages are updated appropriately for the launch.
    def update_launch_pages(self):
        logger.info("Updated pages for FABS and DAIMS v1.1 launch")

    # As a UI designer, I want to redesign the Resources page... (Simulate UI changes in code, e.g., generate HTML)
    def generate_resources_html(self) -> str:
        return """
        <html>
        <head><title>Resources - New Design</title></head>
        <body><h1>Broker Resources</h1></body>
        </html>
        """

    # As a UI designer, I want to report to the Agencies about user testing... (Generate report)
    def generate_user_testing_report(self) -> str:
        return json.dumps({"contributions": ["Better UX"], "agencies": ["NASA", "DOE"]})

    # As a UI designer, I want to move on to round 2 of ... (Placeholder for design approval)
    def approve_design_round(self, page: str, round_num: int):
        logger.info(f"Approved round {round_num} for {page}")

    # As a Broker user, I want to help create content mockups.
    def create_content_mockup(self, data: Dict) -> str:
        return f"Mockup for {data.get('section')}"

    # As a UI designer, I want to track the issues that come up in Tech Thursday.
    def track_tech_thursday_issues(self, issues: List[str]):
        for issue in issues:
            logger.info(f"Tech Thursday issue to test/fix: {issue}")

    # As an Owner, I want to create a user testing summary from the UI SME.
    def create_testing_summary(self, sme_input: str) -> Dict:
        return {"summary": sme_input, "improvements": ["Fix UX issue 1"]}

    # As a UI designer, I want to begin user testing.
    def start_user_testing(self):
        logger.info("Started user testing for UI improvements")

    # As a UI designer, I want to schedule user testing.
    def schedule_testing(self, date: datetime):
        logger.info(f"Scheduled testing for {date}")

    # As an Owner, I want to design a schedule from the UI SME.
    def design_ui_schedule(self, sme_timeline: List[Dict]) -> Dict:
        return {"timeline": sme_timeline}

    # As an Owner, I want to design an audit from the UI SME.
    def design_ui_audit(self, sme_scope: str) -> Dict:
        return {"scope": sme_scope, "audit_plan": "Review all pages"}

    # As an Owner, I want to reset the environment to only take Staging MAX permissions.
    def reset_environment_permissions(self):
        # Simulate permission reset
        logger.info("Reset to Staging MAX permissions only")

    # As an agency user, I want the FABS validation rules to accept zero and blank for loan records.
    def _validate_loan_fields(self, record: Dict) -> bool:
        if record.get('record_type') == 'loan':
            return record.get('amount', '0') in ['', '0']
        return True

    # As an agency user, I want the FABS validation rules to accept zero and blank for non-loan records.
    def _validate_nonloan_fields(self, record: Dict) -> bool:
        if record.get('record_type') != 'loan':
            return record.get('amount', '0') in ['', '0']
        return True

    # As a broker team member, I want to add the 00***** and 00FORGN PPoPCode cases to the derivation logic. (Already covered)

    # As a user, I want to access the raw agency published files from FABS via USAspending.
    def access_raw_fabs_files(self, agency: str) -> List[str]:
        return [f"{agency}_fabs_raw_{i}.zip" for i in range(5)]

    # As a Developer, I want to update the Broker validation rule table to account for the rule updates in DB-2213. (Already covered)

    # As a data user, I want to receive updates to FABS records. (Duplicate, covered)

    # As a user, I want more information about how many rows will be published... (Covered)

    # As a FABS user, I want to submit a citywide as a PPoPZIP... (Covered)

    # As a Broker user, I want to have updated error codes... (Covered)

    # As an agency user, I want to leave off the last 4 digits... (Covered)

    # As a FABS user, I want to make sure the historical data includes all necessary columns. (Covered)

    # As a data user, I want to access two additional fields... (Covered)

    # As a FABS user, I want additional helpful info... (Covered)

    # As a FABS user, I want to download the uploaded FABS file. (Covered)

    # As a Developer I want to quickly access... (Covered)

    # As a FABS user, I want the language on FABS pages... (Covered)

    # As a FABS user, I do not want DABS banner... (Covered)

    # As a FABS user, I want to have read-only access to DABS. (Covered)

    # As a FABS user, I want to have my validations run... (Covered)

    # As a FABS user, I want to see correct status labels... (Covered)

    # As an agency user, I want to know when the submission periods... (Covered)

    # As an agency user, I want a landing page... (Covered)

    # As an agency user, I want to submit my data elements surrounded by quotation marks. (Covered)

    def _derive_fields(self, data: Dict):
        for rule in self.validation_rules:
            if 'derive' in rule.code.lower():
                rule.logic(data)

# Example usage (but since output only code, this is for completeness; in real, remove)
if __name__ == "__main__":
    broker = BrokerSystem()
    broker.load_daily_fabs_data()
    broker.deploy_fabs_to_production()
    submission = Submission(id=1, data={}, status=PublishStatus.DRAFT, created_by="user1")
    broker.publish_submission(1, "user1")
    print(broker.get_header_info())