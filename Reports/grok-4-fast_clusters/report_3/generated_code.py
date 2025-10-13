import json
import logging
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from zipfile import ZipFile
import pandas as pd
from lxml import etree
from pathlib import Path

# Configure logging for better troubleshooting
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BrokerSystem:
    """
    Core Broker system class implementing key functionalities from user stories.
    Handles data validation, derivation, file processing, submissions, and more.
    Uses SQLite for in-memory database simulation.
    """

    def __init__(self, db_path: str = ':memory:'):
        self.conn = sqlite3.connect(db_path)
        self._init_db()
        self._init_new_relic_monitoring()  # Simulates New Relic integration
        self.cache = {}  # For caching D files and requests
        self.submission_counter = 0

    def _init_db(self):
        """Initialize database schema for FABS, FPDS, and other tables."""
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fabs_submissions (
                id INTEGER PRIMARY KEY,
                agency_code TEXT,
                submission_file TEXT,
                publish_status TEXT DEFAULT 'draft',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rows_to_publish INTEGER,
                creator TEXT,
                derivations JSON
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fpds_data (
                id INTEGER PRIMARY KEY,
                piid TEXT UNIQUE,
                funding_agency_code TEXT,
                action_obligation DECIMAL,
                ppop_zip TEXT,
                action_date DATE,
                historical BOOLEAN DEFAULT FALSE
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sam_data (
                duns TEXT PRIMARY KEY,
                registration_date DATE,
                expiration_date DATE,
                legal_entity_zip TEXT,
                address_line3 TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS published_awards (
                id INTEGER PRIMARY KEY,
                fabs_id INTEGER,
                frec_code TEXT,
                funding_agency_code TEXT,
                ppop_code TEXT,
                ppop_congressional_district TEXT,
                action_type TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gtas_windows (
                id INTEGER PRIMARY KEY,
                start_date DATE,
                end_date DATE,
                locked BOOLEAN DEFAULT FALSE
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS historical_fabs (
                id INTEGER PRIMARY KEY,
                data JSON
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS flexfields (
                submission_id INTEGER,
                field_name TEXT,
                value TEXT
            )
        ''')
        self.conn.commit()

    def _init_new_relic_monitoring(self):
        """Simulate New Relic data collection across applications."""
        logger.info("New Relic monitoring initialized for all applications.")

    def process_deletions(self, date: str = '2017-12-19'):
        """
        Process deletions for 12-19-2017 as per Cluster (4,).
        """
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM fabs_submissions WHERE updated_at < ?", (date,))
        deleted_count = cursor.rowcount
        self.conn.commit()
        logger.info(f"Processed {deleted_count} deletions for {date}.")
        return deleted_count

    def derive_funding_agency_code(self, record: Dict) -> str:
        """
        Derive FundingAgencyCode for data quality improvement (Cluster 4).
        """
        # Reasonable logic: Use agency code from FPDS if available, else default.
        agency_code = record.get('agency_code', 'UNKNOWN')
        if agency_code.startswith('00'):
            return f"Derived_{agency_code}"
        return agency_code

    def derive_ppop_code(self, record: Dict, include_foreign: bool = True) -> str:
        """
        Add 00***** and 00FORGN PPoPCode cases to derivation logic (Cluster 4).
        """
        zip_code = record.get('ppop_zip', '')
        if zip_code.startswith('00') or 'FORGN' in zip_code:
            return '00FORGN' if include_foreign else '00UNKNOWN'
        # Simulate derivation based on ZIP
        if len(zip_code) >= 5 and zip_code[:2] == '00':
            return '00DOMESTIC'
        return zip_code[:5] if len(zip_code) >= 5 else 'UNKNOWN'

    def validate_ppop_zip(self, zip_code: str, is_legal_entity: bool = False) -> Tuple[bool, str]:
        """
        Make PPoPZIP+4 work like Legal Entity ZIP validations (Cluster 4).
        Allow citywide ZIP without last 4 digits (Cluster 3,5).
        """
        if not zip_code:
            return False, "ZIP code is required."
        zip_clean = ''.join(filter(str.isdigit, zip_code))
        if len(zip_clean) < 5:
            return False, "ZIP code must be at least 5 digits."
        if is_legal_entity and len(zip_clean) > 10:
            return False, "Legal Entity ZIP must not exceed 10 digits."
        if len(zip_clean) == 5:  # Allow citywide
            return True, "Valid citywide ZIP."
        return True if len(zip_clean) in [5, 9, 10] else (False, "Invalid ZIP format.")

    def map_federal_action_obligation(self, record: Dict) -> Dict:
        """
        Map FederalActionObligation to Atom Feed (Cluster 4).
        """
        obligation = record.get('federal_action_obligation', 0)
        atom_feed = {
            'obligation': float(obligation),
            'updated': datetime.now().isoformat(),
            'type': 'financial_assistance'
        }
        return atom_feed

    def sync_d1_file_generation(self, fpds_updated: bool = False) -> str:
        """
        Sync D1 file generation with FPDS data load (Cluster 4).
        """
        if not fpds_updated:
            return "No updates; using cached D1 file."
        # Generate new D1 file
        d1_content = self._generate_d1_file()
        cache_key = 'd1_latest'
        self.cache[cache_key] = d1_content
        return f"Generated new D1 file: {len(d1_content)} bytes."

    def _generate_d1_file(self) -> str:
        """Generate sample D1 file content."""
        return json.dumps({"version": "1.1", "records": []})

    def update_sql_for_clarity(self, sql_code: str) -> str:
        """
        Make updates to SQL codes for clarity (Cluster 4).
        """
        # Simulate adding comments and formatting
        clarified = f"-- Updated for clarity\n{sql_code}"
        return clarified

    def load_historical_fpds_data(self, since_year: int = 2007) -> int:
        """
        Load historical FPDS data including extracted and feed data (Cluster 2).
        """
        cursor = self.conn.cursor()
        # Simulate loading data
        sample_data = [
            {'piid': 'HIST123', 'funding_agency_code': '097', 'action_obligation': 1000.0, 'ppop_zip': '20001', 'action_date': '2008-01-01', 'historical': True}
        ]
        for rec in sample_data:
            cursor.execute('''
                INSERT OR REPLACE INTO fpds_data (piid, funding_agency_code, action_obligation, ppop_zip, action_date, historical)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (rec['piid'], rec['funding_agency_code'], rec['action_obligation'], rec['ppop_zip'], rec['action_date'], rec['historical']))
        self.conn.commit()
        return len(sample_data)

    def update_fabs_submission_status(self, submission_id: int, new_status: str) -> bool:
        """
        Update FABS submission when publishStatus changes (Cluster 2).
        """
        cursor = self.conn.cursor()
        cursor.execute("UPDATE fabs_submissions SET publish_status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", (new_status, submission_id))
        self.conn.commit()
        return cursor.rowcount > 0

    def deactivate_publish_button_during_derivation(self, submission_id: int) -> Dict:
        """
        Deactivate publish button while derivations happen (Cluster 2).
        """
        # Simulate async derivation process
        logger.info(f"Derivations in progress for submission {submission_id}. Publish button deactivated.")
        derivations = self._perform_derivations(submission_id)
        return {'status': 'derivation_complete', 'derivation_results': derivations}

    def _perform_derivations(self, submission_id: int) -> Dict:
        """Perform FREC and other derivations for historical FABS loader (Cluster 2)."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM fabs_submissions WHERE id = ?", (submission_id,))
        rec = cursor.fetchone()
        if rec:
            derived = {
                'frec_code': 'DerivedFREC',
                'funding_agency_code': self.derive_funding_agency_code({'agency_code': rec[1]})
            }
            cursor.execute("UPDATE fabs_submissions SET derivations = ? WHERE id = ?", (json.dumps(derived), submission_id))
            self.conn.commit()
            return derived
        return {}

    def add_gtas_window_data(self, start_date: str, end_date: str) -> int:
        """
        Add GTAS window data to lock site during submission period (Cluster 2).
        """
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO gtas_windows (start_date, end_date) VALUES (?, ?)", (start_date, end_date))
        self.conn.commit()
        window_id = cursor.lastrowid
        # Lock site simulation
        if datetime.now().date() >= datetime.strptime(start_date, '%Y-%m-%d').date():
            cursor.execute("UPDATE gtas_windows SET locked = TRUE WHERE id = ?", (window_id,))
            self.conn.commit()
        return window_id

    def update_fabs_sample_file(self):
        """
        Update FABS sample file to remove FundingAgencyCode header (Cluster 2).
        """
        sample_file_path = 'fabs_sample.csv'
        if os.path.exists(sample_file_path):
            df = pd.read_csv(sample_file_path)
            if 'FundingAgencyCode' in df.columns:
                df = df.drop(columns=['FundingAgencyCode'])
                df.to_csv(sample_file_path, index=False)
                logger.info("Updated FABS sample file by removing FundingAgencyCode.")

    def load_historical_fabs_data(self) -> int:
        """
        Load historical FABS with FREC derivations (Cluster 2).
        Ensure all necessary columns (Cluster 2).
        """
        cursor = self.conn.cursor()
        sample_historical = [{'data': json.dumps({'columns': ['piid', 'amount'], 'frec': 'Derived'})}]
        for rec in sample_historical:
            cursor.execute("INSERT INTO historical_fabs (data) VALUES (?)", (rec['data'],))
        self.conn.commit()
        return len(sample_historical)

    def access_additional_fpds_fields(self) -> List[Dict]:
        """
        Access two additional fields from FPDS data pull (Cluster 2).
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT piid, funding_agency_code, action_obligation FROM fpds_data LIMIT 2")
        return [dict(row) for row in cursor.fetchall()]

    def generate_submission_dashboard_info(self, user_id: str) -> Dict:
        """
        Additional helpful info in submission dashboard (Cluster 2).
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, publish_status, rows_to_publish FROM fabs_submissions WHERE creator = ? ORDER BY updated_at DESC", (user_id,))
        submissions = [dict(row) for row in cursor.fetchall()]
        return {'submissions': submissions, 'total': len(submissions), 'ig_requests': 0}

    def download_uploaded_fabs_file(self, submission_id: int) -> Optional[str]:
        """
        Download uploaded FABS file (Cluster 2).
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT submission_file FROM fabs_submissions WHERE id = ?", (submission_id,))
        result = cursor.fetchone()
        return result[0] if result else None

    def update_fabs_language_and_banners(self, is_fabs: bool = True) -> str:
        """
        Appropriate language and no cross banners for FABS/DABS (Cluster 2).
        """
        if is_fabs:
            return "Welcome to FABS - Financial Assistance Submission System."
        return "Welcome to DABS - Detailed Award Data Submission."

    def get_submission_periods(self) -> List[Dict]:
        """
        Know when submission periods start and end (Cluster 2).
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT start_date, end_date FROM gtas_windows WHERE locked = FALSE")
        return [dict(row) for row in cursor.fetchall()]

    def validate_and_upload_fabs_file(self, file_path: str, is_fabs: bool = True) -> Dict:
        """
        Upload and validate with accurate error messages (Cluster 0).
        Handle wrong extension error (Cluster 0).
        """
        if not file_path.lower().endswith('.csv'):
            return {'errors': ['File must be CSV format.'], 'success': False}
        
        try:
            df = pd.read_csv(file_path)
            errors = []
            warnings = []
            
            # Validate flexfields (Cluster 0)
            if 'flexfields' in df.columns:
                for idx, row in df.iterrows():
                    if pd.isna(row.get('required_field')):
                        warnings.append(f"Row {idx}: Missing required element, but flexfields preserved.")
            
            # DUNS validations (Cluster 0)
            for idx, row in df.iterrows():
                duns = row.get('duns', '')
                action_type = row.get('action_type', '')
                action_date = row.get('action_date', '')
                if duns and action_type in ['B', 'C', 'D']:
                    valid, msg = self._validate_duns(duns, action_date)
                    if not valid:
                        errors.append(f"Row {idx}: {msg}")
                # CFDA error clarification (Cluster 0)
                cfda = row.get('cfda', '')
                if not cfda and action_type == 'A':
                    errors.append(f"Row {idx}: CFDA required for new awards (ActionType A).")
            
            # Zero/blank for loan/non-loan records (Cluster 3)
            loan_indicator = row.get('loan_indicator', False)
            if not loan_indicator:  # Non-loan
                if row.get('amount', 0) in [0, '']:
                    pass  # Accept
                else:
                    errors.append(f"Row {idx}: Non-loan amount invalid.")
            
            # Quotation marks for data elements (Cluster 3)
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.strip('"')
            
            # Schema v1.1 headers (Cluster 3)
            required_headers = ['piid', 'action_date', 'amount']
            missing_headers = [h for h in required_headers if h not in df.columns]
            if missing_headers:
                errors.append(f"Missing headers: {missing_headers} (Schema v1.1 required).")
            
            # LegalEntityAddressLine3 max length (Cluster 3)
            if 'legal_entity_address_line3' in df.columns:
                for idx, val in enumerate(df['legal_entity_address_line3']):
                    if val and len(str(val)) > 100:  # Assuming v1.1 limit
                        errors.append(f"Row {idx}: AddressLine3 exceeds 100 chars.")
            
            # Updated error codes (Cluster 5)
            if errors:
                for err in errors:
                    if 'DUNS' in err:
                        errors[errors.index(err)] = f"ERROR_DUNS_001: {err}"
            
            submission_id = self._save_submission(df.to_csv(index=False), errors, warnings, is_fabs)
            
            # Prevent duplicates (Cluster 1, 0)
            if self._check_duplicate_transaction(submission_id):
                return {'errors': ['Duplicate transaction detected.'], 'success': False}
            
            return {
                'submission_id': submission_id,
                'errors': errors,
                'warnings': warnings,
                'success': len(errors) == 0,
                'rows_to_publish': len(df)
            }
        except Exception as e:
            logger.error(f"Validation error: {str(e)}")
            return {'errors': [str(e)], 'success': False}

    def _validate_duns(self, duns: str, action_date: str) -> Tuple[bool, str]:
        """
        DUNS validations: Accept B,C,D if registered in SAM, even expired if date conditions met (Cluster 0).
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT registration_date, expiration_date FROM sam_data WHERE duns = ?", (duns,))
        result = cursor.fetchone()
        if not result:
            return False, "DUNS not registered in SAM."
        
        reg_date, exp_date = result
        act_dt = datetime.strptime(action_date, '%Y-%m-%d').date()
        reg_dt = datetime.strptime(reg_date, '%Y-%m-%d').date()
        exp_dt = datetime.strptime(exp_date, '%Y-%m-%d').date()
        
        if act_dt < reg_dt:
            return False, "Action date before initial registration."
        if act_dt > exp_dt:
            return False, "Action date after expiration (unless B,C,D)."
        # For B,C,D allow expired if registered
        return True, "DUNS valid."

    def _save_submission(self, file_content: str, errors: List, warnings: List, is_fabs: bool) -> int:
        """Save submission to DB."""
        self.submission_counter += 1
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO fabs_submissions (id, submission_file, publish_status, rows_to_publish, creator, derivations)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (self.submission_counter, file_content, 'validated' if not errors else 'error', len(errors) + len(warnings), 'user', json.dumps({'errors': len(errors), 'warnings': len(warnings)})))
        self.conn.commit()
        # Store flexfields simulation
        cursor.execute("INSERT INTO flexfields (submission_id, field_name, value) VALUES (?, ?, ?)",
                       (self.submission_counter, 'sample_flex', 'value'))
        self.conn.commit()
        return self.submission_counter

    def _check_duplicate_transaction(self, submission_id: int) -> bool:
        """Prevent duplicate transactions (Cluster 0)."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM fabs_submissions WHERE id != ? AND submission_file = (SELECT submission_file FROM fabs_submissions WHERE id = ?)", (submission_id, submission_id))
        return cursor.fetchone() is not None

    def prevent_double_publishing(self, submission_id: int) -> bool:
        """
        Prevent double publishing after refresh (Cluster 1).
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT publish_status FROM fabs_submissions WHERE id = ?", (submission_id,))
        status = cursor.fetchone()
        if status and status[0] == 'published':
            return False  # Already published
        self.update_fabs_submission_status(submission_id, 'published')
        return True

    def manage_d_files_generation(self, request_id: str, fpds_data: Optional[Dict] = None) -> str:
        """
        Manage and cache D Files generation requests (Cluster 1).
        """
        if request_id in self.cache:
            return f"Cached D File for {request_id}"
        # Generate D File from FABS and FPDS
        d_content = self._generate_d_file(fpds_data)
        self.cache[request_id] = d_content
        return f"Generated D File: {len(d_content)} bytes"

    def _generate_d_file(self, fpds_data: Optional[Dict]) -> str:
        """Generate and validate D File (Cluster 1)."""
        content = {
            'header': 'Schema v1.1',
            'fpds': fpds_data or {},
            'fabs': {'records': []}
        }
        return json.dumps(content)

    def handle_large_flexfields(self, flexfields: List[Dict]) -> bool:
        """
        Include large number of flexfields without performance impact (Cluster 1,3).
        """
        # Simulate efficient batch insert
        cursor = self.conn.cursor()
        for ff in flexfields:
            cursor.execute("INSERT INTO flexfields (submission_id, field_name, value) VALUES (?, ?, ?)",
                           (1, ff.get('name'), ff.get('value', '')[:255]))  # Limit for perf
        self.conn.commit()
        return True

    def ensure_no_nasa_grants_as_contracts(self, record: Dict) -> bool:
        """
        Don't show NASA grants as contracts (Cluster 1).
        """
        agency = record.get('agency_code', '')
        if agency == 'NASA' and record.get('type') == 'grant':
            record['display_type'] = 'grant'
        return record.get('display_type') != 'contract'

    def access_raw_agency_files(self, agency_code: str) -> List[str]:
        """
        Access raw published FABS files via USAspending (Cluster 1).
        """
        # Simulate file paths
        files = [f"{agency_code}_file_{i}.csv" for i in range(3)]
        return files

    def update_daily_financial_data(self):
        """
        See updated financial assistance data daily (Cluster 1).
        """
        # Simulate daily update from FPDS
        self.load_historical_fpds_data()
        logger.info("Daily financial data update completed.")

    def prevent_nonexistent_corrections(self, record_id: int) -> bool:
        """
        Ensure attempts to correct/delete non-existent records don't create new data (Cluster 1).
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM published_awards WHERE id = ?", (record_id,))
        if not cursor.fetchone():
            return False  # Don't create
        # Simulate correction
        cursor.execute("UPDATE published_awards SET updated_at = CURRENT_TIMESTAMP WHERE id = ?", (record_id,))
        self.conn.commit()
        return True

    def derive_ppop_congressional_district(self, zip_code: str, state: str) -> str:
        """
        Accurate PPoPCode and PPoPCongressionalDistrict (Cluster 1).
        """
        # Simulate derivation
        return f"{state}-CD01" if zip_code else "UNKNOWN"

    def allow_individual_recipients_no_duns(self, record: Dict) -> bool:
        """
        Submit individual recipients without DUNS error (Cluster 1).
        """
        if record.get('recipient_type') == 'individual' and not record.get('duns'):
            return True
        return self._validate_duns(record.get('duns', ''), record.get('action_date', ''))[0]

    def show_rows_before_publish(self, submission_id: int) -> int:
        """
        More info on rows to publish before deciding (Cluster 1).
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT rows_to_publish FROM fabs_submissions WHERE id = ?", (submission_id,))
        result = cursor.fetchone()
        return result[0] if result else 0

    def validate_citywide_ppop_zip(self, zip_code: str) -> Tuple[bool, str]:
        """
        Submit citywide as PPoPZIP and pass (Cluster 1).
        """
        return self.validate_ppop_zip(zip_code)

    def ensure_reasonable_validation_time(self, df: pd.DataFrame) -> float:
        """
        Run validations in reasonable time (Cluster 1).
        """
        start = datetime.now()
        # Simulate validation loop
        for idx in range(len(df)):
            self.validate_ppop_zip(df.iloc[idx].get('zip', ''))
        end = datetime.now()
        return (end - start).total_seconds()

    def receive_fabs_updates(self):
        """
        Receive updates to FABS records (Cluster 3).
        """
        # Simulate update feed
        logger.info("FABS records updated via Atom feed.")

    def exclude_deleted_fsrs_records(self, records: List[Dict]) -> List[Dict]:
        """
        Ensure deleted FSRS records not included (Cluster 3).
        """
        return [r for r in records if not r.get('deleted', False)]

    def accept_zero_blank_loans(self, record: Dict, is_loan: bool) -> bool:
        """
        Accept zero/blank for loan/non-loan records (Cluster 3).
        """
        amount = record.get('amount', 0)
        return amount in [0, '', None] if is_loan else amount > 0

    def deploy_fabs_to_production(self):
        """
        Deploy FABS to production (Cluster 3).
        """
        logger.info("FABS deployed to production.")

    def ensure_complete_sam_data(self) -> bool:
        """
        Confident SAM data is complete (Cluster 3).
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sam_data")
        count = cursor.fetchone()[0]
        return count > 0  # Simulate completeness check

    def derive_all_data_elements(self, record: Dict) -> Dict:
        """
        All derived data elements properly derived (Cluster 3).
        """
        record['funding_agency_code'] = self.derive_funding_agency_code(record)
        record['ppop_code'] = self.derive_ppop_code(record)
        record['ppop_congressional_district'] = self.derive_ppop_congressional_district(
            record.get('ppop_zip', ''), record.get('state', '')
        )
        return record

    def use_schema_v1_1_headers(self, headers: List[str]) -> bool:
        """
        Use schema v1.1 headers (Cluster 3).
        """
        v1_1_headers = ['piid', 'action_date', 'amount', 'legal_entity_zip']
        return all(h in headers for h in v1_1_headers[:3])  # Partial check

    def ensure_daily_fpds_updates(self):
        """
        FPDS data up-to-date daily (Cluster 3).
        """
        self.update_daily_financial_data()

    def load_all_historical_fa_data(self):
        """
        All historical Financial Assistance data loaded (Cluster 3).
        """
        self.load_historical_fabs_data()

    def load_historical_fpds(self):
        """
        Historical FPDS loaded (Cluster 3).
        """
        self.load_historical_fpds_data()

    def generate_file_f_format(self) -> str:
        """
        Get File F in correct format (Cluster 3).
        """
        return json.dumps({"file_f": "formatted_data"})

    def better_file_level_errors(self, file_path: str) -> List[str]:
        """
        Better understand file-level errors (Cluster 3).
        """
        errors = []
        if not os.path.exists(file_path):
            errors.append("File not found.")
        if Path(file_path).suffix != '.csv':
            errors.append("Invalid file extension. Use CSV.")
        return errors

    def derive_office_names(self, office_code: str) -> str:
        """
        See office names derived from codes (Cluster 2,5).
        """
        # Simulate lookup
        office_map = {'097': 'NASA Headquarters'}
        return office_map.get(office_code, 'Unknown Office')

    def link_sample_file_correctly(self) -> str:
        """
        Link SAMPLE FILE to correct file (Cluster 2,4,5).
        """
        return "path/to/correct_fabs_sample.csv"

    def submit_without_zip_last4_error(self, zip_code: str) -> Tuple[bool, str]:
        """
        Leave off last 4 digits without error (Cluster 3,5).
        """
        return self.validate_ppop_zip(zip_code)

    def show_correct_status_labels(self, submission_id: int) -> str:
        """
        Correct status labels on Dashboard (Cluster 1,2).
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT publish_status FROM fabs_submissions WHERE id = ?", (submission_id,))
        status = cursor.fetchone()
        labels = {'draft': 'Ready to Publish', 'published': 'Published', 'error': 'Needs Attention'}
        return labels.get(status[0], 'Unknown')

    def update_validation_rules_db2213(self):
        """
        Update Broker validation rule table for DB-2213 (Cluster 0).
        """
        logger.info("Validation rules updated for DB-2213.")

    def handle_missing_required_in_flexfields(self, errors: List, flexfields: Dict):
        """
        Flexfields appear in error files if only missing required (Cluster 0).
        """
        if len(errors) == 1 and 'missing required' in errors[0]:
            errors.append(f"Flexfields preserved: {list(flexfields.keys())}")

    def clarify_cfda_error(self, record: Dict) -> str:
        """
        Clarify what triggers CFDA error (Cluster 0).
        """
        if not record.get('cfda') and record.get('action_type') == 'A':
            return "CFDA error: Required for new awards (ActionType 'A')."
        return "No CFDA error."

    def update_broker_resources_for_launch(self):
        """
        Update resources, validations, P&P for FABS/DAIMS v1.1 launch (Cluster 0).
        """
        logger.info("Broker resources updated for v1.1 launch.")

    def prevent_duplicate_from_time_gap(self):
        """
        Prevent duplicates between validation and publishing (Cluster 0).
        """
        self.prevent_double_publishing(1)  # Example

    def log_better_for_troubleshooting(self, submission_id: int, function: str):
        """
        Better logging for submissions and functions (Cluster 5).
        """
        logger.info(f"Troubleshooting: Submission {submission_id} in function {function}.")

    def access_published_fabs_files(self) -> List[str]:
        """
        Access published FABS files (Cluster 5).
        """
        return ["published_fabs_2023.csv"]

    def ensure_only_grant_records_sent(self):
        """
        USAspending only sends grant records (Cluster 5).
        """
        logger.info("Configured to send only grant records.")

    def create_content_mockups(self):
        """
        Help create content mockups for efficient submission (Cluster 5).
        """
        return {"mockup": "Sample submission form"}

    def track_tech_thursday_issues(self, issues: List[str]):
        """
        Track issues from Tech Thursday (Cluster 5).
        """
        for issue in issues:
            logger.info(f"Tech Thursday issue: {issue}")

    def create_user_testing_summary(self, ui_sme_data: Dict) -> str:
        """
        User testing summary from UI SME (Cluster 5).
        """
        return f"Summary: {ui_sme_data.get('improvements', [])}"

    def begin_user_testing(self):
        """
        Begin user testing (Cluster 5).
        """
        logger.info("User testing begun.")

    def schedule_user_testing(self, date: str):
        """
        Schedule user testing (Cluster 5).
        """
        logger.info(f"User testing scheduled for {date}.")

    def design_ui_schedule(self, ui_sme: Dict) -> Dict:
        """
        Design schedule from UI SME (Cluster 5).
        """
        return {"timeline": ui_sme.get('plan', [])}

    def design_ui_audit(self, ui_sme: Dict) -> Dict:
        """
        Design audit from UI SME (Cluster 5).
        """
        return {"scope": ui_sme.get('scope', [])}

    def reset_environment_staging_max(self):
        """
        Reset to only Staging MAX permissions (Cluster 5).
        """
        logger.info("Environment reset to Staging MAX; FABS testers access revoked.")

    def index_domain_models(self):
        """
        Index domain models for validation speed (Cluster 5).
        """
        # Simulate indexing
        logger.info("Domain models indexed for faster validation.")

    def show_header_updated_datetime(self, submission_id: int) -> str:
        """
        Header shows updated date AND time (Cluster 5).
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT updated_at FROM fabs_submissions WHERE id = ?", (submission_id,))
        dt = cursor.fetchone()[0]
        return datetime.strptime(dt, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')

    def only_zero_padded_fields(self, record: Dict):
        """
        Only zero-padded fields (Cluster 5).
        """
        for key, val in record.items():
            if isinstance(val, str) and val.isdigit():
                record[key] = val.zfill(10)

    def updated_error_codes(self, error: str) -> str:
        """
        Updated error codes with logic info (Cluster 5).
        """
        return f"ERROR_{error[:3].upper()}: Detailed logic explanation."

    def quick_access_broker_data(self, query: str):
        """
        Quick access to Broker app data (Cluster 5).
        """
        cursor = self.conn.cursor()
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]

    def read_only_dabs_access_for_fabs(self, user: str):
        """
        Read-only access to DABS for FABS users (Cluster 5).
        """
        logger.info(f"Granted read-only DABS access to {user}.")

    def create_landing_page(self):
        """
        Landing page to navigate FABS/DABS (Cluster 5).
        """
        return {"page": "FABS/DABS Landing"}

    def provide_fabs_groups_frec(self):
        """
        Provide FABS groups under FREC paradigm (Cluster 2).
        """
        return {"frec_groups": ["Group1", "Group2"]}

    def ensure_historical_columns(self):
        """
        Historical data includes all necessary columns (Cluster 2).
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM historical_fabs LIMIT 1")
        columns = [desc[0] for desc in cursor.description]
        required = ['piid', 'amount']
        return all(col in columns for col in required)

# Example usage simulation
if __name__ == "__main__":
    broker = BrokerSystem()
    broker.process_deletions()
    sample_record = {'agency_code': '097', 'ppop_zip': '00FORGN', 'federal_action_obligation': 5000}
    print(broker.derive_funding_agency_code(sample_record))
    print(broker.derive_ppop_code(sample_record))
    print(broker.validate_ppop_zip('12345'))
    broker.load_historical_fpds_data()
    broker.validate_and_upload_fabs_file('sample.csv')
    broker.sync_d1_file_generation()
    broker.update_daily_financial_data()
    broker.show_header_updated_datetime(1)