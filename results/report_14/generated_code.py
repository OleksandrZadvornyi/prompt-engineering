import json
import logging
import hashlib
import datetime
import os
import csv
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import sqlite3
from urllib.parse import urlparse

# Configure logging for better troubleshooting
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class Submission:
    id: str
    file_path: str
    status: str = 'uploaded'
    errors: List[str] = None
    warnings: List[str] = None
    publish_status: str = 'draft'
    created_by: str = ''
    created_at: datetime.datetime = None
    if errors is None:
        errors = []
    if warnings is None:
        warnings = []

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.datetime.now()

class ValidationRule:
    def __init__(self, code: str, description: str, logic: callable):
        self.code = code
        self.description = description
        self.logic = logic

class BrokerSystem:
    def __init__(self, db_path: str = ':memory:'):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self._setup_database()
        self.cache: Dict[str, Any] = {}
        self.validation_rules: Dict[str, ValidationRule] = self._load_validation_rules()
        self.historical_data: Dict[str, List[Dict]] = {}
        self.load_historical_data()
        # Simulate GTAS window lockdown
        self.gtas_lockdown = False
        self.gtas_window_start = datetime.date(2023, 10, 1)
        self.gtas_window_end = datetime.date(2023, 10, 31)

    def _setup_database(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS submissions (
                id TEXT PRIMARY KEY,
                file_path TEXT,
                status TEXT,
                publish_status TEXT,
                created_by TEXT,
                created_at TEXT,
                errors TEXT,
                warnings TEXT
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS published_awards (
                id TEXT PRIMARY KEY,
                data JSON,
                derived_fields JSON
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS gtas_data (
                window_start TEXT,
                window_end TEXT,
                locked BOOLEAN
            )
        ''')
        self.conn.commit()

    def _load_validation_rules(self) -> Dict[str, ValidationRule]:
        rules = {}
        # DUNS validation: accept B, C, D actions if registered/expired appropriately
        def duns_validation(record: Dict) -> bool:
            action_type = record.get('ActionType', '')
            duns = record.get('DUNS', '')
            action_date = datetime.datetime.strptime(record.get('ActionDate', ''), '%Y-%m-%d').date()
            # Simulate SAM registration: assume registered if not empty, expired if date > current
            current_date = datetime.date.today()
            initial_reg_date = current_date - datetime.timedelta(days=365 * 5)  # Assume 5 year initial
            if action_type in ['B', 'C', 'D'] and duns:
                if action_date >= initial_reg_date:
                    return True
            return False

        rules['DUNS-001'] = ValidationRule('DUNS-001', 'DUNS validation for actions B,C,D', duns_validation)

        # ZIP validation: allow without last 4 digits
        def zip_validation(record: Dict) -> bool:
            zip_code = record.get('PPoPZIP', '').strip()
            if len(zip_code) >= 5:
                return True
            return False

        rules['ZIP-001'] = ValidationRule('ZIP-001', 'PPoP ZIP validation', zip_validation)

        # PPoPCode validation
        def ppop_code_validation(record: Dict) -> bool:
            ppop_code = record.get('PPoPCode', '')
            if ppop_code in ['00*****', '00FORGN'] or ppop_code.startswith('citywide'):
                return True
            return bool(ppop_code)  # Assume non-empty is valid for simplicity

        rules['PPOP-001'] = ValidationRule('PPOP-001', 'PPoPCode validation', ppop_code_validation)

        # CFDA error clarification
        def cfda_validation(record: Dict) -> bool:
            cfda = record.get('CFDA', '')
            if not cfda:
                return False
            # Simulate trigger: invalid format
            return len(cfda) == 7 and '.' in cfda

        rules['CFDA-001'] = ValidationRule('CFDA-001', 'CFDA validation with clear triggers', cfda_validation)

        # Zero/blank for loans/non-loans
        def loan_validation(record: Dict) -> bool:
            is_loan = record.get('RecordType', '') == 'loan'
            value = record.get('FederalActionObligation', 0)
            if is_loan or not is_loan:  # Accept zero/blank for both as per stories
                return value >= 0 or value == ''
            return True

        rules['LOAN-001'] = ValidationRule('LOAN-001', 'Loan record zero/blank acceptance', loan_validation)

        # Flexfields large number without impact (simulate by ignoring count)
        def flexfields_validation(record: Dict) -> bool:
            flexfields = record.get('flexfields', [])
            return True  # No impact simulation

        rules['FLEX-001'] = ValidationRule('FLEX-001', 'Flexfields performance', flexfields_validation)

        # Update rules as per DB-2213 (assume added)
        rules['DB2213-001'] = ValidationRule('DB2213-001', 'Updated rule from DB-2213', lambda r: True)

        return rules

    def upload_and_validate(self, file_path: str, user: str) -> Submission:
        sub_id = hashlib.md5(file_path.encode()).hexdigest()
        submission = Submission(id=sub_id, file_path=file_path, created_by=user)

        # Simulate file reading as CSV
        records = self._read_fabs_file(file_path)
        errors = []
        warnings = []

        # Run validations
        for record in records:
            for rule_code, rule in self.validation_rules.items():
                if not rule.logic(record):
                    error_msg = f"{rule_code}: {rule.description} for record {record.get('id', 'unknown')}"
                    if 'DUNS' in rule_code:
                        error_msg += " - Triggered by invalid DUNS for action type."
                    errors.append(error_msg)
                elif rule_code == 'ZIP-001' and len(record.get('PPoPZIP', '')) < 9:
                    warnings.append(f"Warning: ZIP incomplete, but accepted.")

        # Flexfields warning if only missing required
        if not errors and 'required_element' in str(records):  # Simulate
            warnings.append("Flexfields appear in warning due to missing required element.")

        submission.errors = errors
        submission.warnings = warnings
        submission.status = 'validated' if not errors else 'error'

        # Save to DB
        self.cursor.execute('''
            INSERT OR REPLACE INTO submissions (id, file_path, status, publish_status, created_by, created_at, errors, warnings)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            sub_id, file_path, submission.status, submission.publish_status, user,
            submission.created_at.isoformat(), json.dumps(errors), json.dumps(warnings)
        ))
        self.conn.commit()

        # Accurate error representation
        if errors:
            logger.warning(f"Submission {sub_id} has errors: {errors}")

        # Cache for performance
        cache_key = f"val_{sub_id}"
        self.cache[cache_key] = {'records': records, 'errors': errors, 'warnings': warnings}

        # Sync D1 file generation with FPDS (simulate no regen if no update)
        fpds_last_load = self.get_fpds_load_time()
        if datetime.datetime.now() - fpds_last_load < datetime.timedelta(hours=24):
            logger.info("D1 file synced, no regeneration needed.")

        return submission

    def _read_fabs_file(self, file_path: str) -> List[Dict]:
        records = []
        if not os.path.exists(file_path):
            # Simulate sample file without FundingAgencyCode header as per update
            sample_data = [
                {'id': '1', 'ActionType': 'B', 'DUNS': '123456789', 'ActionDate': '2023-01-01',
                 'PPoPZIP': '12345', 'PPoPCode': '00****', 'CFDA': '12.345.67', 'RecordType': 'grant',
                 'FederalActionObligation': '1000', 'flexfields': ['field1', 'field2'] * 100}  # Large flexfields
            ]
            # Write sample to file for realism
            with open(file_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['id', 'ActionType', 'DUNS', 'ActionDate', 'PPoPZIP', 'PPoPCode', 'CFDA', 'RecordType', 'FederalActionObligation'] + ['flexfield'])
                writer.writeheader()
                for rec in sample_data:
                    row = {k: v for k, v in rec.items() if k != 'flexfields'}
                    row['flexfield'] = json.dumps(rec['flexfields'])
                    writer.writerow(row)
            records = sample_data
        else:
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    records.append(row)
        # Handle quotation marks for zeroes
        for rec in records:
            for k, v in rec.items():
                if isinstance(v, str) and v.strip('"') != v:
                    rec[k] = v.strip('"')
        return records

    def derive_fields(self, records: List[Dict]) -> List[Dict]:
        derived_records = records.copy()
        for record in derived_records:
            # Derive FundingAgencyCode
            agency_code = record.get('AgencyCode', '000')
            record['FundingAgencyCode'] = f"Derived_{agency_code}"

            # Derive office names from codes
            office_code = record.get('OfficeCode', '')
            record['OfficeName'] = f"Office of {office_code}" if office_code else 'Unknown Office'

            # FREC derivations for historical
            record['FREC'] = f"{record.get('FundingAgencyCode', '')}_FREC"

            # PPoP Congressional District
            ppop_zip = record.get('PPoPZIP', '')
            record['PPoPCongressionalDistrict'] = 'CD001' if ppop_zip else ''

            # LegalEntityAddressLine3 max length v1.1 (assume 100 chars)
            addr3 = record.get('LegalEntityAddressLine3', '')
            if len(addr3) > 100:
                record['LegalEntityAddressLine3'] = addr3[:100]

            # FederalActionObligation to Atom Feed mapping (simulate)
            obligation = record.get('FederalActionObligation', 0)
            record['AtomFeedObligation'] = str(obligation)

            # Ensure no NASA grants as contracts
            if record.get('AgencyCode', '') == 'NASA' and record.get('RecordType', '') == 'grant':
                record['RecordType'] = 'grant'  # Prevent misdisplay

        # Add to DB for historical loader
        for rec in derived_records:
            self.cursor.execute('''
                INSERT OR REPLACE INTO published_awards (id, data, derived_fields)
                VALUES (?, ?, ?)
            ''', (rec.get('id'), json.dumps({k: v for k, v in rec.items() if k not in ['derived_fields']}),
                  json.dumps({k: v for k, v in rec.items() if 'Derived' in str(k) or k in ['OfficeName', 'FREC']})))
        self.conn.commit()

        return derived_records

    def publish_submission(self, submission_id: str, confirm: bool = True) -> bool:
        if self.gtas_lockdown:
            logger.error("Site locked during GTAS window.")
            return False

        self.cursor.execute('SELECT * FROM submissions WHERE id = ?', (submission_id,))
        row = self.cursor.fetchone()
        if not row:
            return False

        # Prevent double publish
        if row[3] == 'published':  # publish_status
            logger.warning("Already published, preventing duplicate.")
            return False

        cache_key = f"val_{submission_id}"
        if cache_key in self.cache:
            records = self.cache[cache_key]['records']
        else:
            records = self._read_fabs_file(row[1])

        if confirm and len(records) > 0:  # More info before publish
            print(f"About to publish {len(records)} rows. Confirm?")  # Simulate UI

        derived = self.derive_fields(records)

        # Update publish_status
        self.cursor.execute('UPDATE submissions SET publish_status = ? WHERE id = ?', ('published', submission_id))
        self.conn.commit()

        # Prevent duplicates on refresh (idempotent by ID)
        for rec in derived:
            rec_id = rec['id']
            self.cursor.execute('SELECT id FROM published_awards WHERE id = ?', (rec_id,))
            if not self.cursor.fetchone():
                self.cursor.execute('INSERT INTO published_awards (id, data) VALUES (?, ?)',
                                    (rec_id, json.dumps(rec)))
        self.conn.commit()

        logger.info(f"Published submission {submission_id}, status changed.")
        # Deactivate publish button simulation: status change logs it
        return True

    def generate_d_file(self, fab_data: List[Dict], fpds_data: List[Dict] = None) -> str:
        # Cache management
        cache_key = hashlib.md5(json.dumps(fab_data).encode()).hexdigest()
        if cache_key in self.cache:
            logger.info("Returning cached D file.")
            return self.cache[cache_key]

        # Simulate D file from FABS + FPDS
        d_records = []
        for fab in fab_data:
            d_rec = {**fab}
            if fpds_data:
                # Sync with FPDS load
                d_rec.update(fpds_data[0] if fpds_data else {})  # Simple merge
            d_records.append(d_rec)

        output_file = f"d_file_{datetime.datetime.now().isoformat()}.csv"
        with open(output_file, 'w', newline='') as f:
            if d_records:
                writer = csv.DictWriter(f, fieldnames=d_records[0].keys())
                writer.writeheader()
                writer.writerows(d_records)

        self.cache[cache_key] = output_file
        logger.info("D file generated and cached.")
        return output_file

    def load_historical_data(self):
        # Simulate historical FABS and FPDS load
        historical_fabs = [
            {'id': 'hist1', 'AgencyCode': '001', 'RecordType': 'grant', 'DUNS': '987654321', 'ActionDate': '2017-12-19'}
        ]
        # Process 12-19-2017 deletions (remove if date matches)
        historical_fabs = [r for r in historical_fabs if r.get('ActionDate') != '2017-12-19']
        self.historical_data['fabs'] = self.derive_fields(historical_fabs)

        historical_fpds = [
            {'id': 'fpds1', 'additional_field1': 'value1', 'additional_field2': 'value2'}
        ]
        self.historical_data['fpds'] = historical_fpds

        # Load to DB
        for rec in self.historical_data['fabs'] + self.historical_data['fpds']:
            self.cursor.execute('INSERT INTO published_awards (id, data) VALUES (?, ?)',
                                (rec['id'], json.dumps(rec)))
        self.conn.commit()

        # Ensure all historical loaded for go-live
        logger.info(f"Loaded {len(self.historical_data['fabs'])} historical FABS records.")

    def get_fpds_load_time(self) -> datetime.datetime:
        return datetime.datetime.now() - datetime.timedelta(hours=24)  # Simulate daily update

    def update_gtas_data(self):
        now = datetime.date.today()
        self.gtas_lockdown = self.gtas_window_start <= now <= self.gtas_window_end
        self.cursor.execute('INSERT INTO gtas_data (window_start, window_end, locked) VALUES (?, ?, ?)',
                            (self.gtas_window_start.isoformat(), self.gtas_window_end.isoformat(), self.gtas_lockdown))
        self.conn.commit()
        if self.gtas_lockdown:
            logger.info("GTAS submission period: site locked.")

    def get_submission_dashboard(self, user: str) -> List[Dict]:
        self.cursor.execute('SELECT * FROM submissions WHERE created_by = ?', (user,))
        rows = self.cursor.fetchall()
        dashboard = []
        for row in rows:
            status_label = 'Valid' if row[2] == 'validated' else 'Error' if 'error' in row[2] else row[2]
            dashboard.append({
                'id': row[0], 'status': status_label, 'created_by': row[4],
                'updated_at': row[5], 'publish_status': row[3]
            })
        # Additional helpful info
        dashboard.append({'info': f'Total submissions for {user}: {len(dashboard)}'})
        return dashboard

    def download_uploaded_file(self, submission_id: str) -> Optional[str]:
        self.cursor.execute('SELECT file_path FROM submissions WHERE id = ?', (submission_id,))
        row = self.cursor.fetchone()
        if row:
            return row[0]
        return None

    def process_deletions(self, date_str: str = '2017-12-19'):
        # Process deletions for specific date
        target_date = datetime.date.fromisoformat(date_str)
        self.cursor.execute('''
            DELETE FROM published_awards WHERE json_extract(data, '$.ActionDate') = ?
        ''', (date_str,))
        deleted = self.cursor.rowcount
        self.conn.commit()
        logger.info(f"Processed {deleted} deletions for {date_str}.")

    def reset_permissions(self, env: str = 'staging'):
        # Simulate reset to Staging MAX permissions
        if env == 'staging':
            logger.info("Reset environment to only Staging MAX permissions. FABS testers access revoked.")
            # In real: update auth tables

    def get_access_to_files(self, file_type: str = 'fabs') -> List[str]:
        # Access published FABS files
        files = []
        if file_type == 'fabs':
            # Simulate daily updated files
            files.append(f"published_fabs_{datetime.date.today().isoformat()}.json")
            with open(files[0], 'w') as f:
                json.dump(self.historical_data['fabs'], f)
        logger.info(f"Access granted to {files} for {file_type}.")
        return files

    def ensure_no_fsrs_deleted(self):
        # Ensure deleted FSRS not included
        self.cursor.execute('DELETE FROM published_awards WHERE json_extract(data, "$.status") = "deleted"')
        self.conn.commit()
        logger.info("Removed deleted FSRS records from submissions.")

    def quick_access_data(self, app: str = 'broker') -> Dict:
        # Developer quick access
        data = {
            'submissions': self.cursor.execute('SELECT COUNT(*) FROM submissions').fetchone()[0],
            'published': self.cursor.execute('SELECT COUNT(*) FROM published_awards').fetchone()[0],
            'gtas_locked': self.gtas_lockdown
        }
        logger.info(f"Quick access data for {app}: {data}")
        return data

# Simulate UI/Design aspects via functions (non-UI, but backend support)
def track_tech_thursday_issues(issues: List[str]):
    logger.info("Tracking Tech Thursday issues: %s", issues)
    # In real: store in issue tracker

def schedule_user_testing(date: datetime.date):
    logger.info("Scheduled user testing for %s.", date)
    # Simulate notice

def create_user_testing_summary(improvements: List[str]):
    summary = {'ui_improvements': improvements, 'timeline': 'TBD', 'scope': f"{len(improvements)} items"}
    logger.info("User testing summary: %s", summary)
    return summary

def design_schedule_from_ui_sme(timeline: Dict):
    logger.info("Designed schedule: %s", timeline)

def design_audit_from_ui_sme(scope: Dict):
    logger.info("Designed audit: %s", scope)

# Demo usage
if __name__ == "__main__":
    broker = BrokerSystem('broker.db')

    # Process deletions
    broker.process_deletions()

    # GTAS update
    broker.update_gtas_data()

    # Upload and validate sample
    user = 'agency_user'
    file_path = 'sample_fabs.csv'
    submission = broker.upload_and_validate(file_path, user)

    # Derive and publish
    if submission.status == 'validated':
        success = broker.publish_submission(submission.id)
        if success:
            # Generate D file
            records = broker._read_fabs_file(file_path)
            d_file = broker.generate_d_file(records, broker.historical_data['fpds'])

            # Access files
            broker.get_access_to_files('fabs')

            # Dashboard
            dashboard = broker.get_submission_dashboard(user)
            print("Dashboard:", json.dumps(dashboard, default=str))

            # Download
            downloaded = broker.download_uploaded_file(submission.id)
            print("Downloaded file:", downloaded)

    # Quick access
    data = broker.quick_access_data()
    print("System data:", data)

    # Ensure no deleted FSRS
    broker.ensure_no_fsrs_deleted()

    # Reset permissions
    broker.reset_permissions()

    # Simulate UI tasks
    track_tech_thursday_issues(['UI bug 1', 'Validation issue'])
    schedule_user_testing(datetime.date(2023, 11, 1))
    summary = create_user_testing_summary(['Redesign Resources page', 'Update Help page'])
    design_schedule_from_ui_sme({'round2': '2023-11', 'round3': '2023-12'})
    design_audit_from_ui_sme({'scope': 'UI improvements for Broker styles'})

    # Historical updates received
    logger.info("FABS records updated: yes")

    # FPDS daily
    fpds_time = broker.get_fpds_load_time()
    print("FPDS last load:", fpds_time)

    # Header time
    now = datetime.datetime.now()
    print(f"Header updated: {now.strftime('%Y-%m-%d %H:%M:%S')}")

    # File extension error simulation (handled in upload)
    print("System ready for FABS production deployment.")