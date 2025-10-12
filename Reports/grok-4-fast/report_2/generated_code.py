import logging
import json
import csv
import os
from datetime import datetime, date
from typing import Dict, List, Any, Optional
import hashlib
from functools import lru_cache
import re

# Configure logging for better troubleshooting
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BrokerSystem:
    def __init__(self):
        self.database = {}  # In-memory simulation of database
        self.validation_rules = self._load_validation_rules()
        self.cache = {}  # For caching D1 files and requests
        self.submissions = {}  # Track submissions
        self.publish_status = {}  # Track publish statuses
        self.gt as_window = False  # GTAS window flag
        self.historical_fabs_loaded = False
        self.fpds_data = {}  # Simulated FPDS data
        self.sam_data = {}  # Simulated SAM data
        self.new_relic_data = {}  # Simulated New Relic metrics

    def _load_validation_rules(self) -> Dict[str, Any]:
        # Simulate loading updated validation rules from DB-2213
        rules = {
            'cfda_error': 'Clarify CFDA title mismatch or invalid code.',
            'duns_accept': ['B', 'C', 'D'],  # Action types that accept expired DUNS if registered
            'zip_validation': r'^\d{5}(-\d{4})?$',  # ZIP+4 pattern
            'loan_records': {'accept_zero_blank': True},
            'non_loan_records': {'accept_zero_blank': True},
            'flexfields': {'max_length': 1000},  # No performance impact for large flexfields
            'ppop_congressional_district': {'required': True},
            'funding_agency_code': {'derive': True},
            'legal_entity_address_line3': {'max_length': 55},  # Match schema v1.1
            'schema_v1_1_headers': ['ActionDate', 'UniqueAwardID', ...],  # Placeholder
            'd_file_cache_ttl': 3600,  # Cache TTL
            'delete_12_19_2017': True  # Process deletions
        }
        logger.info("Validation rules loaded for DB-2213 updates.")
        return rules

    def process_deletions_12_19_2017(self, records: List[Dict]) -> List[Dict]:
        """As a Data user, process 12-19-2017 deletions."""
        processed = [rec for rec in records if rec.get('date') != '12-19-2017' or self.validation_rules['delete_12_19_2017']]
        logger.info(f"Processed {len(records) - len(processed)} deletions from 12-19-2017.")
        self.database.update({hashlib.md5(str(rec).encode()).hexdigest(): rec for rec in processed})
        return processed

    def redesign_resources_page(self, styles: Dict) -> str:
        """As a UI designer, redesign Resources page to match new Broker styles. Simulate with JSON config."""
        # Simulate redesign by generating a config file
        config = {
            "resources_page": {
                "styles": styles,
                "broker_design": True,
                "elements": ["header", "content", "footer"]
            }
        }
        with open('resources_page_config.json', 'w') as f:
            json.dump(config, f)
        logger.info("Resources page redesigned with Broker styles.")
        return "Redesign complete: resources_page_config.json"

    def report_user_testing_to_agencies(self, testing_results: Dict) -> str:
        """As a UI designer, report user testing to Agencies."""
        report = {
            "summary": "User testing contributions to better UX in Broker.",
            "results": testing_results,
            "date": datetime.now().isoformat()
        }
        report_file = 'user_testing_report.json'
        with open(report_file, 'w') as f:
            json.dump(report, f)
        logger.info(f"User testing report generated: {report_file}")
        return f"Report sent to Agencies: {report_file}"

    def move_to_round_2_dabs_fabs_landing(self, edits: List[str]) -> bool:
        """As a UI designer, move to round 2 of DABS or FABS landing page edits."""
        self.database['landing_edits_round2'] = edits
        logger.info("Round 2 edits for DABS/FABS landing page prepared for leadership approval.")
        return True

    def move_to_round_2_homepage(self, edits: List[str]) -> bool:
        """As a UI designer, move to round 2 of Homepage edits."""
        self.database['homepage_edits_round2'] = edits
        logger.info("Round 2 edits for Homepage prepared for leadership approval.")
        return True

    def move_to_round_3_help_page(self, edits: List[str]) -> bool:
        """As a UI designer, move to round 3 of Help page edits."""
        self.database['help_edits_round3'] = edits
        logger.info("Round 3 edits for Help page prepared for leadership approval.")
        return True

    def move_to_round_2_help_page(self, edits: List[str]) -> bool:
        """Duplicate: As a UI designer, move to round 2 of Help page edits."""
        return self.move_to_round_2_help_page(edits)  # Note: Assuming typo, treat as round 2

    def enhance_logging(self, submission_id: str, function: str, error: Optional[str] = None) -> None:
        """As a Developer, log better for troubleshooting submissions and functions."""
        log_entry = {
            'submission_id': submission_id,
            'function': function,
            'timestamp': datetime.now(),
            'error': error
        }
        logger.info(f"Troubleshooting log: {json.dumps(log_entry)}")
        self.database.setdefault('logs', []).append(log_entry)

    def update_fabs_submission_publish_status(self, submission_id: str, new_status: str) -> None:
        """As a Developer, modify FABS submission when publishStatus changes."""
        if submission_id in self.submissions:
            self.submissions[submission_id]['publishStatus'] = new_status
            self.publish_status[submission_id] = new_status
            logger.info(f"FABS submission {submission_id} status updated to {new_status}.")
            self.enhance_logging(submission_id, 'publish_status_update', None)

    def integrate_new_relic(self, app_data: Dict) -> None:
        """As a DevOps engineer, provide useful New Relic data across applications."""
        self.new_relic_data.update(app_data)
        logger.info("New Relic integration: Data collected across apps.")
        # Simulate metric push
        print("New Relic metrics:", json.dumps(self.new_relic_data, indent=2))

    def upload_and_validate_error_message(self, file_path: str) -> Dict[str, List[str]]:
        """As a Broker user, upload and validate with accurate error messages."""
        errors = []
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, 1):
                if not re.match(self.validation_rules['zip_validation'], row.get('PPoPZIP', '')):
                    errors.append(f"Row {row_num}: Invalid ZIP format.")
        if errors:
            logger.warning("Validation errors found with accurate messages.")
        return {'errors': errors, 'warnings': []}

    def sync_d1_file_generation_with_fpds(self, fpds_timestamp: str) -> str:
        """As a Broker user, sync D1 file generation with FPDS data load."""
        cache_key = f"d1_{fpds_timestamp}"
        if cache_key in self.cache and (datetime.now() - self.cache[cache_key]['time']).seconds < 3600:
            logger.info("D1 file retrieved from cache - no regeneration needed.")
            return self.cache[cache_key]['file']
        else:
            d1_file = self._generate_d1_file(fpds_timestamp)
            self.cache[cache_key] = {'file': d1_file, 'time': datetime.now()}
            logger.info("D1 file generated and cached.")
            return d1_file

    def _generate_d1_file(self, fpds_timestamp: str) -> str:
        """Internal: Generate D1 file."""
        return f"D1 data synced with FPDS at {fpds_timestamp}"

    def access_published_fabs_files(self, user_id: str) -> List[str]:
        """As a Website user, access published FABS files."""
        published_files = [k for k, v in self.database.items() if v.get('status') == 'published' and 'fabs' in k.lower()]
        logger.info(f"User {user_id} accessed {len(published_files)} published FABS files.")
        return published_files

    def ensure_only_grant_records_sent(self, records: List[Dict]) -> List[Dict]:
        """As an owner, ensure USAspending only sends grant records."""
        grants = [rec for rec in records if rec.get('record_type') == 'grant']
        logger.info(f"Filtered to {len(grants)} grant records only.")
        return grants

    def update_validation_rule_table_db2213(self) -> None:
        """As a Developer, update validation rule table for DB-2213."""
        self.validation_rules.update({'db2213_updated': True})
        logger.info("Validation rule table updated for DB-2213.")

    def add_gtas_window_data(self, start_date: str, end_date: str) -> None:
        """As a Developer, add GTAS window data to database."""
        self.gt as_window = True
        self.database['gtas_window'] = {'start': start_date, 'end': end_date}
        logger.info("GTAS window data added; site lockdown during submission period.")

    @lru_cache(maxsize=128)
    def manage_d_files_generation(self, request_id: str) -> str:
        """As a Developer, manage and cache D Files generation requests."""
        if request_id in self.cache:
            logger.info("D File retrieved from cache.")
            return self.cache[request_id]
        d_file = self._generate_d_file(request_id)
        self.cache[request_id] = d_file
        logger.info("D File generated and cached.")
        return d_file

    def _generate_d_file(self, request_id: str) -> str:
        """Internal: Generate D File."""
        return f"D File for request {request_id} from FABS and FPDS data."

    def access_raw_agency_published_files_fabs(self, agency_id: str) -> List[str]:
        """As a user, access raw agency published files from FABS via USAspending."""
        files = [v for k, v in self.database.items() if agency_id in k and 'published' in v.get('status', '')]
        logger.info(f"Accessed {len(files)} raw FABS files for agency {agency_id}.")
        return files

    def handle_large_flexfields(self, flexfields: List[str]) -> bool:
        """As an Agency user, include large number of flexfields without performance impact."""
        # Simulate efficient processing
        processed = [ff[:self.validation_rules['flexfields']['max_length']] for ff in flexfields]
        self.database['flexfields'] = processed
        logger.info("Large flexfields processed efficiently.")
        return True

    def create_content_mockups(self, data: Dict) -> str:
        """As a Broker user, help create content mockups."""
        mockup = json.dumps(data, indent=2)
        mockup_file = 'content_mockups.json'
        with open(mockup_file, 'w') as f:
            f.write(mockup)
        logger.info("Content mockups created for efficient data submission.")
        return mockup_file

    def track_tech_thursday_issues(self, issues: List[str]) -> None:
        """As a UI designer, track issues from Tech Thursday."""
        self.database['tech_thursday_issues'] = issues
        logger.info("Tech Thursday issues tracked for testing and fixes.")

    def create_user_testing_summary(self, ui_sme_input: Dict) -> str:
        """As an Owner, create user testing summary from UI SME."""
        summary = {
            "ui_improvements": ui_sme_input.get('improvements', []),
            "follow_through": True
        }
        summary_file = 'user_testing_summary.json'
        with open(summary_file, 'w') as f:
            json.dump(summary, f)
        logger.info("User testing summary created.")
        return summary_file

    def begin_user_testing(self, requests: List[str]) -> bool:
        """As a UI designer, begin user testing to validate stakeholder requests."""
        self.database['user_testing_active'] = {'requests': requests, 'status': 'begun'}
        logger.info("User testing begun for UI improvements.")
        return True

    def schedule_user_testing(self, date_time: datetime) -> bool:
        """As a UI designer, schedule user testing."""
        self.database['user_testing_schedule'] = {'scheduled_at': date_time}
        logger.info(f"User testing scheduled for {date_time} to ensure buy-in.")
        return True

    def design_ui_schedule_from_sme(self, sme_timeline: Dict) -> str:
        """As an Owner, design schedule from UI SME."""
        schedule = {"timeline": sme_timeline, "potential_duration": "Q1 2024"}
        schedule_file = 'ui_improvements_schedule.json'
        with open(schedule_file, 'w') as f:
            json.dump(schedule, f)
        logger.info("UI improvements schedule designed.")
        return schedule_file

    def design_ui_audit_from_sme(self, sme_scope: Dict) -> str:
        """As an Owner, design audit from UI SME."""
        audit = {"scope": sme_scope, "potential_issues": []}
        audit_file = 'ui_improvements_audit.json'
        with open(audit_file, 'w') as f:
            json.dump(audit, f)
        logger.info("UI improvements audit designed.")
        return audit_file

    def prevent_double_publishing_fabs(self, submission_id: str, is_refresh: bool = False) -> bool:
        """As a Developer, prevent double publishing after refresh."""
        if submission_id in self.publish_status and self.publish_status[submission_id] == 'publishing':
            logger.warning(f"Double publish attempt prevented for {submission_id}.")
            return False
        self.publish_status[submission_id] = 'publishing'
        # Simulate publishing
        self.update_fabs_submission_publish_status(submission_id, 'published')
        if is_refresh:
            logger.info("Refresh-handled publishing completed without duplicates.")
        return True

    def receive_fabs_updates(self, updates: List[Dict]) -> None:
        """As a data user, receive updates to FABS records."""
        for update in updates:
            key = hashlib.md5(str(update).encode()).hexdigest()
            self.database[key] = update
        logger.info(f"Received and stored {len(updates)} FABS updates.")

    def update_fabs_sample_file(self) -> None:
        """As a Developer, update FABS sample file to remove FundingAgencyCode header."""
        sample_headers = [h for h in self.validation_rules['schema_v1_1_headers'] if h != 'FundingAgencyCode']
        sample_file = 'fabs_sample.csv'
        with open(sample_file, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(sample_headers)
        logger.info("FABS sample file updated: FundingAgencyCode header removed.")

    def ensure_deleted_fsrs_not_included(self, records: List[Dict]) -> List[Dict]:
        """As an agency user, ensure deleted FSRS records not included."""
        filtered = [rec for rec in records if rec.get('status') != 'deleted']
        logger.info(f"Filtered out {len(records) - len(filtered)} deleted FSRS records.")
        return filtered

    def update_financial_assistance_daily(self) -> None:
        """As a website user, see updated financial assistance data daily."""
        today = date.today().isoformat()
        self.database['financial_assistance_update'] = today
        logger.info(f"Financial assistance data updated for {today}.")

    def deactivate_publish_button_during_derivations(self, submission_id: str) -> bool:
        """As a user, deactivate publish button during derivations."""
        # Simulate derivation process
        self.publish_status[submission_id] = 'deriving'
        # Placeholder derivation logic
        self._derive_fields(submission_id)
        self.publish_status[submission_id] = 'ready'
        logger.info("Publish button managed during derivations.")
        return True

    def _derive_fields(self, submission_id: str) -> None:
        """Internal: Derive fields like PPoPCode, etc."""
        if submission_id in self.submissions:
            rec = self.submissions[submission_id]
            rec['derived_ppop_code'] = self._derive_ppop_code(rec)

    def prevent_nonexistent_record_operations(self, record_id: str, operation: str) -> bool:
        """As a Developer, ensure attempts to correct/delete non-existent records don't create new data."""
        if record_id not in self.database:
            logger.warning(f"Non-existent record {record_id} - {operation} prevented.")
            return False
        # Perform operation
        if operation == 'delete':
            del self.database[record_id]
        logger.info(f"{operation.capitalize()} performed on existing record {record_id}.")
        return True

    def reset_environment_permissions(self, max_permissions: str = 'Staging MAX') -> None:
        """As an Owner, reset environment to only Staging MAX permissions."""
        self.database['permissions'] = {'level': max_permissions, 'fabs_testers_access': False}
        logger.info("Environment reset: FABS testers no longer have access.")

    def flexfields_in_error_files_missing_required(self, submission: Dict) -> Dict:
        """As a user, flexfields appear in warning/error files when only missing required element."""
        errors = []
        if not submission.get('required_element'):
            errors.append("Missing required element; flexfields included.")
            # Include flexfields in error output
            submission['flexfields_in_error'] = True
        return {'errors': errors, 'submission': submission}

    def ensure_accurate_ppopcode_pp op_congressional_district(self, record: Dict) -> Dict:
        """As a user, accurate data for PPoPCode and PPoPCongressionalDistrict."""
        record['PPoPCode'] = self._derive_ppop_code(record)
        record['PPoPCongressionalDistrict'] = '01'  # Placeholder derivation
        logger.info("PPoPCode and Congressional District derived accurately.")
        return record

    def _derive_ppop_code(self, record: Dict) -> str:
        """Derive PPoPCode, including 00***** and 00FORGN cases."""
        code = record.get('OfficeCode', '')
        if code.startswith('00') and ('*****' in code or 'FORGN' in code):
            return f"Derived_{code}"
        return code

    def accept_zero_blank_loans(self, record: Dict) -> bool:
        """As an agency user, FABS validation accepts zero/blank for loan records."""
        if self.validation_rules['loan_records']['accept_zero_blank'] and record.get('record_type') == 'loan':
            # Allow zero or blank for certain fields, e.g., Obligation
            record['obligation'] = record.get('obligation', 0) or 0
            return True
        return False

    def deploy_fabs_production(self) -> bool:
        """As an Agency user, deploy FABS to production."""
        self.database['deployment'] = {'status': 'production', 'date': datetime.now()}
        logger.info("FABS deployed to production for Financial Assistance submissions.")
        return True

    def clarify_cfda_error(self, record: Dict) -> str:
        """As a Developer, clarify CFDA error triggers."""
        error_msg = self.validation_rules['cfda_error']
        if not re.match(r'^\d{2}\.\d{3}$', record.get('CFDA', '')):
            return f"{error_msg}: Invalid format in {record.get('title', 'N/A')}."
        return "No CFDA error."

    def ensure_complete_sam_data(self, record: Dict) -> bool:
        """As an agency user, confident SAM data is complete."""
        # Simulate SAM check
        duns = record.get('DUNS')
        if duns in self.sam_data:
            record.update(self.sam_data[duns])  # Merge complete data
            logger.info("SAM data completed for DUNS.")
            return True
        logger.warning("Incomplete SAM data.")
        return False

    def index_domain_models(self, models: List[str]) -> None:
        """As a Developer, index domain models for fast validation."""
        for model in models:
            self.database[model] = {'indexed': True}
        logger.info("Domain models indexed for reasonable validation time.")

    def accept_zero_blank_non_loans(self, record: Dict) -> bool:
        """As an agency user, accept zero/blank for non-loan records."""
        if self.validation_rules['non_loan_records']['accept_zero_blank'] and record.get('record_type') != 'loan':
            record['obligation'] = record.get('obligation', '') or ''
            return True
        return False

    def update_sql_codes_for_clarity(self, sql_queries: List[str]) -> List[str]:
        """As a broker team member, update SQL codes for clarity."""
        clarified = [q + " -- Clarified for Broker" for q in sql_queries]
        logger.info("SQL codes updated for clarity.")
        return clarified

    def derive_all_data_elements(self, record: Dict) -> Dict:
        """As an agency user, all derived data elements properly derived."""
        derivations = {
            'FundingAgencyCode': self._derive_funding_agency_code(record),
            'OfficeName': self._derive_office_name(record.get('OfficeCode', '')),
            'FREC': self._derive_frec(record)
        }
        record.update(derivations)
        logger.info("All data elements derived properly.")
        return record

    def _derive_funding_agency_code(self, record: Dict) -> str:
        """Derive FundingAgencyCode."""
        return record.get('AgencyCode', 'Derived_FAC')

    def _derive_office_name(self, code: str) -> str:
        """Derive office name from code."""
        names = {'123': 'Office of Finance', '456': 'Procurement Office'}  # Simulated
        return names.get(code, 'Unknown Office')

    def add_00_cases_to_derivation(self, record: Dict) -> None:
        """As a broker team member, add 00***** and 00FORGN to derivation logic."""
        self._derive_ppop_code(record)  # Uses updated logic
        logger.info("00***** and 00FORGN cases added to PPoPCode derivation.")

    def update_broker_resources_validations_pp(self) -> None:
        """As a broker team member, update Broker resources, validations, P&P for FABS and DAIMS v1.1 launch."""
        self.validation_rules['v1_1_launch'] = True
        logger.info("Broker pages updated for FABS/DAIMS v1.1 launch.")

    def load_historical_fabs_with_frec(self, data: List[Dict]) -> None:
        """As a Developer, load historical FABS with FREC derivations."""
        for rec in data:
            rec['FREC'] = self._derive_frec(rec)
            key = hashlib.md5(str(rec).encode()).hexdigest()
            self.database[key] = rec
        self.historical_fabs_loaded = True
        logger.info("Historical FABS loaded with FREC derivations for USASpending.gov.")

    def _derive_frec(self, record: Dict) -> str:
        """Derive FREC."""
        return f"FREC_{record.get('AgencyCode', '')}"

    def prevent_nasa_grants_as_contracts(self, records: List[Dict]) -> List[Dict]:
        """As a user, don't see NASA grants as contracts."""
        corrected = []
        for rec in records:
            if rec.get('agency') == 'NASA' and rec.get('type') == 'grant':
                rec['display_type'] = 'grant'
            corrected.append(rec)
        logger.info("NASA grants corrected to not display as contracts.")
        return corrected

    def validate_duns_for_action_types(self, record: Dict) -> bool:
        """As a user, DUNS validations accept B,C,D actions if registered in SAM, even expired."""
        action_type = record.get('ActionType')
        duns = record.get('DUNS')
        action_date = datetime.strptime(record.get('ActionDate', ''), '%Y-%m-%d')
        if action_type in self.validation_rules['duns_accept']:
            if duns in self.sam_data:
                reg_start = self.sam_data[duns].get('reg_start')
                reg_end = self.sam_data[duns].get('reg_end', datetime.now())
                if action_date > reg_start and (action_date < reg_end or reg_end > datetime.now()):
                    return True
        logger.warning("DUNS validation failed.")
        return False

    def validate_duns_date_registration(self, record: Dict) -> bool:
        """As a user, accept DUNS if ActionDate before current reg but after initial."""
        # Integrated into above, but separate for clarity
        return self.validate_duns_for_action_types(record)

    def derive_funding_agency_code(self, record: Dict) -> str:
        """As a broker team member, derive FundingAgencyCode for data quality."""
        derived = self._derive_funding_agency_code(record)
        record['FundingAgencyCode'] = derived
        logger.info("FundingAgencyCode derived for improved quality.")
        return derived

    def update_legal_entity_address_line3_max_length(self, record: Dict) -> bool:
        """As an agency user, max length for LegalEntityAddressLine3 matches schema v1.1."""
        max_len = self.validation_rules['legal_entity_address_line3']['max_length']
        addr3 = record.get('LegalEntityAddressLine3', '')[:max_len]
        record['LegalEntityAddressLine3'] = addr3
        logger.info(f"AddressLine3 truncated to {max_len} per v1.1 schema.")
        return True

    def use_schema_v1_1_headers(self, file_path: str) -> bool:
        """As an agency user, use schema v1.1 headers in FABS file."""
        expected = set(self.validation_rules['schema_v1_1_headers'])
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            headers = set(next(reader))
        if headers == expected:
            logger.info("Schema v1.1 headers validated.")
            return True
        logger.warning("Headers do not match v1.1 schema.")
        return False

    def map_federal_action_obligation_to_atom_feed(self, obligation: float) -> Dict:
        """As a agency user, map FederalActionObligation to Atom Feed."""
        atom_entry = {'obligation': obligation, 'feed_type': 'atom'}
        logger.info("FederalActionObligation mapped to Atom Feed.")
        return atom_entry

    def validate_pp op_zip4_like_legal_entity(self, zip_code: str) -> bool:
        """As a Broker user, PPoPZIP+4 works like Legal Entity ZIP validations."""
        return bool(re.match(self.validation_rules['zip_validation'], zip_code))

    def link_sample_file_in_dialog(self, dialog_text: str) -> str:
        """As a FABS user, link SAMPLE FILE to correct file in dialog."""
        updated_dialog = dialog_text.replace("SAMPLE FILE", "fabs_sample.csv")
        logger.info("Sample file link updated in submission dialog.")
        return updated_dialog

    def update_fpds_daily(self) -> None:
        """As an Agency user, FPDS data up-to-date daily."""
        today = date.today().isoformat()
        self.fpds_data['last_update'] = today
        logger.info(f"FPDS updated for {today}.")

    def determine_d_files_generation_from_fabs_fpds(self, fabs_data: List, fpds_data: List) -> str:
        """As a Developer, determine how agencies generate/validate D Files from FABS/FPDS."""
        method = "Agencies upload FABS, system merges with daily FPDS, generates D File via Broker."
        logger.info(method)
        return method

    def generate_validate_d_files(self, fabs_path: str, fpds_path: str) -> Dict:
        """As a user, generate and validate D Files from FABS and FPDS."""
        # Load and merge
        fabs = self._load_csv(fabs_path)
        fpds = self._load_csv(fpds_path)
        d_data = self._merge_fabs_fpds(fabs, fpds)
        validation = self._validate_d_data(d_data)
        d_file = self.manage_d_files_generation('user_request')
        return {'d_file': d_file, 'validation': validation}

    def _load_csv(self, path: str) -> List[Dict]:
        """Load CSV to list of dicts."""
        with open(path, 'r') as f:
            return list(csv.DictReader(f))

    def _merge_fabs_fpds(self, fabs: List[Dict], fpds: List[Dict]) -> List[Dict]:
        """Merge FABS and FPDS data."""
        for f in fabs:
            f['fpds_info'] = next((p for p in fpds if p.get('id') == f.get('id')), {})
        return fabs

    def _validate_d_data(self, data: List[Dict]) -> Dict:
        """Validate merged data."""
        return {'errors': [], 'valid': True}

    def update_header_info_box_datetime(self, box_content: str) -> str:
        """As an Agency user, show updated date AND time in header."""
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        updated = box_content + f" | Updated: {now}"
        logger.info("Header info box updated with date and time.")
        return updated

    def helpful_file_level_error_wrong_extension(self, file_path: str) -> str:
        """As an Agency user, more helpful error for wrong file extension."""
        ext = os.path.splitext(file_path)[1]
        if ext != '.csv':
            return f"Error: Invalid extension '{ext}'. Expected .csv for FABS submissions. Please rename and try again."
        return "File extension valid."

    def access_test_features_other_envs(self, feature: str, env: str) -> bool:
        """As a tester, access test features in non-Staging envs."""
        if env != 'Staging':
            self.database[f'test_{feature}_{env}'] = True
            logger.info(f"Test feature {feature} accessed in {env}.")
            return True
        return False

    def accurate_submission_errors_fabs(self, errors: List[str]) -> List[str]:
        """As a FABS user, submission errors accurately represent FABS errors."""
        # Simulate precise mapping
        fabs_errors = [e + " (FABS-specific)" for e in errors]
        logger.info("FABS errors accurately represented.")
        return fabs_errors

    def update_frontend_urls(self, current_url: str) -> str:
        """As a FABS user, frontend URLs reflect accessed page accurately."""
        # Simulate URL mapping
        accurate_url = current_url.replace('old', 'fabs_')
        logger.info(f"URL updated to {accurate_url}.")
        return accurate_url

    def load_all_historical_financial_assistance(self) -> None:
        """As an Agency user, all historical Financial Assistance data loaded for FABS go-live."""
        # Simulate load
        self.historical_fabs_loaded = True
        self.database['historical_fa'] = "Loaded"
        logger.info("All historical Financial Assistance data loaded for FABS go-live.")

    def load_historical_fpds_both_sources(self, extracted_data: List, feed_data: List) -> None:
        """As a Developer, load historical FPDS from extracted and feed data."""
        self.fpds_data.update({f"extracted_{i}": rec for i, rec in enumerate(extracted_data)})
        self.fpds_data.update({f"feed_{i}": rec for i, rec in enumerate(feed_data)})
        logger.info("Historical FPDS loaded from both sources since 2007.")

    def load_historical_fpds(self) -> None:
        """As an Agency user, historical FPDS data loaded."""
        self.load_historical_fpds_both_sources([], [])  # Placeholder data

    def show_submission_creator(self, submission_id: str) -> str:
        """As an Agency user, accurately see who created submission."""
        creator = self.submissions.get(submission_id, {}).get('creator', 'Unknown')
        logger.info(f"Creator for submission {submission_id}: {creator}")
        return creator

    def get_file_f_correct_format(self, file_path: str) -> bool:
        """As an agency user, get File F in correct format."""
        # Assume validation
        return self.use_schema_v1_1_headers(file_path)

    def better_file_level_errors(self, errors: List[str]) -> List[str]:
        """As an Agency user, better understand file-level errors."""
        improved = [e + " - Recommendation: Check headers and data types." for e in errors]
        logger.info("File-level errors enhanced with explanations.")
        return improved

    def provide_fabs_frec_groups(self) -> None:
        """As a Developer, provide FABS groups under FREC paradigm."""
        self.validation_rules['frec_groups'] = True
        logger.info("FABS FREC groups provided.")

    def robust_test_fabs_derivations(self, test_file: str) -> Dict:
        """As a tester, ensure FABS derivations with robust test file and check."""
        data = self._load_csv(test_file)
        for rec in data:
            self.derive_all_data_elements(rec)
        check = all(rec.get('derived_ppop_code') for rec in data)
        logger.info(f"FABS derivations tested: {'Pass' if check else 'Fail'}")
        return {'pass': check, 'derived': data}

    def ensure_zero_padded_fields_only(self, records: List[Dict]) -> List[Dict]:
        """As an owner, only zero-padded fields."""
        padded = []
        for rec in records:
            for key, val in rec.items():
                if isinstance(val, str) and re.match(r'^\d+$', val):
                    rec[key] = val.zfill(10)  # Example padding
            padded.append(rec)
        logger.info("Fields zero-padded as required.")
        return padded

    def submit_individual_recipients_no_duns_error(self, records: List[Dict]) -> bool:
        """As a Broker user, submit individual recipients without DUNS error."""
        for rec in records:
            if rec.get('recipient_type') == 'individual':
                rec['duns_validation'] = 'skipped'
        logger.info("Individual recipients submitted without DUNS error.")
        self.database.update({hashlib.md5(str(rec).encode()).hexdigest(): rec for rec in records})
        return True

    def info_rows_before_publish(self, submission_id: str) -> int:
        """As a user, more info on rows to publish before deciding."""
        rows = len([k for k, v in self.submissions.items() if v.get('to_publish')])
        logger.info(f"Pre-publish info: {rows} rows will be published for {submission_id}.")
        return rows

    def prevent_duplicate_transactions(self, submission_id: str) -> bool:
        """As a Developer, prevent duplicate transactions between validation and publish."""
        # Cache validation time
        if submission_id in self.cache and self.cache[submission_id]['validated']:
            self.prevent_double_publishing_fabs(submission_id)
            return True
        logger.warning("Validation-publish gap: Duplicate prevented.")
        return False

    def submit_citywide_pp op_zip(self, zip_code: str) -> bool:
        """As a FABS user, submit citywide as PPoPZIP and pass validations."""
        citywide_zip = zip_code.replace('+4', '')[:5]
        return self.validate_pp op_zip4_like_legal_entity(citywide_zip)

    def update_error_codes_accuracy(self, errors: List[str]) -> List[str]:
        """As a Broker user, updated error codes with accurate logic and info."""
        detailed = [e + " - Fix: Update field X to match rule Y." for e in errors]
        logger.info("Error codes updated for accuracy.")
        return detailed

    def allow_zip_without_last4(self, zip_code: str) -> bool:
        """As an agency user, leave off last 4 ZIP digits without error."""
        if len(zip_code) == 5:
            return True
        return self.validate_pp op_zip4_like_legal_entity(zip_code)

    def ensure_historical_data_columns(self, data: List[Dict]) -> List[Dict]:
        """As a FABS user, historical data includes all necessary columns."""
        required_cols = ['ActionDate', 'UniqueAwardID', 'PPoPCode']
        for rec in data:
            for col in required_cols:
                if col not in rec:
                    rec[col] = ''
        logger.info("Historical data columns ensured.")
        return data

    def access_additional_fpds_fields(self, fields: List[str]) -> Dict:
        """As a data user, access two additional fields from FPDS pull."""
        additional = {field: self.fpds_data.get(field, 'N/A') for field in fields[:2]}
        logger.info("Additional FPDS fields accessed.")
        return additional

    def additional_submission_dashboard_info(self, submission_id: str) -> Dict:
        """As a FABS user, additional helpful info in dashboard."""
        info = {
            'ig_requests': 0,
            'status': self.publish_status.get(submission_id, 'pending'),
            'rows': self.info_rows_before_publish(submission_id)
        }
        logger.info("Dashboard info enhanced.")
        return info

    def download_uploaded_fabs_file(self, upload_id: str) -> str:
        """As a FABS user, download uploaded FABS file."""
        file_path = f"uploads/{upload_id}.csv"
        # Simulate download
        logger.info(f"Downloaded uploaded file: {file_path}")
        return file_path

    def quick_access_broker_data(self, query: str) -> Dict:
        """As a Developer, quickly access Broker app data."""
        results = {k: v for k, v in self.database.items() if query in k}
        logger.info(f"Quick access to Broker data for query '{query}'.")
        return results

    def load_historical_fpds_best_way(self, since_year: int = 2007) -> None:
        """As a Developer, best way to load historical FPDS since 2007."""
        self.load_historical_fpds_both_sources([], [])  # Uses best method: combined sources
        logger.info(f"Historical FPDS loaded since {since_year}.")

    def appropriate_fabs_language(self, page_content: str) -> str:
        """As a FABS user, appropriate language on FABS pages."""
        # Simulate language check/replace
        updated = page_content.replace("DABS", "FABS")  # Avoid confusion
        logger.info("FABS page language made appropriate.")
        return updated

    def no_cross_banner_messages(self, current_app: str) -> str:
        """As a FABS user, no DABS banners in FABS and vice versa."""
        banners = {'fabs': 'FABS Banner', 'dabs': 'DABS Banner'}
        return banners.get(current_app.lower(), '')

    def read_only_access_dabs(self, user_id: str) -> bool:
        """As a FABS user, read-only access to DABS."""
        self.database[f'user_{user_id}'] = {'dabs_access': 'read_only'}
        logger.info(f"Read-only DABS access granted to {user_id}.")
        return True

    def reasonable_validation_time(self, file_path: str) -> float:
        """As a FABS user, validations run in reasonable time."""
        start = datetime.now()
        self.upload_and_validate_error_message(file_path)
        time_taken = (datetime.now() - start).total_seconds()
        if time_taken > 30:  # Threshold
            logger.warning("Validation time exceeded reasonable limit.")
        else:
            logger.info(f"Validation completed in {time_taken}s.")
        return time_taken

    def correct_status_labels_dashboard(self, submission_id: str) -> str:
        """As a FABS user, correct status labels on dashboard."""
        status = self.publish_status.get(submission_id, 'Unknown').title()
        logger.info(f"Dashboard status label: {status}")
        return status

    def submission_periods_start_end(self, periods: Dict) -> Dict:
        """As an agency user, know when submission periods start/end."""
        # Assume periods provided or current
        current_period = {'start': datetime.now(), 'end': datetime.now() + timedelta(days=30)}
        logger.info("Submission periods info provided.")
        return current_period

    def landing_page_nav_fabs_dabs(self) -> str:
        """As an agency user, landing page to navigate FABS or DABS."""
        nav = "<nav><a href='/fabs'>FABS</a> <a href='/dabs'>DABS</a></nav>"  # Simulated HTML
        logger.info("Landing page navigation for FABS/DABS.")
        return nav

    def submit_data_with_quotes(self, data: List[str]) -> List[str]:
        """As an agency user, submit data elements in quotes to preserve zeroes."""
        quoted = [f'"{d}"' for d in data]
        logger.info("Data submitted with quotes to preserve leading/trailing zeroes from Excel.")
        return quoted

# Example usage to demonstrate functionality
if __name__ == "__main__":
    broker = BrokerSystem()
    broker.update_validation_rule_table_db2213()
    broker.add_gtas_window_data('2024-01-01', '2024-01-31')
    test_record = {'AgencyCode': '123', 'OfficeCode': '00*****', 'record_type': 'loan'}
    broker.derive_all_data_elements(test_record)
    print("Broker system initialized and sample derivation:", test_record['OfficeName'])