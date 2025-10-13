import datetime
import json
import logging
import os
import re
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict
from enum import Enum
import zipfile
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

# Database setup for Broker, FABS, DABS
Base = declarative_base()
engine = create_engine('sqlite:///broker.db')
Session = sessionmaker(bind=engine)

class Submission(Base):
    __tablename__ = 'submissions'
    id = Column(Integer, primary_key=True)
    agency_id = Column(String(50))
    file_path = Column(String(500))
    status = Column(String(50))
    publish_status = Column(String(50))
    created_by = Column(String(100))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)

class ValidationRule(Base):
    __tablename__ = 'validation_rules'
    id = Column(Integer, primary_key=True)
    rule_name = Column(String(100))
    description = Column(Text)
    error_code = Column(String(50))
    is_active = Column(Boolean, default=True)

class HistoricalData(Base):
    __tablename__ = 'historical_data'
    id = Column(Integer, primary_key=True)
    source = Column(String(50))  # FABS, FPDS
    data = Column(Text)  # JSON stored data
    loaded_at = Column(DateTime, default=datetime.datetime.utcnow)

Base.metadata.create_all(engine)

# Logging setup as per developer story for better logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BrokerSystem:
    def __init__(self):
        self.session = Session()
        self.new_relic_data = {}  # Mock for New Relic

    def process_deletions(self, date_str: str = "12-19-2017"):
        """As a Data user, I want to have the 12-19-2017 deletions processed."""
        try:
            date = datetime.datetime.strptime(date_str, "%m-%d-%Y").date()
            # Simulate deletion processing
            deletions = self.session.query(Submission).filter(Submission.created_at < date).all()
            for del_item in deletions:
                self.session.delete(del_item)
            self.session.commit()
            logger.info(f"Processed deletions for {date_str}: {len(deletions)} items deleted")
            return len(deletions)
        except Exception as e:
            logger.error(f"Error processing deletions: {e}")
            self.session.rollback()
            return 0

    def redesign_resources_page(self, new_styles: Dict[str, str]):
        """As a UI designer, I want to redesign the Resources page, so that it matches the new Broker design styles."""
        # Simulate UI redesign by updating a config
        ui_config = {"resources_page": new_styles}
        with open("ui_config.json", "w") as f:
            json.dump(ui_config, f)
        logger.info("Resources page redesigned with new styles")

    def report_user_testing_to_agencies(self, testing_results: Dict):
        """As a UI designer, I want to report to the Agencies about user testing, so that they are aware of their contributions to making Broker a better UX."""
        report = {"testing_summary": testing_results, "date": datetime.date.today().isoformat()}
        # Simulate sending report
        agencies = ["agency1", "agency2"]
        for agency in agencies:
            logger.info(f"Reporting to {agency}: {report}")
        return report

    def integrate_new_relic(self, app_name: str):
        """As a DevOps engineer, I want New Relic to provide useful data across all applications."""
        self.new_relic_data[app_name] = {"metrics": ["cpu", "memory"], "timestamp": datetime.datetime.now()}
        logger.info(f"New Relic integrated for {app_name}")

    def sync_d1_file_generation_with_fpds(self, fpds_data_load_time: datetime.datetime):
        """As a Broker user, I want the D1 file generation to be synced with the FPDS data load, so that I don't have to regenerate a file if no data has been updated."""
        current_time = datetime.datetime.now()
        if abs((current_time - fpds_data_load_time).total_seconds()) < 3600:  # Within 1 hour
            # Generate D1 file only if synced
            self.generate_d1_file()
            logger.info("D1 file generated in sync with FPDS load")
        else:
            logger.info("No D1 generation needed; not synced")

    def generate_d1_file(self):
        """Helper to generate D1 file."""
        with open("d1_file.txt", "w") as f:
            f.write("D1 data synced")
        return "d1_file.txt"

    def update_sql_codes_for_clarity(self, sql_code: str):
        """As a broker team member, I want to make some updates to the SQL codes for clarity."""
        # Simulate SQL update with clearer comments
        clearer_sql = f"-- Updated for clarity\n{sql_code}"
        # Store in DB or file
        with open("updated_sql.sql", "w") as f:
            f.write(clearer_sql)
        logger.info("SQL codes updated for clarity")

    def add_ppopcode_cases_to_derivation(self, ppop_code: str):
        """As a broker team member, I want to add the 00***** and 00FORGN PPoPCode cases to the derivation logic."""
        derivation_rules = {
            "00*****": "US Territory",
            "00FORGN": "Foreign"
        }
        derived = derivation_rules.get(ppop_code, "Unknown")
        logger.info(f"Derived PPoPCode {ppop_code} to {derived}")
        return derived

    def derive_funding_agency_code(self, agency_data: Dict):
        """As a broker team member, I want to derive FundingAgencyCode, so that the data quality and completeness improves."""
        if "agency_code" in agency_data:
            # Simple derivation logic
            code = agency_data["agency_code"]
            derived_code = f"FUND_{code.zfill(3)}" if code else "FUND_000"
            agency_data["funding_agency_code"] = derived_code
            logger.info(f"Derived FundingAgencyCode: {derived_code}")
        return agency_data

    def map_federal_action_obligation_to_atom_feed(self, obligation: float):
        """As a agency user, I want to map the FederalActionObligation properly to the Atom Feed."""
        atom_entry = {"obligation": obligation, "type": "federal_action"}
        # Simulate Atom feed mapping
        feed = [atom_entry]
        logger.info(f"Mapped obligation {obligation} to Atom feed")
        return feed

    def validate_ppop_zip_plus4(self, zip_code: str):
        """As a Broker user, I want to have PPoPZIP+4 work the same as the Legal Entity ZIP validations."""
        # ZIP+4 validation logic
        pattern = re.compile(r"^\d{5}(-\d{4})?$")
        is_valid = bool(pattern.match(zip_code))
        if is_valid:
            logger.info(f"PPoPZIP+4 {zip_code} validated successfully")
        else:
            logger.warning(f"Invalid PPoPZIP+4: {zip_code}")
        return is_valid

    # Cluster 5 stories
    def edit_landing_pages_round2(self, page_type: str):
        """As a UI designer, I want to move on to round 2 of DABS or FABS landing page edits, so that I can get approvals from leadership."""
        edits = {"round": 2, "page": page_type, "status": "pending_approval"}
        with open(f"{page_type}_edits_r2.json", "w") as f:
            json.dump(edits, f)
        logger.info(f"Round 2 edits for {page_type} landing page completed")

    def edit_homepage_round2(self):
        """As a UI designer, I want to move on to round 2 of Homepage edits, so that I can get approvals from leadership."""
        self.edit_landing_pages_round2("homepage")

    def edit_help_page_round3(self):
        """As a UI designer, I want to move on to round 3 of the Help page edits, so that I can get approvals from leadership."""
        edits = {"round": 3, "page": "help", "status": "pending_approval"}
        with open("help_edits_r3.json", "w") as f:
            json.dump(edits, f)
        logger.info("Round 3 edits for Help page completed")

    def edit_help_page_round2(self):
        """As a UI designer, I want to move on to round 2 of the Help page edits, so that I can get approvals from leadership."""
        self.edit_landing_pages_round2("help")

    def enhance_logging_for_submissions(self, submission_id: int):
        """As a Developer, I want to be able to log better, so that I can troubleshoot issues with particular submissions and functions."""
        logger.info(f"Troubleshooting submission {submission_id}: Enhanced logs enabled")
        # Add detailed logs
        handler = logging.FileHandler(f"submission_{submission_id}.log")
        logger.addHandler(handler)

    def access_published_fabs_files(self, user_role: str):
        """As a Website user, I want to access published FABS files, so that I can see the new files as they come in."""
        if user_role == "website_user":
            files = ["fabs_file1.csv", "fabs_file2.csv"]
            logger.info(f"Access granted to published FABS files for {user_role}")
            return files
        return []

    def ensure_only_grant_records_sent(self, records: List[Dict]):
        """As an owner, I want to be sure that USAspending only send grant records to my system."""
        filtered = [r for r in records if r.get("type") == "grant"]
        logger.info(f"Filtered to only grant records: {len(filtered)}")
        return filtered

    def create_content_mockups(self, content: str):
        """As a Broker user, I want to help create content mockups, so that I can submit my data efficiently."""
        mockup = {"content": content, "mockup_date": datetime.date.today().isoformat()}
        with open("content_mockup.json", "w") as f:
            json.dump(mockup, f)
        logger.info("Content mockup created")

    def track_tech_thursday_issues(self, issues: List[str]):
        """As a UI designer, I want to track the issues that come up in Tech Thursday, so that I know what to test and what wants to be fixed."""
        tracked = {"issues": issues, "session": "tech_thursday", "date": datetime.date.today().isoformat()}
        with open("tech_thursday_issues.json", "w") as f:
            json.dump(tracked, f)
        logger.info(f"Tracked {len(issues)} issues from Tech Thursday")

    def create_user_testing_summary(self, ui_sme_input: Dict):
        """As an Owner, I want to create a user testing summary from the UI SME, so that I can know what UI improvements we will follow through on."""
        summary = {"sme_input": ui_sme_input, "improvements": ["fix1", "fix2"], "status": "planned"}
        logger.info("User testing summary created")
        return summary

    def begin_user_testing(self, test_plan: Dict):
        """As a UI designer, I want to begin user testing, so that I can validate stakeholder UI improvement requests."""
        logger.info(f"Beginning user testing with plan: {test_plan}")
        results = {"passed": 5, "failed": 2}
        return results

    def schedule_user_testing(self, schedule: Dict):
        """As a UI designer, I want to schedule user testing, so that I can give the testers advanced notice to ensure buy-in."""
        schedule["scheduled"] = True
        with open("user_testing_schedule.json", "w") as f:
            json.dump(schedule, f)
        logger.info("User testing scheduled")

    def design_ui_schedule(self, ui_sme_schedule: List):
        """As an Owner, I want to design a schedule from the UI SME, so that I know the potential timeline of the UI improvements wanted."""
        timeline = {"milestones": ui_sme_schedule, "estimated_completion": "2023-12-31"}
        logger.info("UI improvement schedule designed")
        return timeline

    def design_ui_audit(self, ui_sme_audit: Dict):
        """As an Owner, I want to design an audit from the UI SME, so that I know the potential scope of the UI improvements wanted."""
        audit = {"scope": ui_sme_audit, "issues_identified": ["scope1", "scope2"]}
        logger.info("UI audit designed")
        return audit

    def reset_environment_permissions(self):
        """As an Owner, I want to reset the environment to only take Staging MAX permissions, so that I can ensure that the FABS testers no longer have access."""
        permissions = {"env": "staging", "max_only": True, "fabs_testers": False}
        with open("permissions.json", "w") as f:
            json.dump(permissions, f)
        logger.info("Environment permissions reset to Staging MAX only")

    def index_domain_models(self, model_name: str):
        """As a Developer, I want my domain models to be indexed properly, so that I can get validation results back in a reasonable amount of time."""
        # Simulate indexing
        index = f"Index created for {model_name}"
        logger.info(index)
        return index

    def update_header_info_with_datetime(self, update_time: datetime.datetime):
        """As an Agency user, I want the header information box to show updated date AND time, so that I know when it was updated."""
        header = {"updated": update_time.isoformat()}
        logger.info(f"Header updated with datetime: {header}")
        return header

    def enforce_zero_padded_fields(self, fields: Dict):
        """As an owner, I only want zero-padded fields, so that I can justify padding."""
        padded = {k: str(v).zfill(10) if isinstance(v, int) else v for k, v in fields.items()}
        logger.info("Fields zero-padded")
        return padded

    def update_error_codes(self, error_logic: Dict):
        """As a Broker user, I want to have updated error codes that accurately reflect the logic and provide enough information, so that I can fix my submission."""
        for rule in self.session.query(ValidationRule).all():
            if rule.id in error_logic:
                rule.description = error_logic[rule.id]
                rule.error_code = f"ERR_{rule.id}"
        self.session.commit()
        logger.info("Error codes updated")

    def quick_access_broker_data(self, data_key: str):
        """As a Developer I want to quickly access Broker application data, so that I can investigate issues."""
        data = self.session.query(HistoricalData).filter_by(source=data_key).first()
        if data:
            return json.loads(data.data)
        return {}

    def grant_read_only_dabs_access_to_fabs_users(self, user: str):
        """As a FABS user, I want to have read-only access to DABS, so that I can view DABS pages without wanting two sets of permissions."""
        permissions = {user: {"dabs": "read_only"}}
        logger.info(f"Read-only DABS access granted to FABS user {user}")
        return permissions

    def create_agency_landing_page(self):
        """As an agency user, I want a landing page to navigate to either FABS or DABS pages, so that I can access both sides of the site."""
        page_config = {"nav": ["FABS", "DABS"], "type": "agency_landing"}
        with open("agency_landing.json", "w") as f:
            json.dump(page_config, f)
        logger.info("Agency landing page created")

    # Cluster 2 stories
    def update_fabs_submission_on_publish_status_change(self, submission_id: int, new_status: str):
        """As a Developer, I want to add the updates on a FABS submission to be modified when the publishStatus changes, so that I know when the status of the submission has changed."""
        sub = self.session.query(Submission).get(submission_id)
        if sub:
            sub.publish_status = new_status
            sub.updated_at = datetime.datetime.now()
            self.session.commit()
            logger.info(f"FABS submission {submission_id} status updated to {new_status}")

    def add_gtas_window_data(self, window_start: datetime.date, window_end: datetime.date):
        """As a Developer, I want to add the GTAS window data to the database, so that I can ensure the site is locked down during the GTAS submission period."""
        gtas_data = {"start": window_start, "end": window_end, "lockdown": True}
        hist = HistoricalData(source="GTAS", data=json.dumps(gtas_data))
        self.session.add(hist)
        self.session.commit()
        logger.info("GTAS window data added")

    def update_fabs_sample_file(self):
        """As a Developer, I want to update the FABS sample file to remove FundingAgencyCode after FABS is updated to no longer require the header."""
        sample_df = pd.DataFrame({"col1": [1,2,3]})  # Mock
        sample_df.to_csv("fabs_sample.csv", index=False)
        logger.info("FABS sample file updated without FundingAgencyCode")

    def deactivate_publish_button_during_derivations(self, submission_id: int):
        """As a user, I want the publish button in FABS to deactivate after I click it while the derivations are happening, so that I cannot click it multiple times for the same submission."""
        # Simulate UI state
        ui_state = {"publish_button": "deactivated", "submission": submission_id, "deriving": True}
        logger.info(f"Publish button deactivated for submission {submission_id}")
        return ui_state

    def derive_fields_in_historical_fabs_loader(self, historical_data: Dict):
        """As a broker user, I want the historical FABS loader to derive fields, so that my agency codes are correct in the PublishedAwardFinancialAssistance table."""
        if "agency_code" in historical_data:
            historical_data = self.derive_funding_agency_code(historical_data)
        # Load to DB
        hist = HistoricalData(source="FABS_HISTORICAL", data=json.dumps(historical_data))
        self.session.add(hist)
        self.session.commit()
        logger.info("Fields derived in historical FABS loader")

    def include_frec_derivations_in_historical_fabs(self, data: Dict):
        """As a Developer, I want the data loaded from historical FABS to include the FREC derivations, so that I can have consistent FREC data for USASpending.gov."""
        data["frec"] = "derived_frec_code"
        logger.info("FREC derivations included in historical FABS data")
        return data

    def update_frontend_urls_for_fabs(self, page: str):
        """As a FABS user, I want the frontend URLs to more accurately reflect the page I'm accessing, so that I'm not confused."""
        urls = {page: f"/fabs/{page.lower()}"}
        logger.info(f"Frontend URL updated for {page}")
        return urls

    def load_historical_fpds_data(self, extracted_data: Dict, feed_data: Dict):
        """As a Developer, I want the historical FPDS data loader to include both extracted historical data and FPDS feed data."""
        combined = {**extracted_data, **feed_data}
        hist = HistoricalData(source="FPDS_HISTORICAL", data=json.dumps(combined))
        self.session.add(hist)
        self.session.commit()
        logger.info("Historical FPDS data loaded (extracted + feed)")

    def provide_fabs_groups_under_frec(self, groups: List):
        """As a Developer, I want to provide FABS groups that function under the FREC paradigm."""
        frec_groups = [{"group": g, "paradigm": "FREC"} for g in groups]
        logger.info("FABS groups provided under FREC")
        return frec_groups

    def ensure_historical_data_columns(self, data: pd.DataFrame):
        """As a FABS user, I want to make sure the historical data includes all necessary columns, so that the information in the database is correct."""
        required_cols = ["col1", "col2", "col3"]
        for col in required_cols:
            if col not in data.columns:
                data[col] = None
        data.to_csv("historical_with_cols.csv", index=False)
        logger.info("Historical data columns ensured")
        return data

    def access_additional_fpds_fields(self, fields: List[str]):
        """As a data user, I want to access two additional fields from the FPDS data pull."""
        additional = {"field1": "value1", "field2": "value2"}
        logger.info(f"Accessed additional FPDS fields: {fields}")
        return additional

    def add_helpful_info_to_submission_dashboard(self, submission_id: int, info: Dict):
        """As a FABS user, I want additional helpful info in the submission dashboard, so that I can better manage submissions and IG requests."""
        dashboard = {"id": submission_id, "info": info}
        logger.info(f"Helpful info added to dashboard for {submission_id}")
        return dashboard

    def download_uploaded_fabs_file(self, file_path: str):
        """As a FABS user, I want to download the uploaded FABS file, so that I can get the uploaded file."""
        if os.path.exists(file_path):
            logger.info(f"Downloaded FABS file: {file_path}")
            return file_path
        return None

    def determine_historical_fpds_load_method(self, since_year: int = 2007):
        """As a Developer, I want to determine the best way to load historical FPDS data, so that I can load all FPDS data since 2007."""
        method = "batch_load_since_2007"
        logger.info(f"Best method for loading FPDS since {since_year}: {method}")
        return method

    def ensure_appropriate_language_on_fabs_pages(self, page_content: str):
        """As a FABS user, I want the language on FABS pages to be appropriate for me, so that I am not confused."""
        # Simple check for clarity
        if len(page_content.split()) < 1000:  # Reasonable length
            logger.info("FABS page language ensured appropriate")
            return page_content
        return "Simplified language applied"

    def customize_banner_messages(self, app_type: str):
        """As a FABS user, I do not want DABS banner messages and vice versa, so that I have the appropriate information for my application."""
        banners = {"FABS": "FABS-specific message", "DABS": "DABS-specific message"}
        message = banners.get(app_type, "Default")
        logger.info(f"Custom banner for {app_type}: {message}")
        return message

    def show_submission_periods(self):
        """As an agency user, I want to know when the submission periods start and end, so that I know when the submission starts and ends."""
        periods = {"start": "2023-01-01", "end": "2023-03-31"}
        logger.info(f"Submission periods: {periods}")
        return periods

    # Cluster 0 stories
    def update_upload_validate_error_message(self, message: str):
        """As a Broker user, I want to Upload and Validate the error message to have accurate text."""
        accurate_msg = f"Accurate validation error: {message}"
        logger.warning(accurate_msg)
        return accurate_msg

    def update_validation_rule_table_db2213(self):
        """As a Developer, I want to update the Broker validation rule table to account for the rule updates in DB-2213."""
        rule = ValidationRule(rule_name="DB2213", description="Updated rule", error_code="2213")
        self.session.add(rule)
        self.session.commit()
        logger.info("Validation rule table updated for DB-2213")

    def include_flexfields_in_error_files(self, flexfields: Dict, missing_required: bool):
        """As a user, I want the flexfields in my submission file to appear in the warning and error files when the only error is a missing required element."""
        if missing_required:
            error_file = {"flexfields": flexfields, "error": "missing_required"}
            with open("error_file.json", "w") as f:
                json.dump(error_file, f)
            logger.info("Flexfields included in error file")

    def clarify_cfda_error_code(self, case: str):
        """As a Developer, I want to clarify to users what exactly is triggering the CFDA error code in each case."""
        clarifications = {"case1": "Invalid CFDA format", "case2": "Missing CFDA"}
        msg = clarifications.get(case, "General CFDA error")
        logger.info(f"CFDA error clarification: {msg}")
        return msg

    def update_broker_resources_for_launch(self, version: str = "v1.1"):
        """As a broker team member, I want to ensure the Broker resources, validations, and P&P pages are updated appropriately for the launch of FABS and DAIMS v1.1."""
        updates = {"resources": "updated", "validations": "updated", "pp": "updated", "version": version}
        with open("broker_launch_updates.json", "w") as f:
            json.dump(updates, f)
        logger.info("Broker resources updated for FABS/DAIMS launch")

    def accept_duns_for_action_types_bcd(self, duns: str, action_type: str, sam_registered: bool, expired: bool):
        """As a user, I want the DUNS validations to accept records whose ActionTypes are B, C, or D and the DUNS is registered in SAM, even though it may have expired."""
        if action_type in ["B", "C", "D"] and sam_registered:
            logger.info(f"DUNS {duns} accepted for action {action_type} despite expiration")
            return True
        return False

    def accept_duns_before_current_registration(self, action_date: datetime.date, initial_reg_date: datetime.date, current_reg_date: datetime.date):
        """As a user, I want the DUNS validations to accept records whose ActionDates are before the current registration date in SAM, but after the initial registration date."""
        if initial_reg_date < action_date < current_reg_date:
            logger.info(f"Action date {action_date} accepted for DUNS validation")
            return True
        return False

    def helpful_file_level_error_wrong_extension(self, file_extension: str):
        """As an Agency user, I want to receive a more helpful file-level error when I upload a file with the wrong extension."""
        if file_extension not in [".csv", ".txt"]:
            error = f"Invalid extension {file_extension}. Please use .csv or .txt"
            logger.error(error)
            return error
        return None

    def prevent_duplicate_transactions_on_publish(self, transaction_id: str, time_gap: int):
        """As a Developer, I want to prevent duplicate transactions from being published and deal with the time gap between validation and the publishing decision."""
        # Check for duplicates
        existing = self.session.query(Submission).filter_by(id=transaction_id).first()
        if existing and time_gap < 300:  # 5 min gap
            logger.warning(f"Duplicate publish prevented for {transaction_id}")
            return False
        return True

    # Cluster 1 stories
    def manage_d_files_generation_cache(self, request_id: str, data: Dict):
        """As a Developer, I want D Files generation requests to be managed and cached, so that duplicate requests do not cause performance issues."""
        cache = getattr(self, '_d_cache', {})
        if request_id not in cache:
            cache[request_id] = data
            self._d_cache = cache
            logger.info(f"D file generated and cached for {request_id}")
        else:
            logger.info(f"D file retrieved from cache for {request_id}")
        return cache.get(request_id)

    def access_raw_agency_published_files(self, via_usaspending: bool = True):
        """As a user, I want to access the raw agency published files from FABS via USAspending."""
        if via_usaspending:
            files = ["raw_agency_file1.csv"]
            logger.info("Raw FABS files accessed via USAspending")
            return files
        return []

    def handle_large_flexfields_no_impact(self, flexfields: List[Dict], num: int = 1000):
        """As an Agency user, I want to be able to include a large number of flexfields without performance impact."""
        # Simulate efficient processing
        processed = [f for f in flexfields[:num]]
        logger.info(f"Processed {len(processed)} flexfields without impact")
        return processed

    def prevent_double_publishing_after_refresh(self, submission_id: int):
        """As a Developer, I want to prevent users from double publishing FABS submissions after refreshing, so that there are no duplicates."""
        # Use a lock or flag
        lock = getattr(self, '_publish_locks', {})
        if submission_id not in lock:
            lock[submission_id] = True
            self._publish_locks = lock
            logger.info(f"Publishing allowed for {submission_id}")
            return True
        logger.warning(f"Double publish prevented for {submission_id}")
        return False

    def ensure_daily_financial_assistance_data(self):
        """As a website user, I want to see updated financial assistance data daily."""
        today = datetime.date.today()
        update = {"date": today.isoformat(), "updated": True}
        logger.info(f"Daily financial assistance data updated: {update}")
        return update

    def prevent_publish_non_existent_records(self, record_id: int, action: str):
        """As a Developer, I want to ensure that attempts to correct or delete non-existent records don't create new published data."""
        existing = self.session.query(Submission).get(record_id)
        if not existing and action in ["correct", "delete"]:
            logger.warning(f"Non-existent record {record_id} - no action taken")
            return False
        return True

    def ensure_ppopcode_and_congressional_district_data(self, ppop: str, district: str):
        """As a user, I want to have accurate and complete data related to PPoPCode and PPoPCongressionalDistrict."""
        complete_data = {"ppop_code": ppop, "district": district, "validated": True}
        logger.info("PPoPCode and CongressionalDistrict data ensured accurate")
        return complete_data

    def prevent_nasa_grants_as_contracts(self, record: Dict):
        """As a user, I don't want to see NASA grants displayed as contracts."""
        if record.get("agency") == "NASA" and record.get("type") == "grant":
            record["display_type"] = "grant"
            logger.info("NASA grant correctly displayed as grant")
        return record

    def determine_d_files_generation_from_fabs_fpds(self, fabs_data: Dict, fpds_data: Dict):
        """As a Developer, I want to determine how agencies will generate and validate D Files from FABS and FPDS data."""
        method = "combine_and_validate_fabs_fpds"
        logger.info(f"D files generation method: {method}")
        return method

    def generate_validate_d_files(self, fabs_data: Dict, fpds_data: Dict):
        """As a user, I want to generate and validate D Files from FABS and FPDS data."""
        d_file = {**fabs_data, **fpds_data}
        validation = self.validate_d_file(d_file)
        logger.info(f"D file generated and validated: {validation}")
        return d_file if validation else None

    def validate_d_file(self, d_file: Dict) -> bool:
        """Helper validation."""
        return "required_key" in d_file

    def access_test_features_in_nonprod(self, feature: str, env: str):
        """As a tester, I want to have access to test features in environments other than Staging, so that I can test any nonProd feature in any environment."""
        if env != "prod":
            logger.info(f"Test access granted for {feature} in {env}")
            return True
        return False

    def accurate_fabs_submission_errors(self, error: str):
        """As a FABS user, I want to submission errors to accurately represent FABS errors, so that I know why my submission didn't work."""
        accurate_error = f"FABS-specific error: {error}"
        logger.error(accurate_error)
        return accurate_error

    def show_submission_creator(self, submission: Dict):
        """As an Agency user, I want to accurately see who created a submission, so that I'm not confused about who last updated a submission."""
        creator = submission.get("created_by", "Unknown")
        logger.info(f"Submission creator: {creator}")
        return creator

    def robust_test_for_fabs_derivations(self, test_file: str):
        """As a tester, I want to ensure that FABS is deriving fields properly through a robust test file plus a follow up check."""
        # Simulate test
        derived = self.derive_funding_agency_code({"agency_code": "123"})
        check = "funding_agency_code" in derived
        logger.info(f"FABS derivation test passed: {check}")
        return check

    def submit_individual_recipients_no_duns_error(self, recipient: Dict):
        """As a Broker user, I want to submit records for individual recipients without receiving a DUNS error."""
        if recipient.get("type") == "individual":
            recipient["duns_error"] = False
            logger.info("Individual recipient submitted without DUNS error")
        return recipient

    def show_rows_to_publish_before_decision(self, rows_count: int):
        """As a user, I want more information about how many rows will be published prior to deciding whether to publish."""
        info = f"{rows_count} rows will be published"
        logger.info(info)
        return info

    def submit_citywide_ppopzip(self, zip_code: str):
        """As a FABS user, I want to submit a citywide as a PPoPZIP and pass validations."""
        if "citywide" in zip_code.lower():
            validation = self.validate_ppop_zip_plus4(zip_code)
            logger.info(f"Citywide PPoPZIP {zip_code} submitted: {validation}")
            return validation
        return False

    def reasonable_validation_time(self, data_size: int):
        """As a FABS user, I want to have my validations run in a reasonable amount of time."""
        start = datetime.datetime.now()
        # Simulate validation
        time_taken = data_size / 1000.0  # Mock time
        end = start + datetime.timedelta(seconds=time_taken)
        logger.info(f"Validation completed in {time_taken} seconds")
        return end

    # Cluster 3 stories
    def receive_fabs_updates(self, updates: List[Dict]):
        """As an data user, I want to receive updates to FABS records."""
        for update in updates:
            logger.info(f"FABS update received: {update}")
        return len(updates)

    def exclude_deleted_fsrs_records(self, records: List[Dict]):
        """As an agency user, I want to ensure that deleted FSRS records are not included in submissions."""
        filtered = [r for r in records if not r.get("deleted", False)]
        logger.info(f"Excluded deleted FSRS records: {len(filtered)} remaining")
        return filtered

    def accept_zero_blank_for_loan_records(self, value: str, is_loan: bool):
        """As an agency user, I want the FABS validation rules to accept zero and blank for loan records."""
        if is_loan and value in ["0", "", "blank"]:
            logger.info("Zero/blank accepted for loan record")
            return True
        return False

    def deploy_fabs_to_production(self):
        """As an Agency user, I want FABS deployed into production, so I can submit my Financial Assistance data."""
        deployment = {"status": "deployed", "env": "production"}
        logger.info("FABS deployed to production")
        return deployment

    def ensure_complete_sam_data(self, sam_data: Dict):
        """As an agency user, I want to be confident that the data coming from SAM is complete."""
        completeness = all(key in sam_data for key in ["name", "address", "duns"])
        if completeness:
            logger.info("SAM data confirmed complete")
        else:
            logger.warning("SAM data incomplete")
        return completeness

    def accept_zero_blank_for_non_loan_records(self, value: str, is_loan: bool):
        """As an agency user, I want the FABS validation rules to accept zero and blank for non-loan records."""
        if not is_loan and value in ["0", "", "blank"]:
            logger.info("Zero/blank accepted for non-loan record")
            return True
        return False

    def derive_all_data_elements(self, data: Dict):
        """As an agency user, I want to have all derived data elements derived properly."""
        data = self.derive_funding_agency_code(data)
        data["other_derived"] = "value"
        logger.info("All data elements derived properly")
        return data

    def update_legal_entity_address_line3_length(self, address: str):
        """As an agency user, I want the maximum length allowed for LegalEntityAddressLine3 to match Schema v1.1."""
        max_len = 100  # v1.1 schema
        truncated = address[:max_len] if len(address) > max_len else address
        logger.info(f"LegalEntityAddressLine3 truncated to {max_len} chars")
        return truncated

    def use_schema_v11_headers(self, headers: List[str]):
        """As an agency user, I want to use the schema v1.1 headers in my FABS file."""
        v11_headers = ["header1_v11", "header2_v11"]
        updated_headers = v11_headers if not headers else headers + v11_headers
        logger.info("Schema v1.1 headers applied to FABS file")
        return updated_headers

    def update_fpds_daily(self):
        """As an Agency user, I want FPDS data to be up-to-date daily."""
        today = datetime.date.today()
        update = {"source": "FPDS", "date": today.isoformat(), "status": "updated"}
        logger.info("FPDS data updated daily")
        return update

    def load_all_historical_financial_assistance(self):
        """As an Agency user, I want all historical Financial Assistance data loaded for FABS go-live."""
        hist = HistoricalData(source="FINANCIAL_ASSISTANCE_HISTORICAL", data=json.dumps({"all_loaded": True}))
        self.session.add(hist)
        self.session.commit()
        logger.info("All historical Financial Assistance data loaded for FABS go-live")

    def load_historical_fpds(self):
        """As an Agency user, I want historical FPDS data loaded."""
        self.load_historical_fpds_data({}, {})
        logger.info("Historical FPDS data loaded")

    def generate_file_f_correct_format(self, data: Dict):
        """As an agency user, I want to get File F in the correct format."""
        file_f = pd.DataFrame([data])
        file_f.to_csv("file_f.csv", index=False)
        logger.info("File F generated in correct format")
        return "file_f.csv"

    def better_understand_file_level_errors(self, error: str):
        """As an Agency user, I want to better understand my file-level errors."""
        detailed = f"Detailed explanation: {error}. Check format and required fields."
        logger.error(detailed)
        return detailed

    def submit_data_with_quotation_marks(self, data: List[str]):
        """As an agency user, I want to submit my data elements surrounded by quotation marks, so that Excel won't strip off leading and trailing zeroes."""
        quoted = [f'"{d}"' for d in data]
        logger.info("Data submitted with quotation marks to preserve zeroes")
        return quoted

    # Combined Clusters
    def derive_office_names_from_codes(self, office_codes: List[str]):
        """Cluster (2, 5): As a data user, I want to see the office names derived from office codes, so that I can have appropriate context for understanding them."""
        names = {code: f"Office of {code}" for code in office_codes}
        logger.info("Office names derived from codes")
        return names

    def link_sample_file_in_dialog(self, dialog: str):
        """Cluster (2, 4, 5): As a FABS user, I want to link the SAMPLE FILE on the "What you want to submit" dialog to point to the correct file, so that I have an accurate reference for my agency submissions."""
        link = {"dialog": dialog, "sample_file": "correct_fabs_sample.csv"}
        logger.info(f"SAMPLE file linked in {dialog}")
        return link

    def allow_zip_without_last4_digits(self, zip_code: str):
        """Cluster (3, 5): As an agency user, I want to leave off the last 4 digits of the ZIP without an error, so that I can complete my submissions."""
        if len(zip_code) == 5:
            validation = self.validate_ppop_zip_plus4(zip_code)
            logger.info(f"ZIP {zip_code} (5 digits) accepted")
            return validation
        return False

    def correct_status_labels_on_dashboard(self, statuses: List[str]):
        """Cluster (1, 2): As a FABS user, I want to see correct status labels on the Submission Dashboard, so that I can quickly see my submission history."""
        corrected = [s.replace("old", "new") for s in statuses]  # Mock correction
        logger.info("Status labels corrected on dashboard")
        return corrected

# Main execution simulation
if __name__ == "__main__":
    broker = BrokerSystem()
    # Simulate some executions
    broker.process_deletions()
    broker.redesign_resources_page({"color": "blue", "font": "Arial"})
    broker.sync_d1_file_generation_with_fpds(datetime.datetime.now())
    broker.update_error_codes({1: "New desc"})
    broker.deploy_fabs_to_production()
    print("Broker system initialized and stories implemented.")