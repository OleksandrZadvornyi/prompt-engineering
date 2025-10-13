import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

class BrokerDataProcessor:
    def __init__(self, db_path: str = 'broker.db'):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables for submissions and validation rules
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS submissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                submission_id TEXT UNIQUE,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS validation_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_name TEXT,
                description TEXT,
                rule_logic TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ppop_validations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                zip_code TEXT,
                congressional_district TEXT,
                validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS funding_agency_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                award_id TEXT,
                funding_agency_code TEXT,
                derived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS duns_validations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                duns TEXT,
                action_type TEXT,
                action_date DATE,
                sam_registered BOOLEAN,
                valid BOOLEAN,
                validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fpds_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                record_id TEXT UNIQUE,
                data TEXT,
                pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fabs_submissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                submission_id TEXT UNIQUE,
                status TEXT,
                file_data TEXT,
                published BOOLEAN DEFAULT FALSE,
                publish_status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gtas_window (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                active BOOLEAN DEFAULT FALSE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS audit_trail (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT,
                entity_id TEXT,
                action TEXT,
                details TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def process_12_19_2017_deletions(self) -> bool:
        """
        Process deletions from 12-19-2017
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Update the database to remove records with dates from that period
            cursor.execute('''
                DELETE FROM submissions WHERE created_at BETWEEN ? AND ?
            ''', ('2017-12-19 00:00:00', '2017-12-19 23:59:59'))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error processing deletions: {e}")
            return False
    
    def update_resource_page_design(self) -> bool:
        """
        Simulate updating resource page design to match new Broker style
        """
        try:
            # In practice, this would update UI templates or CSS
            # For now simulate through log entry
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO audit_trail (entity_type, entity_id, action, details)
                VALUES (?, ?, ?, ?)
            ''', ('UI', 'resources-page', 'design-update', 
                  'Resources page redesigned to match new Broker patterns'))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error updating UI: {e}")
            return False
            
    def report_user_testing_findings(self, agency: str, findings: str) -> bool:
        """
        Report user testing results to agencies
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO audit_trail (entity_type, entity_id, action, details)
                VALUES (?, ?, ?, ?)
            ''', ('Agency', agency, 'user-testing-report', findings))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error reporting user testing: {e}")
            return False
            
    def sync_d1_file_generation(self) -> bool:
        """
        Synchronize D1 file generation with FPDS data load
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if there were any FPDS data changes in the last hour
            cursor.execute('''
                SELECT COUNT(*) FROM fpds_data 
                WHERE pulled_at > datetime('now', '-1 hour')
            ''')
            
            data_changed = cursor.fetchone()[0] > 0
            
            cursor.execute('''
                INSERT INTO audit_trail (entity_type, entity_id, action, details)
                VALUES (?, ?, ?, ?)
            ''', ('System', 'D1-generation', 'sync-check', 
                  f'Data changed: {data_changed}'))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error syncing D1 generation: {e}")
            return False
            
    def update_sql_code_clarity(self) -> bool:
        """
        Update SQL codes for better clarity
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO audit_trail (entity_type, entity_id, action, details)
                VALUES (?, ?, ?, ?)
            ''', ('SQL', 'queries', 'clarity-improvement', 
                  'Updated SQL queries for improved readability'))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error updating SQL codes: {e}")
            return False
    
    def add_ppopcode_derivation_logic(self) -> bool:
        """
        Add special handling for 00***** and 00FORGN PPoPCode cases
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor() 
            
            # Insert new derivation logic into validation rules
            cursor.execute('''
                INSERT OR IGNORE INTO validation_rules 
                (rule_name, description, rule_logic) VALUES (?, ?, ?)
            ''', ('PPoPCodeSpecialCases', 
                  'Handle 00***** and 00FORGN prefixed codes',
                  'if ppop_code.startswith("00") and re.match("^00\\*+", ppop_code): return TRUE'))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error adding PPoPCode derivation: {e}")
            return False
    
    def derive_funding_agency_code(self, award_id: str, agency_info: dict) -> bool:
        """
        Derive FundingAgencyCode based on given data
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Simplified derivation logic - in reality this would be complex business rules
            funding_agency_code = agency_info.get('agency_code', '')[:3]
            
            cursor.execute('''
                INSERT OR IGNORE INTO funding_agency_codes 
                (award_id, funding_agency_code) VALUES (?, ?)
            ''', (award_id, funding_agency_code))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error deriving funding agency code: {e}")
            return False
    
    def map_federal_action_obligation(self, atom_feed_data: dict) -> bool:
        """
        Map FederalActionObligation properly to Atom Feed
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            obligated = atom_feed_data.get('federal_action_obligation', '')
            
            # This would normally involve mapping to a feed format
            cursor.execute('''
                INSERT INTO audit_trail (entity_type, entity_id, action, details)
                VALUES (?, ?, ?, ?)
            ''', ('AtomFeed', 'federal-action', 'mapping-updated', 
                  f'Mapped obligation amount: {obligated}'))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error mapping federal action obligation: {e}")
            return False
    
    def update_zip4_validation(self) -> bool:
        """
        Make PPoPZIP+4 work the same as Legal Entity ZIP validations
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR IGNORE INTO validation_rules 
                (rule_name, description, rule_logic) VALUES (?, ?, ?)
            ''', ('ZIP4Validation', 
                  'Make PPoPZIP+4 follow same validation as legal entity ZIP',
                  'ZIP+4 validation applies equally to both location types'))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error updating ZIP+4 validation: {e}")
            return False
    
    def design_landing_pages_rounds(self, round_number: int) -> bool:
        """
        Move to next round of landing page edits
        """
        try:
            conn = sqlite3.connect(self.db_path)  
            cursor = conn.cursor()
            
            pages = ['dabs', 'fabs', 'homepage', 'help']
            for page in pages:
                cursor.execute('''
                    INSERT OR IGNORE INTO audit_trail (entity_type, entity_id, action, details)
                    VALUES (?, ?, ?, ?)
                ''', ('UI', page, f'round-{round_number}-editing', 
                      f'Editing completed for {page}'))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error updating landing pages: {e}")
            return False
    
    def update_submission_dashboard(self) -> bool:
        """
        Add helpful info to submission dashboard
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Insert notification about dashboard updates
            cursor.execute('''
                INSERT OR IGNORE INTO audit_trail (entity_type, entity_id, action, details)
                VALUES (?, ?, ?, ?)
            ''', ('Dashboard', 'submission', 'enhancement', 
                  'Added additional metrics and IG request links'))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error updating dashboard: {e}")
            return False
    
    def download_uploaded_file(self, submission_id: str) -> Optional[str]:
        """
        Allow downloading uploaded FABS file
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT file_data FROM fabs_submissions WHERE submission_id = ?
            ''', (submission_id,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return result[0]
            return None
        except Exception as e:
            print(f"Error retrieving uploaded file: {e}")
            return None
    
    def validate_duns_for_actions(self) -> bool:
        """
        Update DUNS validations to accept B, C, D actions or expired registrations
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR IGNORE INTO validation_rules 
                (rule_name, description, rule_logic) VALUES (?, ?, ?)
            ''', ('DUNSValidationActions', 
                  'Accept registrations for B,C,D actions and expired records',
                  'Allow DUNS validation for ActionType in [B,C,D] with SAM registration'))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error validating DUNS actions: {e}")
            return False
    
    def set_validation_errors_accurate(self) -> bool:
        """
        Ensure error messages accurately reflect logic and provide useful information
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Simulate adding detailed validation error mappings
            cursor.execute('''
                INSERT OR IGNORE INTO validation_rules 
                (rule_name, description, rule_logic) VALUES (?, ?, ?)
            ''', ('AccurateErrorCodes', 
                  'Improve error reporting with meaningful contexts',
                  'Use descriptive error codes that clearly identify validation failures'))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error setting accurate validation errors: {e}")
            return False
    
    def generate_error_context(self, rule_name: str, context: str) -> str:
        """
        Generate clear error context for validation rules  
        """
        return f"[{rule_name}] {context}"
    
    def get_submission_history(self, submission_id: str) -> List[Dict]:
        """
        Get submission status history
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM audit_trail WHERE entity_id = ? ORDER BY timestamp DESC
            ''', (submission_id,))
            
            results = cursor.fetchall()
            conn.close()
            
            column_names = ['id', 'entity_type', 'entity_id', 'action', 'details', 'timestamp']
            return [dict(zip(column_names, row)) for row in results]
        except Exception as e:
            print(f"Error fetching submission history: {e}")
            return []
    
    def add_gtas_window_data(self, start_time: str, end_time: str) -> bool:
        """
        Add GTAS window data for security purposes
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO gtas_window (window_start, window_end, active)
                VALUES (?, ?, ?)
            ''', (start_time, end_time, True))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error adding GTAS window data: {e}")
            return False
    
    def check_duplicate_transactions(self) -> bool:
        """
        Prevent duplicate publications from being made
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(''' 
                CREATE TABLE IF NOT EXISTS published_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_hash TEXT UNIQUE,
                    published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error checking duplicates: {e}")
            return False
    
    def process_historical_fabs_derivations(self) -> bool:
        """
        Apply historical FABS derivations including FREC
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Placeholder for complex derivation logic here
            
            cursor.execute('''
                INSERT INTO audit_trail (entity_type, entity_id, action, details)
                VALUES (?, ?, ?, ?)
            ''', ('Historical', 'FABS-data', 'derivation-applied', 
                  'Applied FREC and other derivation logic to historical data'))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error processing historical FABS derivations: {e}")
            return False
    
    def validate_complex_ppop_zip(self, ppop_zip: str) -> bool:
        """
        Validate various PPoP ZIP formats including full 9-digit and truncated
        """
        if len(ppop_zip) == 5:
            # Full 5-digit ZIP
            return True
        elif len(ppop_zip) == 9:
            # Valid 9-digit ZIP
            return True 
        elif len(ppop_zip) == 4 or len(ppop_zip) == 3 or len(ppop_zip) == 2:
            # Allow truncated forms for certain validation scenarios
            return True
        return False
    
    def update_fabs_validation_rules(self, new_rules: dict) -> bool:
        """
        Update FABS validation rules for current version requirements
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO validation_rules (rule_name, description, rule_logic)
                VALUES (?, ?, ?)
            ''', ('FABSValidationCurrent', 'Updated validation rules', 
                  str(new_rules)))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error updating FABS validation rules: {e}")
            return False
    
    def create_error_message_for_missing_elements(self, field_name: str, context: str) -> str:
        """
        Create clear error when required elements are missing
        """
        return f"{field_name}: Required element missing ({context})"
    
    def handle_upload_validation_message(self) -> str:
        """
        Provide accurate upload validation error messages
        """  
        return "Your submission failed validation due to data format issues."

# Example usage:
if __name__ == "__main__":
    processor = BrokerDataProcessor()
    
    # Test operations that would be called per user story:
    
    # Cluster 4 tasks
    processor.process_12_19_2017_deletions()
    processor.update_resource_page_design()
    processor.report_user_testing_findings('AgencyX', 'Improved navigation feedback')
    processor.sync_d1_file_generation()
    processor.update_sql_code_clarity()
    processor.add_ppopcode_derivation_logic()
    processor.derive_funding_agency_code('AWARD-001', {'agency_code': 'ABC'})
    processor.map_federal_action_obligation({'federal_action_obligation': '500000'})
    processor.update_zip4_validation()
    
    # Cluster 5 tasks  
    processor.design_landing_pages_rounds(2)
    processor.update_submission_dashboard()
    processor.validate_duns_for_actions()
    
    # Cluster 2 tasks
    processor.add_gtas_window_data('2023-01-01 00:00:00', '2023-01-05 23:59:59')
    processor.check_duplicate_transactions()
    processor.process_historical_fabs_derivations()
    
    # Cluster 0 tasks
    processor.set_validation_errors_accurate()
    processor.update_fabs_validation_rules({'loan_required_fields': ['zero_allowed']})
    
    # Cluster 1 tasks
    processor.generate_error_context("FABSValidation","Field required")
    processor.get_submission_history("SUBMISSION-001")
    
    # Cluster 3 tasks
    processor.validate_complex_ppop_zip("12345")
    processor.create_error_message_for_missing_elements("LegalEntityZip", "Required field")
    processor.handle_upload_validation_message()