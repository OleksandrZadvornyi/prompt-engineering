import sqlite3
from datetime import datetime
from typing import List, Dict, Optional, Tuple

class DatabaseManager:
    def __init__(self, db_path: str = "broker.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS submissions (
                submission_id INTEGER PRIMARY KEY AUTOINCREMENT,
                agency_code TEXT,
                submission_type TEXT, -- 'FABS' or 'DABS'
                status TEXT, -- 'draft', 'validated', 'published'
                submitter TEXT,
                submission_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                publish_status TEXT,
                file_path TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS validation_errors (
                error_id INTEGER PRIMARY KEY AUTOINCREMENT,
                submission_id INTEGER,
                error_code TEXT,
                error_message TEXT,
                field_name TEXT,
                severity TEXT,
                FOREIGN KEY (submission_id) REFERENCES submissions(submission_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS file_metadata (
                metadata_id INTEGER PRIMARY KEY AUTOINCREMENT,
                submission_id INTEGER,
                file_name TEXT,
                file_type TEXT,
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (submission_id) REFERENCES submissions(submission_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ppop_zips (
                zip_id INTEGER PRIMARY KEY AUTOINCREMENT,
                submission_id INTEGER,
                zip_code TEXT,
                zip_plus_four TEXT,
                congressional_district TEXT,
                FOREIGN KEY (submission_id) REFERENCES submissions(submission_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS historical_fabs_data (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                submission_id INTEGER,
                agency_code TEXT,
                funding_agency_code TEXT,
                frec_code TEXT,
                legal_entity_zip TEXT,
                ppop_code TEXT,
                FOREIGN KEY (submission_id) REFERENCES submissions(submission_id)
            )
        ''')
        
        conn.commit()
        conn.close()

class ValidationEngine:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        
    def validate_fabs_submission(self, submission_data: Dict) -> List[Dict]:
        """
        Validates FABS submission data according to defined rules.
        """
        errors = []
        
        # DUNS validation for ActionTypes B, C, D and valid SAM registrations
        if submission_data.get('action_type') in ['B', 'C', 'D']:
            duns_validated = self._validate_duns_sam_registration(
                submission_data['duns'], 
                submission_data['action_date']
            )
            if not duns_validated:
                errors.append({
                    'error_code': 'DUNS_INVALID',
                    'error_message': 'DUNS is invalid for this action type and registration date',
                    'field_name': 'DUNS',
                    'severity': 'critical'
                })
                
        # ZIP+4 validation
        zip_input = submission_data.get('ppop_zip')
        if zip_input:
            if not self._validate_zip_plus_four(zip_input):
                errors.append({
                    'error_code': 'INVALID_ZIP',
                    'error_message': 'PPoP ZIP+4 does not meet length requirements',
                    'field_name': 'PPoPZIP+4',
                    'severity': 'critical'
                })
                
        # CFDA error code clarification
        cfda_field = submission_data.get('cfda_number')
        if cfda_field:
            if not self._validate_cfda_format(cfda_field):
                errors.append({
                    'error_code': 'CFDA_FORMAT_ERROR',
                    'error_message': 'CFDA Number is not in proper format',
                    'field_name': 'CFDA_Number',
                    'severity': 'warning'
                })
                
        return errors
    
    def _validate_duns_sam_registration(self, duns: str, action_date: str) -> bool:
        """Validate DUNS based on SAM registration rules."""
        # Simplified simulation - real implementation would query SAM API
        return True
        
    def _validate_zip_plus_four(self, zip_input: str) -> bool:
        """Validate ZIP+4 format."""
        zip_clean = zip_input.replace('-', '').replace(' ', '')
        return len(zip_clean) in [5, 9]
        
    def _validate_cfda_format(self, cfda_field: str) -> bool:
        """Validate CFDA number format."""
        return len(cfda_field) >= 3 and cfda_field.replace('.', '').isdigit()

class FABSDataProcessor:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.validation_engine = ValidationEngine(db_manager)
    
    def process_submission(self, submission_data: Dict) -> Dict:
        """Process a new FABS submission."""
        # Generate submission ID
        conn = sqlite3.connect(self.db_manager.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO submissions (agency_code, submission_type, status, submitter)
            VALUES (?, ?, ?, ?)
        ''', (
            submission_data.get('agency_code'),
            'FABS',
            'draft',  # Initially draft
            submission_data.get('submitter', 'Unknown')
        ))
        
        submission_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        # Run validation checks
        validation_errors = self.validation_engine.validate_fabs_submission(submission_data)
        
        # Save validation errors
        if validation_errors:
            conn = sqlite3.connect(self.db_manager.db_path)
            cursor = conn.cursor()
            
            for error in validation_errors:
                cursor.execute('''
                    INSERT INTO validation_errors (submission_id, error_code, error_message, field_name, severity)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    submission_id,
                    error['error_code'],
                    error['error_message'],
                    error['field_name'],
                    error['severity']
                ))
                
            conn.commit()
            conn.close()
        
        # Derive funding agency code if needed
        funding_agency_code = self._derive_funding_agency_code(submission_data)
        
        # Save historical FABS data
        conn = sqlite3.connect(self.db_manager.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO historical_fabs_data (submission_id, agency_code, funding_agency_code, frec_code)
            VALUES (?, ?, ?, ?)
        ''', (
            submission_id,
            submission_data.get('agency_code'),
            funding_agency_code,
            submission_data.get('frec_code', '')
        ))
        
        conn.commit()
        conn.close()
        
        return {
            'submission_id': submission_id,
            'validation_errors': validation_errors,
            'status': 'valid' if not validation_errors else 'invalid',
            'funding_agency_code': funding_agency_code
        }
    
    def _derive_funding_agency_code(self, submission_data: Dict) -> str:
        """Derive funding agency code using specific rules."""
        # Placeholder logic - should be replaced with actual derivation logic
        agency_code = submission_data.get('agency_code', '')
        if agency_code.startswith("00") and (agency_code.endswith("FORGN") or len(agency_code) > 6):
            return "FRECEXAMPLE"
        return agency_code[:3] + "000"

class FABSPublishingManager:
    """Manages FABS publication workflow"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        
    def publish_submission(self, submission_id: int) -> Dict:
        """Publish a validated FABS submission."""
        # Check if already published
        conn = sqlite3.connect(self.db_manager.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT status, publish_status FROM submissions WHERE submission_id = ?
        ''', (submission_id,))
        
        row = cursor.fetchone()
        if not row:
            return {'error': 'Submission not found'}
            
        status, publish_status = row
        
        if status != 'validated':
            return {'error': 'Submission must be validated first'}
            
        if publish_status == 'published':
            return {'error': 'Submission already published'}
            
        # Update status
        cursor.execute('''
            UPDATE submissions SET publish_status = 'published' WHERE submission_id = ?
        ''', (submission_id,))
        
        conn.commit()
        conn.close()
        
        return {'success': True, 'message': 'Submission published successfully'}

class ReportGenerator:
    """Generates reports for UI design and agency testing"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        
    def generate_user_testing_summary(self) -> Dict:
        """Generate a summary of user testing feedback"""
        conn = sqlite3.connect(self.db_manager.db_path)
        cursor = conn.cursor()
        
        summary = {
            'total_submissions': 0,
            'total_validation_errors': 0,
            'common_issues': {},
            'user_testing_feedback': []
        }
        
        # Get total submissions
        cursor.execute('SELECT COUNT(*) FROM submissions')
        summary['total_submissions'] = cursor.fetchone()[0]
        
        # Get total validation errors
        cursor.execute('SELECT COUNT(*) FROM validation_errors')
        summary['total_validation_errors'] = cursor.fetchone()[0]
        
        # Find common issues
        cursor.execute('''
            SELECT error_code, COUNT(*) as count 
            FROM validation_errors 
            GROUP BY error_code 
            ORDER BY count DESC
            LIMIT 5
        ''')
        
        for row in cursor.fetchall():
            summary['common_issues'][row[0]] = row[1]
            
        conn.close()
        return summary

class ResourcePageManager:
    """Handles the redesign and maintenance of Resources pages"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        
    def sync_resource_design_update(self):
        """Syncs resource page design changes with new broker styles"""
        # This simulates an update to resource page styling parameters
        return {"message": "Resources page design updated to match new broker styles"}
        
    def generate_error_codes_report(self) -> List[Dict]:
        """Generate report of updated error codes"""
        conn = sqlite3.connect(self.db_manager.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT error_code, error_message, field_name, severity 
            FROM validation_errors
            ORDER BY severity DESC, error_code
        ''')
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'error_code': row[0],
                'error_message': row[1],
                'field_name': row[2],
                'severity': row[3]
            })
            
        conn.close()
        return results

# System-wide classes
class BrokerSystem:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.fabs_processor = FABSDataProcessor(self.db_manager)
        self.publishing_manager = FABSPublishingManager(self.db_manager)
        self.report_generator = ReportGenerator(self.db_manager)
        self.resource_page_manager = ResourcePageManager(self.db_manager)
        
    def process_fabs_submission(self, data: Dict) -> Dict:
        """Main entry point for processing a FABS submission"""
        return self.fabs_processor.process_submission(data)
        
    def publish_fabs_submission(self, submission_id: int) -> Dict:
        """Submit to publish a valid FABS submission"""
        return self.publishing_manager.publish_submission(submission_id)
        
    def generate_user_summary(self)-> Dict:
        """Generate summary for UI designers"""
        return self.report_generator.generate_user_testing_summary()
        
    def update_resources(self):
        """Perform resource page design updates"""
        return self.resource_page_manager.sync_resource_design_update()
        
    def generate_error_report(self) -> List[Dict]:
        """Generate comprehensive error report"""
        return self.resource_page_manager.generate_error_codes_report()

# Example usage:
if __name__ == "__main__":
    broker = BrokerSystem()
    
    # Simulate a new FABS submission
    submission_data = {
        "agency_code": "001",
        "frec_code": "0010",
        "action_type": "B",
        "duns": "123456789",
        "action_date": "2023-01-01",
        "cfda_number": "10.123",
        "ppop_zip": "12345-6789",
        "submitter": "TestUser"
    }
    
    result = broker.process_fabs_submission(submission_data)
    print("Submission Result:", result)
    
    # If successful, attempt to publish
    if result['status'] == 'valid':
        publish_result = broker.publish_fabs_submission(result['submission_id'])
        print("Publish Results:", publish_result)
        
    # Generate summary for UI team
    summary = broker.generate_user_summary()
    print("Summary Report:", summary)
    
    # Sync resource page updates
    design_update = broker.update_resources()
    print("Design Update:", design_update)
    
    # Generate error codes report
    error_report = broker.generate_error_report()
    print("Error Report:", error_report)