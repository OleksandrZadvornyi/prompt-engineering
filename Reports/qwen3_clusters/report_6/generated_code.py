import sqlite3
from datetime import datetime, timedelta
import json
from typing import Dict, List, Optional

class BrokerDatabase:
    def __init__(self, db_path: str = "broker.db"):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables for Broker system
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS submissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                submission_type TEXT,
                status TEXT,
                publish_status TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                agency_code TEXT,
                file_name TEXT,
                error_message TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS validations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id TEXT,
                description TEXT,
                category TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS validation_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_text TEXT,
                explanation TEXT,
                updated_date DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gtas_window (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time DATETIME,
                end_time DATETIME
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ppop_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                zip_plus_four TEXT,
                zip_five TEXT,
                congressional_district TEXT,
                county_code TEXT,
                state_code TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fabs_submissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                submission_id INTEGER,
                funding_agency_code TEXT,
                frec_code TEXT,
                file_name TEXT,
                publication_status TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(submission_id) REFERENCES submissions(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fpds_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id TEXT,
                agency_code TEXT,
                action_date DATE,
                duns_number TEXT,
                file_name TEXT,
                loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS historical_fabs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_content TEXT,
                loading_date DATE,
                status TEXT,
                frec_derived TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS d_files_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                submission_id INTEGER,
                generation_request DATETIME DEFAULT CURRENT_TIMESTAMP,
                cache_key TEXT UNIQUE,
                generated_file TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS flex_fields (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                submission_id INTEGER,
                field_name TEXT,
                field_value TEXT,
                is_required BOOLEAN
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def process_deletions_2017_12_19(self):
        """Process deletions from 12-19-2017"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Simulate deletion processing - remove submissions older than 3 months
        three_months_ago = datetime.now() - timedelta(days=90)
        cursor.execute("DELETE FROM submissions WHERE created_at < ?", (three_months_ago,))
        
        conn.commit()
        conn.close()
        
    def update_validation_rules(self):
        """Update validation rule table based on DB-2213 changes"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Sample data to insert/update rules
        rules = [
            ("DB2213-01", "Invalid zip code format for PPoP", "validation"),
            ("DB2213-02", "Incorrect FABS header format", "headers"),
            ("DB2213-03", "CFDA code must be 4 digit numeric", "data_quality")
        ]
        
        for rule_id, description, category in rules:
            cursor.execute(
                "INSERT OR REPLACE INTO validation_rules (rule_text, explanation, updated_date) VALUES (?, ?, CURRENT_TIMESTAMP)",
                (rule_id, description)
            )
            
        conn.commit()
        conn.close()
    
    def load_historical_fabs_data(self, file_content: str, loading_date: str):
        """Load historical FABS data with FREC derivation"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Insert the file's content with automatic FREC derivation placeholder
        cursor.execute(
            "INSERT INTO historical_fabs (file_content, loading_date, status, frec_derived) VALUES (?, ?, 'loaded', ?)",
            (file_content, loading_date, self.derive_frec_code(file_content))
        )
        
        conn.commit()
        conn.close()
    
    def derive_frec_code(self, file_content: str) -> str:
        """Derive FREC code from file content"""
        # Simulated derivation logic
        return "008-0001"  # Placeholder
    
    def add_gtas_window(self, start_time: datetime, end_time: datetime):
        """Add GTAS window data to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "INSERT INTO gtas_window (start_time, end_time) VALUES (?, ?)",
            (start_time, end_time)
        )
        
        conn.commit()
        conn.close()

class FABSSubmissionManager:
    def __init__(self, db_manager: BrokerDatabase):
        self.db = db_manager  # Use BrokerDatabase instance
        
    def handle_publish_status_change(self, submission_id: int, old_status: str, new_status: str):
        """Handle FABS submission status changes"""
        if new_status == 'published' and old_status != 'published':
            # Update publish timestamp only when changing to 'published'
            conn = sqlite3.connect(self.db.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "UPDATE submissions SET publish_status = ? WHERE id = ?",
                (new_status, submission_id)
            )
            conn.commit()
            conn.close()
            return True
        return False
    
    def deactivate_publish_button(self, submission_id: int):
        """Deactivate publish button when derivation is active"""
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()
        
        # Mark submission as processing (simulated)
        cursor.execute(
            "UPDATE submissions SET status = 'processing' WHERE id = ?", 
            (submission_id,)
        )
        
        conn.commit()
        conn.close()
        
    def create_d_file(self, submission_id: int) -> Optional[str]:
        """Generate D file (simulated)"""
        # Check if request already exists
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT cache_key FROM d_files_cache WHERE submission_id = ?",
            (submission_id,)
        )
        
        existing = cursor.fetchone()
        
        if existing:
            return existing[0]  # Return cached result
            
        # Generate new file and cache it
        cache_key = f"d_file_{submission_id}_{datetime.now().timestamp()}"
        file_content = f"Generated D file for submission {submission_id}"
        
        cursor.execute(
            "INSERT INTO d_files_cache (submission_id, cache_key, generated_file) VALUES (?, ?, ?)",
            (submission_id, cache_key, file_content)
        )
        
        conn.commit()
        conn.close()
        
        return cache_key
    
    def validate_submission_errors(self, submission_id: int) -> str:
        """Validate submission errors to be FABS-specific"""
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()
        
        # Get the submission details
        cursor.execute("SELECT * FROM submissions WHERE id = ?", (submission_id,))
        submission = cursor.fetchone()
        
        conn.close()
        
        if not submission:
            return "Submission does not exist"
            
        error_codes_map = {
            1: "Invalid ZIP code format",
            2: "Required fields missing",
            3: "Agency code incorrect",
            4: "CFDA code invalid"
        }
        
        # Simulate FABS-specific error check
        sample_error = error_codes_map.get(1, "FABS Validation error")
        return f"FABS: {sample_error}"

class PPoPZipValidation:
    def __init__(self, db_manager: BrokerDatabase):
        self.db = db_manager
    
    def validate_ppop_zip(self, zip_code: str) -> bool:
        """Validate PPoP ZIP+4 format similar to legal entity ZIP validation"""
        # Accept both zip+4 and 5-digit ZIP
        if len(zip_code) in [5, 9]:
            return True
        elif len(zip_code) in [4, 6, 7, 8]:  # Allow leaving out last 4 digits
            try:
                int(zip_code)
                return True
            except ValueError:
                return False
        return False
    
    def save_ppop_zip_data(self, zip_plus_four: str, zip_five: str, congressional_district: str):
        """Save PPoP ZIP data in proper format"""
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "INSERT INTO ppop_data (zip_plus_four, zip_five, congressional_district) VALUES (?, ?, ?)",
            (zip_plus_four, zip_five, congressional_district)
        )
        
        conn.commit()
        conn.close()

class FABSFileDownloader:
    def __init__(self, db_manager: BrokerDatabase):
        self.db = db_manager
    
    def save_uploaded_fabs_file(self, submission_id: int, file_content: str, filename: str):
        """Store uploaded FABS file in database"""
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "INSERT INTO fabs_submissions (submission_id, file_name, publication_status) VALUES (?, ?, ?)",
            (submission_id, filename, 'uploaded')
        )
        
        conn.commit()
        conn.close()

class UserTestingTracker:
    def __init__(self, db_manager: BrokerDatabase):
        self.db = db_manager
    
    def create_user_testing_log(self, test_description: str, tester_name: str, test_date: datetime):
        """Track user testing activities"""
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()
        
        # Store as metadata in DB or integrate with tracking system
        cursor.execute(
            "INSERT INTO submissions (agency_code, file_name, status) VALUES (?, ?, ?)",
            (tester_name, f"Test: {test_description}", "user_test")
        )
        
        conn.commit()
        conn.close()
    
    def get_ui_sme_summary(self) -> Dict:
        """Get summary of UI SME feedback"""
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()
        
        # Simulate retrieving summary from UI testing
        cursor.execute("SELECT COUNT(*) FROM submissions WHERE status LIKE '%user_test%'")
        total_tests = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "total_user_tests": total_tests,
            "feedback_summary": "UI improvements validated"
        }

class DataUser:
    def __init__(self, db_manager: BrokerDatabase):
        self.db = db_manager
    
    def fetch_fpds_updates(self) -> List[Dict]:
        """Fetch additional FPDS data fields"""
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()
        
        # Simulate fetching FPDS data 
        cursor.execute("""
            SELECT transaction_id, agency_code, action_date, duns_number, file_name  
            FROM fpds_data 
            ORDER BY loaded_at DESC LIMIT 10
        """)
        
        results = []
        for row in cursor.fetchall():
            results.append({
                "transaction_id": row[0],
                "agency_code": row[1],
                "action_date": row[2],
                "duns_number": row[3],
                "filename": row[4]
            })
        
        conn.close()
        return results

# Example usage
if __name__ == "__main__":
    db = BrokerDatabase()
    fabs_mngr = FABSSubmissionManager(db)
    ppop_validator = PPoPZipValidation(db)
    test_tracker = UserTestingTracker(db)
    data_user = DataUser(db)
    
    # Test cluster 4 features
    db.process_deletions_2017_12_19()
    db.update_validation_rules()
    
    # Test cluster 2 and 5 combined features
    test_tracker.create_user_testing_log(
        "PPoPZIP validation test", 
        "UI SME", 
        datetime.now()
    )
    
    # Test cluster 3 features
    ppop_validator.save_ppop_zip_data("12345", "12345", "01")
    
    # Show data fetch
    print(data_user.fetch_fpds_updates())