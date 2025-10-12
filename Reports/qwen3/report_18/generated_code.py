import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

class BrokerDatabase:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.setup_tables()

    def setup_tables(self):
        cursor = self.conn.cursor()
        
        # FABS Submission Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fabs_submissions (
                id INTEGER PRIMARY KEY,
                submission_id TEXT UNIQUE,
                status TEXT,
                publish_status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # FABS Records Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fabs_records (
                id INTEGER PRIMARY KEY,
                submission_id TEXT,
                record_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # GTAS Window Data Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gtas_window_data (
                id INTEGER PRIMARY KEY,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                is_locked BOOLEAN DEFAULT FALSE
            )
        ''')
        
        # FABS Validation Rules Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fabs_validation_rules (
                id INTEGER PRIMARY KEY,
                rule_name TEXT,
                description TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # D File Generation Cache Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dfile_generation_cache (
                id INTEGER PRIMARY KEY,
                request_hash TEXT UNIQUE,
                generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                cache_duration INTEGER
            )
        ''')
        
        # DUNS Validation Records Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS duns_validations (
                id INTEGER PRIMARY KEY,
                duns TEXT,
                action_type TEXT,
                action_date TIMESTAMP,
                sam_registered BOOLEAN,
                registered_date TIMESTAMP,
                initial_register_date TIMESTAMP
            )
        ''')
        
        # FPDS Data Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fpds_data (
                id INTEGER PRIMARY KEY,
                data TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Agency Submission Access Logs
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS submission_logs (
                id INTEGER PRIMARY KEY,
                submission_id TEXT,
                user_id TEXT,
                action TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.conn.commit()

    def process_deletions_2017_12_19(self):
        """Process the 2017-12-19 deletions"""
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM fabs_records WHERE created_at < ?", ('2017-12-19',))
        cursor.execute("DELETE FROM fabs_submissions WHERE created_at < ?", ('2017-12-19',))
        self.conn.commit()
        return f"Deleted records older than 2017-12-19"

    def update_fabs_validation_rules(self):
        """Update the FABS validation rule table based on DB-2213"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO fabs_validation_rules 
            VALUES 
            (1, 'rule_123', 'Updated Rule Description for DB-2213'),
            (2, 'rule_456', 'Another updated validation rule')
        """)
        self.conn.commit()
        return "Validation rules updated successfully"

    def add_gtas_window_data(self, start_date: str, end_date: str):
        """Add GTAS window data to the database"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO gtas_window_data 
            (start_date, end_date, is_locked)
            VALUES (?, ?, ?)
        """, (start_date, end_date, False))
        self.conn.commit()
        return f"GTAS window data added from {start_date} to {end_date}"

    def cache_dfile_generation_request(self, request_hash: str, cache_duration_seconds: int):
        """Cache D File generation requests to avoid duplication"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO dfile_generation_cache 
                (request_hash, cache_duration) VALUES (?, ?)
            """, (request_hash, cache_duration_seconds))
            self.conn.commit()
            return True
        except Exception:
            return False

    def get_cached_dfile_request(self, request_hash: str) -> Optional[Dict]:
        """Retrieve cached D File request if exists"""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM dfile_generation_cache WHERE request_hash = ?", 
            (request_hash,)
        )
        result = cursor.fetchone()
        if result:
            return {
                'id': result[0],
                'request_hash': result[1],
                'generated_at': result[2],
                'cache_duration': result[3]
            }
        return None

    def log_submission_action(self, submission_id: str, user_id: str, action: str):
        """Log actions taken on submissions"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO submission_logs (submission_id, user_id, action)
            VALUES (?, ?, ?)
        """, (submission_id, user_id, action))
        self.conn.commit()

    def update_fabs_record_if_publish_status_changed(self, submission_id: str, new_publish_status: str):
        """Update FABS records when publish status changes"""
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE fabs_submissions SET publish_status = ?, updated_at = ?
            WHERE submission_id = ?
        """, (new_publish_status, datetime.now(), submission_id))
        self.conn.commit()

    def validate_duns_record(self, duns: str, action_type: str, action_date: str) -> bool:
        """Validate DUNS records with specific conditions for different action types"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT sam_registered, registered_date, initial_register_date 
            FROM duns_validations 
            WHERE duns = ?
        """, (duns,))
        result = cursor.fetchone()
        
        if not result:
            return False
            
        sam_registered, registered_date, initial_register_date = result
        
        # Check if DUNS is valid based on action type and dates
        if action_type in ['B', 'C', 'D'] and sam_registered and registered_date:
            return True
        elif action_date and registered_date and initial_register_date:
            return register_date > action_date > initial_register_date
        return False

    def is_gtas_locked(self) -> bool:
        """Check whether GTAS window is currently active and locked"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT is_locked FROM gtas_window_data ORDER BY start_date DESC LIMIT 1")
        result = cursor.fetchone()
        return result[0] if result else False if not result else True

    def get_fpds_data(self) -> List[Dict]:
        """Retrieve FPDS data pulled from source"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM fpds_data ORDER BY last_updated DESC")
        results = cursor.fetchall()
        return [
            {'id': row[0], 'data': row[1], 'last_updated': row[2]}
            for row in results
        ]

    def delete_expired_duns_records(self):
        """Delete old or invalid DUNS records"""
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM duns_validations WHERE sam_registered = ?", (False,))
        self.conn.commit()

    def get_published_fabs_file_list(self) -> List[str]:
        """Return list of published FABS file names"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT DISTINCT submission_id 
            FROM fabs_submissions 
            WHERE status='published'
            ORDER BY updated_at DESC
        """)
        return [row[0] for row in cursor.fetchall()]

    def ensure_unique_fabs_submission(self, submission_id: str) -> str:
        """Prevent duplicate FABS submissions"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO fabs_submissions (submission_id, status)
            VALUES (?, ?)
        """, (submission_id, 'pending'))
        self.conn.commit()
        
        if cursor.rowcount == 0:
            # Submission already exists; fetch existing status
            cursor.execute(
                "SELECT status FROM fabs_submissions WHERE submission_id = ?", 
                (submission_id,)
            )
            existing_status = cursor.fetchone()[0]
            return existing_status
        return 'pending'

    def get_fabs_submission_details(self, submission_id: str) -> Optional[Dict]:
        """Get detailed information about FABS submission"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM fabs_submissions WHERE submission_id = ?
        """, (submission_id,))
        result = cursor.fetchone()
        
        if result:
            return {
                'id': result[0],
                'submission_id': result[1],
                'status': result[2],
                'publish_status': result[3],
                'created_at': result[4],
                'updated_at': result[5]
            }
        return None

    def validate_fabs_header_schema(self, header_fields: List[str]) -> bool:
        """Validate that FABS file headers match latest schema version"""
        expected_headers_v11 = ['FundingAgencyCode', 'LegalEntityAddressLine3', 
                              'PPoPCode', 'AwardeeOrRecipientUniqueIdentifier']
        return all(header in expected_headers_v11 for header in header_fields)

    def get_agency_fabs_files(self, agency_code: str) -> List[Dict]:
        """Get all FABS files associated with a particular agency"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT s.submission_id, s.status, s.created_at
            FROM fabs_submissions s
            JOIN fabs_records r ON s.submission_id = r.submission_id
            WHERE r.record_data LIKE ?
            ORDER BY s.created_at DESC
        """, (f"%{agency_code}%",))
        return [
            {'submission_id': row[0], 'status': row[1], 'created_at': row[2]}
            for row in cursor.fetchall()
        ]

    def add_historical_fabs_data(self, records: List[Dict]):
        """Load historical FABS data into the system"""
        cursor = self.conn.cursor()
        for record in records:
            cursor.execute("""
                INSERT INTO fabs_records (submission_id, record_data)
                VALUES (?, ?)
            """, (record.get('id'), str(record)))
        self.conn.commit()
        return f"Loaded {len(records)} historical FABS records"

    def set_user_testing_summary(self, summary: str, author: str):
        """Store user testing summaries"""
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_testing_results (
                id INTEGER PRIMARY KEY,
                summary TEXT,
                author TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            INSERT INTO user_testing_results (summary, author)
            VALUES (?, ?)
        """, (summary, author))
        self.conn.commit()
        return "User testing summary stored"

    def reset_environment_to_staging_max_permissions(self):
        """Reset environment to use staging MAX permissions only"""
        cursor = self.conn.cursor()
        # This would typically integrate with user role management system
        cursor.execute("""
            UPDATE users SET role = 'staging_max' WHERE role IN ('admin', 'tester')
        """)
        self.conn.commit()
        return "Environment reset to staging_MAX permissions"

    def get_submission_creation_info(self, submission_id: str) -> Dict:
        """Identify who created a submission"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT s.submission_id, sl.user_id, sl.timestamp
            FROM fabs_submissions s
            JOIN submission_logs sl ON s.submission_id = sl.submission_id
            WHERE s.submission_id = ?
            ORDER BY sl.timestamp ASC
            LIMIT 1
        """, (submission_id,))
        result = cursor.fetchone()
        if result:
            return {
                'submission_id': result[0],
                'creator': result[1],
                'creation_time': result[2]
            }
        return {}

    def get_fabs_sample_file_link(self) -> str:
        """Provide correct sample file link for FABS submissions"""
        return "https://usaspending.gov/sample/fabs/sample-file-v1.1.csv"

    def derive_funding_agency_code(self, raw_data: Dict) -> str:
        """Derive FundingAgencyCode based on input data"""
        # Simplified logic for demonstration - real implementation would be complex
        if 'agency_code' in raw_data:
            return f"F{raw_data['agency_code'][:8]}"
        return "UNKNOWN_AGENCY"

    def get_error_messages_with_context(self, error_code: str) -> str:
        """Provide detailed context for validation errors"""
        error_descriptions = {
            "CFDA_123": "The CFDA number used is outside the valid range",
            "ZIP_401": "ZIP code has incorrect length or format",
            "DUPLICATE_RECORD": "This submission contains duplicate transaction IDs"
        }
        return error_descriptions.get(error_code, "Generic error explanation")

    def get_raw_fabs_files_from_usaspending(self) -> List[Dict]:
        """Fetch available raw FABS files published via USAspending"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT DISTINCT submission_id FROM fabs_records")
        return [{"filename": row[0] + ".csv"} for row in cursor.fetchall()]

    def create_log_entry(self, user_id: str, description: str):
        """Create detailed log entry for troubleshooting"""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO submission_logs (user_id, action) VALUES (?, ?)
        """, (user_id, description))
        self.conn.commit()

    def cleanup_expired_cache_entries(self):
        """Remove expired entries from cache"""
        cursor = self.conn.cursor()
        cursor.execute("""
            DELETE FROM dfile_generation_cache WHERE cache_duration < strftime('%s', 'now') 
        """)
        self.conn.commit()

    def close(self):
        self.conn.close()