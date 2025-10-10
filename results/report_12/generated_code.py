from datetime import datetime, timedelta
from typing import List, Dict, Optional
import hashlib
import json
import os

# Mock classes for demonstration purposes
class Submission:
    def __init__(self, submission_id: str, status: str = 'draft', publish_status: str = 'unpublished'):
        self.submission_id = submission_id
        self.status = status  # draft, validated, submitted, published
        self.publish_status = publish_status  # unpublished, pending_publish, published
        self.timestamp = datetime.now()
        self.errors = []
        self.warnings = []
    
    def update_status(self, status: str):
        self.status = status
        
    def update_publish_status(self, status: str):
        self.publish_status = status

class User:
    def __init__(self, user_id: str, role: str):
        self.user_id = user_id
        self.role = role  # developer, ui_designer, agency_user, broker_user, owner, tester
        self.permissions = []

class File:
    def __init__(self, file_id: str, content: str, file_type: str):
        self.file_id = file_id
        self.content = content
        self.file_type = file_type  # FABS, DABS, etc.
        self.created_at = datetime.now()
        self.updated_at = datetime.now()

class DatabaseConnection:
    def __init__(self):
        self.submissions = {}
        self.users = {}
        self.files = {}
        
    def save_submission(self, submission: Submission):
        self.submissions[submission.submission_id] = submission
        
    def save_file(self, file: File):
        self.files[file.file_id] = file
        
    def find_submission(self, submission_id: str) -> Optional[Submission]:
        return self.submissions.get(submission_id)
        
    def find_user(self, user_id: str) -> Optional[User]:
        return self.users.get(user_id)

class BrokerDB:
    def __init__(self):
        self.db = DatabaseConnection()
        
    def process_deletions_2017_12_19(self):
        """Process the deletions mentioned in user story"""
        print("Processing deletions from 12-19-2017")
        # In a real implementation, this would involve actual DB queries and updates
        # For now, just logging
        return {"message": "Deletions processed successfully"}
        
    def update_validation_rules(self, rule_updates):
        """Update Broker validation rule table"""
        print(f"Updating validation rules with: {rule_updates}")
        return {"message": "Validation rules updated successfully", "rules": rule_updates}
    
    def add_gtas_window_data(self, gtas_data):
        """Add GTAS window data to database"""
        print(f"Adding GTAS window data: {gtas_data}")
        # Placeholder for actual database insert
        return {"message": "GTAS data added successfully"}
        
    def generate_d_files(self, request_params):
        """Manage D Files generation requests"""
        cache_key = hashlib.md5(str(request_params).encode()).hexdigest()
        print(f"Checking cache for D file generation request: {cache_key}")
        # Placeholder for caching logic
        result = {"generated": True, "cache_key": cache_key, "request": request_params}
        return result
    
    def derive_fields_for_historical_fabs(self, file_content):
        """Derive fields for historical FABS loader"""
        print("Deriving fields for historical FABS data")
        # Simulate field derivation
        derived_data = {
            "office_names": ["Office A", "Office B"],
            "agency_codes": ["AGENCY1", "AGENCY2"]
        }
        return derived_data
    
    def derive_frec_data(self, fabs_records):
        """Include FREC derivations in historical FABS data"""
        print("Applying FREC derivations to historical data")
        return [record.update({"frec_code": record.get("agency_code", "")[:4]}) for record in fabs_records] if isinstance(fabs_records, list) else []

    def update_fabs_sample_file(self):
        """Remove FundingAgencyCode from FABS sample file"""
        print("Removing FundingAgencyCode from FABS sample file")
        return {"message": "Sample file updated", "changes_made": "FundingAgencyCode removed"}

class ValidationService:
    def __init__(self):
        self.rules = {
            "PPoPZIP": self.validate_ppop_zip,
            "DUNS": self.validate_duns
        }
        
    def validate_ppop_zip(self, zip_data):
        if len(zip_data) == 5 or len(zip_data) == 9:
            return {'valid': True, 'error': None}
        else:
            return {'valid': False, 'error': 'Invalid ZIP code format'}
            
    def validate_duns(self, duns_record):
        actions_supported = ['B', 'C', 'D']
        action_type = duns_record.get('action_type', '')
        if action_type in actions_supported:
            return {'valid': True, 'error': None}
        else:
            return {'valid': False, 'error': f'Action type {action_type} not supported for DUNS registration'}

class FABSSubmissionService:
    def __init__(self):
        self.db = BrokerDB()
        self.validation_service = ValidationService()
        
    def save_submission(self, submission_data):
        """Save a new FABS submission"""
        submission = Submission(
            submission_id="SUBMISSION_" + str(hashlib.sha256(str(datetime.now()).encode()).hexdigest()[:8]),
            status='draft'
        )
        submission.update_status('validated')
        self.db.db.save_submission(submission)
        return {
            "message": "Submission saved",
            "submission_id": submission.submission_id,
            "status": submission.status
        }
        
    def publish_submission(self, submission_id: str):
        """Publish a FABS submission"""
        submission = self.db.db.find_submission(submission_id)
        if not submission:
            return {"error": "Submission not found"}
            
        if submission.publish_status == "published":
            return {"error": "Submission already published"}
            
        # Simulate derivation processing
        submission.update_publish_status("pending_publish")
        # ... more logic ...
        submission.update_publish_status("published")
        return {"message": f"Submission {submission_id} published successfully"}
        
    def generate_and_validate_d_files(self, data):
        """Generate D Files and perform validations"""
        # This would process FABS and FPDS data
        print("Generating D Files...")
        result = self.db.generate_d_files(data)
        return result
        
    def upload_and_validate(self, file_content: str, filename: str):
        """Upload and validate FABS file"""
        # Validate file extension
        valid_extensions = ['.csv', '.json']
        if not any(filename.lower().endswith(ext) for ext in valid_extensions):
            return {"error": "Invalid file extension. Please upload a CSV or JSON file."}
            
        # Simulate validation
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "row_count": 100
        }
        return validation_result
        
    def get_submission_dashboard_info(self, submission_id: str):
        """Get detailed submission information including status"""
        submission = self.db.db.find_submission(submission_id)
        if not submission:
            return {"error": "Submission not found"}
            
        dashboard_info = {
            "submission_id": submission_id,
            "status": submission.status,
            "publish_status": submission.publish_status,
            "timestamp": submission.timestamp.isoformat(),
            "errors": submission.errors,
            "warnings": submission.warnings
        }
        return dashboard_info

class UserService:
    def __init__(self):
        self.db = BrokerDB()
        
    def create_new_user(self, user_id: str, role: str):
        """Create a new user"""
        user = User(user_id=user_id, role=role)
        self.db.db.users[user_id] = user
        return user
        
    def get_user_permissions(self, user_id: str):
        """Get user's permissions based on role"""
        user = self.db.db.find_user(user_id)
        if not user:
            return {"error": "User not found"}
            
        permissions = {
            "developer": ["read", "write", "update_validations", "manage_submissions"],
            "ui_designer": ["design_ui", "test_ui", "review_requirements"],
            "agency_user": ["submit_data", "view_submissions", "download_files"],
            "owner": ["reset_environment", "grant_access", "audit_changes"],
            "tester": ["access_nonprod_env", "test_features"]
        }
        
        return permissions.get(user.role, [])

class FileManagementService:
    def __init__(self):
        self.db = BrokerDB()
        
    def get_published_fabs_files(self):
        """Access published FABS files"""
        # Returns list of files accessible via USAspending
        return [
            {"name": "Published_FABS_2023.csv", "date": "2023-01-15"},
            {"name": "Published_FABS_2023.json", "date": "2023-01-16"}
        ]
        
    def get_raw_agency_files(self):
        """Access raw agency published files from FABS"""
        return [
            {"file_name": "agency_data_abc.csv", "size": "2.4MB", "updated": "2023-02-10"},
            {"file_name": "agency_data_xyz.xlsx", "size": "3.1MB", "updated": "2023-02-14"}
        ]

class NotificationService:
    def __init__(self):
        self.notifications = []
        
    def send_notification(self, recipient, message, timestamp=None):
        notification = {
            "recipient": recipient,
            "message": message,
            "timestamp": timestamp or datetime.now()
        }
        self.notifications.append(notification)
        return notification

# Implementation classes for various user stories
class FABSWorkflow:
    def __init__(self):
        self.fabs_service = FABSSubmissionService()
        self.file_service = FileManagementService()
        self.user_service = UserService()
        
    def handle_fabs_submission_validation(self, file_content: str, filename: str):
        """Broker user wants to Upload and Validate the error message to have accurate text"""
        return self.fabs_service.upload_and_validate(file_content, filename)
        
    def handle_publish_status_update(self, submission_id: str):
        """Developer wants to add updates to FABS submission when publishStatus changes"""
        return self.fabs_service.publish_submission(submission_id)
        
    def get_published_files(self):
        """Website user wants to access published FABS files"""
        return self.file_service.get_published_fabs_files()
        
    def handle_data_loading(self):
        """Ensure proper data loading from historical FABS"""
        # This represents handling historical data loading
        historical_data = [
            {"award_id": "A123", "agency_code": "ABC"},
            {"award_id": "B456", "agency_code": "XYZ"}
        ]
        
        # Derive FREC data
        derived_data = self.fabs_service.db.derive_frec_data(historical_data)
        return {
            "historical_data": historical_data,
            "derived_frec_data": derived_data
        }

class UIImprovementTracker:
    def __init__(self):
        self.ui_notes = []
        self.testing_schedules = []
        
    def track_tech_thursday_issues(self, issue_summary: str):
        """UI designer tracks issues from Tech Thursday"""
        self.ui_notes.append({
            "issue": issue_summary,
            "category": "tech_thursday",
            "timestamp": datetime.now()
        })
        return {"message": "Issue tracked successfully"}
        
    def schedule_user_testing(self, test_details: dict):
        """Schedule user testing for better UX validation"""
        self.testing_schedules.append({
            "details": test_details,
            "scheduled_date": datetime.now() + timedelta(days=5),
            "status": "scheduled"
        })
        return {"message": "Testing scheduled", "schedule": self.testing_schedules[-1]}

class SystemMaintenance:
    def __init__(self):
        self.db = BrokerDB()
        
    def reset_to_staging_max(self):
        """Reset environment to only take Staging MAX permissions"""
        return {"message": "Environment reset to staging max permissions", "status": "completed"}
        
    def check_system_performance(self):
        """Ensure New Relic provides useful data"""
        return {"message": "System monitored for performance metrics", "data_points": 5}

# Main application setup
def create_broker_application():
    app = {}
    
    # Core services
    app['fabs_workflow'] = FABSWorkflow()
    app['ui_tracker'] = UIImprovementTracker()
    app['system_maintenance'] = SystemMaintenance()
    app['notification_service'] = NotificationService()
    
    return app

# Example usage demonstrating implementation
if __name__ == "__main__":
    # Create the application components
    broker_app = create_broker_application()
    
    # Demonstrate processing deletions
    print("1. Processing deletions from 12-19-2017:")
    deletion_result = broker_app['fabs_workflow'].db.process_deletions_2017_12_19()
    print(deletion_result)
    print()
    
    # Demonstrate publishing a submission
    print("2. Publishing submission:")
    submission_result = broker_app['fabs_workflow'].fabse_service.save_submission({
        "agency_code": "ABC123",
        "award_amount": 100000
    })
    print(submission_result)
    print()
    
    # Demonstrate uploading and validating
    print("3. Uploading and validating FABS file:")
    validate_result = broker_app['fabs_workflow'].handle_fabs_submission_validation(
        "sample,csv,content", 
        "test.csv"
    )
    print(validate_result)
    print()
    
    # Demonstrate publishing status change
    print("4. Publish status changes:")
    pub_status = broker_app['fabs_workflow'].handle_publish_status_update("SUBMISSION_12345678")
    print(pub_status)
    print()
    
    # Demonstrate tracking UI issues from Tech Thursday
    print("5. Tracking Tech Thursday issues:")
    tech_issue_result = broker_app['ui_tracker'].track_tech_thursday_issues("Bug in dropdown menus")
    print(tech_issue_result)
    print()
    
    # Demonstrate scheduling user testing
    print("6. Scheduling user testing:")
    testing_schedule = broker_app['ui_tracker'].schedule_user_testing({
        "topic": "UI Improvements",
        "participants": ["Design Team", "UX Researcher"],
        "deadline": "2023-05-01"
    })
    print(testing_schedule)
    
    # Print sample notifications
    print("\n7. Notifications sample:")
    notification_service = broker_app['notification_service']
    notif1 = notification_service.send_notification(
        "admin@example.com",
        "New submission requires review"
    )
    notif2 = notification_service.send_notification(
        "ui_team@example.com",
        "Tech Thursday issues reported"
    )
    print(notif1)
    print(notif2)