from datetime import datetime
import json
import logging

# Setup for logging
logging.basicConfig(level=logging.INFO)

class Submission:
    def __init__(self):
        self.publish_status = None
        self.last_updated = datetime.now()
        self.errors = []
    
    def update_status(self, status):
        self.publish_status = status
        self.last_updated = datetime.now()
        logging.info(f'Submission status updated to: {self.publish_status} at {self.last_updated}')

    def validate(self, data):
        # Example validation logic
        if 'DUNS' not in data or not data['DUNS']:
            self.errors.append("DUNS is a required field.")
        if 'ActionType' in data and data['ActionType'] not in ['B', 'C', 'D']:
            self.errors.append(f"Invalid ActionType: {data['ActionType']}")
        if self.errors:
            logging.info(f'Validation errors: {self.errors}')

class FileUploader:
    def __init__(self):
        self.files = []

    def upload_file(self, file):
        if file.endswith('.csv'):
            self.files.append(file)
            logging.info(f'File uploaded: {file}')
        else:
            logging.error('File upload failed: incorrect file format.')

class AgencyUser:
    def __init__(self):
        self.submissions = []

    def create_submission(self, data):
        submission = Submission()
        submission.validate(data)
        self.submissions.append(submission)

class UIReport:
    def __init__(self):
        self.reports = []

    def create_report(self, feedback):
        report = {
            "created_at": datetime.now(),
            "feedback": feedback
        }
        self.reports.append(report)
        logging.info(f"UI Report created: {feedback}")

class FlexFieldManager:
    def __init__(self):
        self.flexfields = []

    def add_flexfield(self, field):
        self.flexfields.append(field)
        logging.info(f'Flexfield added: {field}')

    def validate_flexfields(self, submission):
        performance_issue = False  # This should be determined by actual logic
        if performance_issue:
            logging.warning("Including a large number of flexfields may impact performance.")

class BrokerDataHandler:
    @staticmethod
    def generate_d1_file(data):
        # Pseudo synchronization with FPDS data
        if data.get('updated'):
            logging.info("D1 file generated with updated data.")
        else:
            logging.info("No changes detected. D1 file generation skipped.")

class FileDownloader:
    @staticmethod
    def download_file(filename):
        logging.info(f"Downloading file: {filename}")
        return f"{filename} content"

# Example use cases based on user stories
if __name__ == "__main__":
    agency_user = AgencyUser()
    uploader = FileUploader()
    report = UIReport()
    flexfield_manager = FlexFieldManager()
    
    # Sample data for a submission
    sample_data = {
        "DUNS": "123456789",
        "ActionType": "B"
    }
    
    # Creating a submission
    agency_user.create_submission(sample_data)
    
    # Feedback for UI report
    report.create_report("User testing was insightful and will guide the next design phase.")
    
    # Adding a flexfield and validating performance
    flexfield_manager.add_flexfield("NewFlexField1")
    
    # Uploading a file
    uploader.upload_file("submission_data.csv")
    
    # Generating D1 file
    BrokerDataHandler.generate_d1_file({'updated': True})
    
    # Downloading a file
    content = FileDownloader.download_file("submission_data.csv")
    print(content)