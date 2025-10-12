from datetime import datetime
import random

class Submission:
    def __init__(self, submission_id, publish_status):
        self.submission_id = submission_id
        self.publish_status = publish_status
        self.updated_at = datetime.now()
        self.errors = []

    def update_publish_status(self, new_status):
        self.publish_status = new_status
        self.updated_at = datetime.now()
        print(f"Updated submit ID {self.submission_id} publish status to {new_status}")

    def add_error(self, error_message):
        self.errors.append(error_message)

class FileManager:
    @staticmethod
    def validate_file(file_name):
        if not file_name.endswith(('.csv', '.xls', '.xlsx')):
            raise ValueError("Invalid file format. Please upload a .csv, .xls, or .xlsx file.")
        return True

class Logger:
    @staticmethod
    def log_event(event_message):
        with open("log.txt", "a") as log_file:
            log_file.write(f"{datetime.now()}: {event_message}\n")

class Validation:
    @staticmethod
    def validate_duns(duns):
        if len(duns) == 9 and duns.isdigit():
            return True
        return False

class AgencyUser:
    def __init__(self, user_id):
        self.user_id = user_id

    def submit_data(self, data):
        logger.log_event(f"User {self.user_id} submitted data: {data}")
        # More complex validation and submission logic can be implemented here.

class FABSUser:
    def __init__(self, user_id):
        self.user_id = user_id

    def validate_submission(self, submission):
        # Simulating validation errors
        if submission.errors:
            print(f"Submission {submission.submission_id} has errors: {submission.errors}")
            return False
        return True

class UI_Documentation:
    def __init__(self, title, report):
        self.title = title
        self.report = report

    def share_report(self):
        print(f"Sharing report '{self.title}': {self.report}")

class DataUser:
    def receive_updates(self, records):
        print(f"Received updates for FABS records: {records}")
        
class DevOps:
    @staticmethod
    def integrate_new_relic(data):
        print(f"Integrating New Relic with the following data: {data}")

if __name__ == "__main__":
    # Example usage:
    logger = Logger()
    submission = Submission(submission_id=1, publish_status="Pending")
    
    # Update status and log it
    submission.update_publish_status("Published")
    logger.log_event("Submission status updated.")

    # Validate a file
    file_manager = FileManager()
    try:
        file_manager.validate_file("submission_file.csv")
    except ValueError as e:
        logger.log_event(str(e))

    # Validate DUNS number
    duns = "123456789"
    if Validation.validate_duns(duns):
        print(f"DUNS {duns} is valid.")
    else:
        print(f"DUNS {duns} is not valid.")
    
    # A user submits data
    agency_user = AgencyUser("user_01")
    agency_user.submit_data({"name": "Sample Agency", "amount": 1000})

    # FABS user validates a submission
    fabs_user = FABSUser("fabs_user_01")
    fabs_user.validate_submission(submission)
    
    # DevOps integration
    devops = DevOps()
    devops.integrate_new_relic({"app": "Broker", "metric": "response_time"})