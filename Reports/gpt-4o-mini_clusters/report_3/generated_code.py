import datetime
from typing import List, Dict
import logging

# Set up logging for better monitoring and troubleshooting
logging.basicConfig(level=logging.INFO)

class BrokerSystem:
    def __init__(self):
        self.data_entries = []
        self.validations = []
        self.history = []
    
    def upload_and_validate(self, entry: Dict[str, str]) -> List[str]:
        errors = []
        # Validate DUNS and other required fields
        if not self.validate_duns(entry.get('DUNS')):
            errors.append("Invalid DUNS number.")
        if not self.validate_required_fields(entry):
            errors.append("Missing required fields.")
        if errors:
            logging.error(f"Validation errors: {errors}")
        else:
            self.data_entries.append(entry)
            logging.info("Entry uploaded successfully.")
        return errors
    
    def validate_duns(self, duns: str) -> bool:
        # Example validation logic for DUNS
        return duns.isdigit() and len(duns) == 9

    def validate_required_fields(self, entry: Dict[str, str]) -> bool:
        required_fields = ['DUNS', 'FundingAgencyCode', 'ActionType']
        return all(field in entry and entry[field] for field in required_fields)

    def update_validation_rules(self, rule_updates: List[str]) -> None:
        self.validations.extend(rule_updates)
        logging.info("Validation rules updated.")

    def generate_d_files(self, submissions: List[Dict[str, str]]) -> None:
        for submission in submissions:
            status = self.process_submission(submission)
            logging.info(f"Processed submission with status: {status}")
            self.history.append((submission, status))

    def process_submission(self, submission: Dict[str, str]) -> str:
        # Mock processing logic
        if submission.get('publishStatus') == 'Published':
            return "Submission Published"
        return "Error in Submission"

    def derive_office_name(self, office_code: str) -> str:
        # Mocked mapping of office codes to names
        office_map = {
            "100": "Office of Management",
            "200": "Office of Analytics"
        }
        return office_map.get(office_code, "Unknown Office")

    def sync_data_with_fpds(self) -> None:
        # Mock syncing process
        logging.info("Data synced with FPDS.")

    def log_submission_errors(self, submission_id: str, error_message: str) -> None:
        logging.error(f"Submission ID: {submission_id} failed due to: {error_message}")

    def get_submission_history(self) -> List[Dict[str, str]]:
        return [{
            "submission": submission,
            "status": status
        } for submission, status in self.history]

# Example usage scenario
if __name__ == "__main__":
    broker_system = BrokerSystem()
    
    # Sample data upload
    sample_submission = {
        "DUNS": "123456789",
        "FundingAgencyCode": "100",
        "ActionType": "B",
        "Amount": "10000.00"
    }
    
    validation_errors = broker_system.upload_and_validate(sample_submission)
    if not validation_errors:
        broker_system.generate_d_files([sample_submission])
    else:
        for error in validation_errors:
            print(error)