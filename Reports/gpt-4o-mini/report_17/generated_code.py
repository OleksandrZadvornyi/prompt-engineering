import datetime
import logging
from typing import List, Dict, Union

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Submission:
    def __init__(self, user_id: int, status: str, data: dict):
        self.user_id = user_id
        self.status = status
        self.data = data
        self.timestamp = datetime.datetime.now()

    def update_status(self, new_status: str):
        logging.info(f"Updating status for submission {self.user_id} from {self.status} to {new_status}")
        self.status = new_status
        self.timestamp = datetime.datetime.now()

class BrokerApplication:
    def __init__(self):
        self.submissions = {}
        self.logging_enabled = True

    def log_submission(self, submission: Submission):
        if self.logging_enabled:
            logging.info(f"Submission logged: {submission.user_id}, Status: {submission.status}")

    def submit_data(self, user_id: int, data: dict) -> Submission:
        submission = Submission(user_id=user_id, status="Pending", data=data)
        self.submissions[user_id] = submission
        self.log_submission(submission)
        return submission

    def update_submission_status(self, user_id: int, new_status: str):
        if user_id in self.submissions:
            self.submissions[user_id].update_status(new_status)

    def validate_submission(self, user_id: int) -> bool:
        if user_id not in self.submissions:
            logging.error("Submission not found.")
            return False
        
        submission = self.submissions[user_id]
        
        if submission.status != "Pending":
            logging.error("Submission status must be Pending to validate.")
            return False

        # Assume some validation rules
        if "required_field" not in submission.data:
            logging.error("Validation failed: missing required_field.")
            return False

        submission.update_status("Validated")
        logging.info("Submission validated successfully.")
        return True

    def delete_submission(self, user_id: int):
        if user_id in self.submissions:
            del self.submissions[user_id]
            logging.info(f"Submission {user_id} deleted successfully.")
        else:
            logging.error(f"Submission {user_id} not found.")

class UserInterface:
    def __init__(self, broker_app: BrokerApplication):
        self.broker_app = broker_app

    def display_submission_status(self, user_id: int):
        if user_id in self.broker_app.submissions:
            submission = self.broker_app.submissions[user_id]
            return f"Submission ID: {user_id}, Status: {submission.status}, Timestamp: {submission.timestamp}"
        else:
            return "Submission not found."

class UXTesting:
    def __init__(self):
        self.test_results = []

    def track_issue(self, issue: str):
        logging.info(f"Tracking issue: {issue}")
        self.test_results.append(issue)

    def generate_summary(self) -> List[str]:
        logging.info("Generating user testing summary.")
        return self.test_results

# Example usage
if __name__ == "__main__":
    broker_app = BrokerApplication()
    ui = UserInterface(broker_app)
    testing = UXTesting()

    # Simulate user actions
    user_submission = broker_app.submit_data(1, {"required_field": "data"})
    print(ui.display_submission_status(1))
    
    broker_app.validate_submission(1)
    print(ui.display_submission_status(1))

    # Track testing issues
    testing.track_issue("Issue with the Resources page design")
    summary = testing.generate_summary()
    print(f"User Testing Summary: {summary}")

    # Delete submission
    broker_app.delete_submission(1)
    print(ui.display_submission_status(1))