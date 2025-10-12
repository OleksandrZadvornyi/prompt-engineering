from datetime import datetime
import logging

# Setup basic configuration for logging
logging.basicConfig(level=logging.INFO)

class AgencyUser:
    def __init__(self, name):
        self.name = name

    def submit_data_elements(self, data_elements):
        quoted_data = [f'"{elem}"' for elem in data_elements]
        logging.info(f"Data submitted by {self.name}: {quoted_data}")
        return quoted_data

    def include_flexfields(self, fields):
        if len(fields) > 100:
            raise ValueError("Too many flexfields included!")
        logging.info(f"Included flexfields by {self.name}: {fields}")
        return fields

    def receive_file_error(self, file_extension):
        if file_extension not in ['.csv', '.xlsx']:
            logging.warning(f"Error: Invalid file extension {file_extension} uploaded.")
            return "Please upload a file with a .csv or .xlsx extension."
        return "File uploaded successfully."

    def get_submission_periods(self):
        logging.info(f"{self.name} is checking submission periods.")
        return {"start": "2023-01-01", "end": "2023-12-31"}


class FabsUser(AgencyUser):
    def upload_file(self, file_path):
        logging.info(f"{self.name} uploaded file: {file_path}")
        
    def validation_errors(self, submission_data):
        errors = []
        if not submission_data.get('DUNS'):
            errors.append("Missing DUNS number.")
        if submission_data.get('ActionType') not in ['B', 'C', 'D']:
            errors.append("Invalid ActionType specified.")
        if errors:
            logging.error(f"Validation errors for {self.name}: {errors}")
            return errors
        return "Submission is valid."


class Developer:
    def __init__(self, name):
        self.name = name

    def log_submission(self, submission_status, submission_id):
        logging.info(f"{submission_id}: {submission_status} logged by {self.name}")

    def prevent_duplicate_publish(self, submission_id):
        logging.info(f"Checked for duplicates on submission: {submission_id}")
        return True  # Assume no duplicates

    
class UIUser:
    def __init__(self, name):
        self.name = name

    def schedule_testing(self, date):
        logging.info(f"{self.name} scheduled user testing on {date}.")
        return date


class BrokerUser(AgencyUser):
    def upload_and_validate(self, file_path):
        logging.info(f"{self.name} uploaded and validated file: {file_path}")

    def get_submission_history(self):
        logging.info(f"{self.name} is retrieving submission history.")
        # This would typically involve a database query
        return []

      
class Owner:
    def __init__(self, name):
        self.name = name

    def create_user_testing_summary(self, feedback):
        logging.info(f"User testing summary created by {self.name}: {feedback}")
        return feedback


class DevOpsEngineer:
    def __init__(self, name):
        self.name = name

    def ensure_integration_with_new_relic(self):
        logging.info(f"{self.name} ensured New Relic integration for monitoring.")
        return True


# Example usage
agency_user = AgencyUser(name="John Doe")
fabs_user = FabsUser(name="Jane Roe")
developer = Developer(name="Dev Team")
ui_user = UIUser(name="Design Team")
broker_user = BrokerUser(name="Alice")
owner = Owner(name="Bob")
dev_ops = DevOpsEngineer(name="Charlie")

# Simulate user stories
agency_user.include_flexfields(['Field1', 'Field2', 'Field3'])
fabs_user.upload_file("datafile.csv")
validation_results = fabs_user.validation_errors({'DUNS': '', 'ActionType': 'A'})
developer.log_submission("New Submission", "SUB123")
owner.create_user_testing_summary("Positive feedback on new interface")
dev_ops.ensure_integration_with_new_relic()