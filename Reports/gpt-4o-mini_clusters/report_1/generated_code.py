import datetime

class Submission:
    def __init__(self, user_id, status='pending'):
        self.user_id = user_id
        self.submission_time = datetime.datetime.now()
        self.status = status
        self.errors = []
        
    def validate(self):
        # Dummy validation logic
        if self.status == 'pending':
            return True
        else:
            self.errors.append("Submission is not pending.")
            return False

class FABSUser:
    def __init__(self, user_id):
        self.user_id = user_id
        self.submissions = []

    def submit_record(self, submission):
        if submission.validate():
            self.submissions.append(submission)
            submission.status = 'submitted'
            print(f"Submission successful for user {self.user_id}!")
        else:
            print(f"Submission failed: {submission.errors}")

    def upload_and_validate_errors(self):
        for submission in self.submissions:
            if submission.errors:
                print(f"Errors for submission {submission.user_id}: {submission.errors}")
            else:
                print(f"No errors for submission {submission.user_id}.")

class Developer:
    def __init__(self):
        pass

    def update_validation_rule_table(self, updates):
        # Assume updates is a dictionary of rules
        print(f"Updated validation rules: {updates}")

    def log_submission(self, submission):
        print(f"Logging submission for user {submission.user_id} with status {submission.status}.")

    def determine_data_loading_methods(self):
        print("Determining the best way to load historical FPDS data.")

class AgencyUser:
    def __init__(self, user_id):
        self.user_id = user_id

    def check_submission_period(self):
        start_date = datetime.datetime(2023, 10, 1)
        end_date = datetime.datetime(2023, 10, 31)
        now = datetime.datetime.now()
        
        if start_date <= now <= end_date:
            print("Submission period is open.")
        else:
            print("Submission period is closed.")

    def submit_fabs_data(self, fabs_data):
        # Assume fabs_data is properly formatted
        print(f"Submitting FABS data for agency user {self.user_id}")

class UIUser:
    def __init__(self):
        pass

    def create_content_mockup(self):
        print("Creating content mockup.")

    def request_user_testing(self):
        print("Requesting user testing.")

if __name__ == "__main__":
    # Example usage based on user stories
    
    # Create users
    fabs_user = FABSUser(user_id=1)
    developer = Developer()
    agency_user = AgencyUser(user_id=101)
    ui_user = UIUser()

    # Simulating submission process
    submission = Submission(user_id=1)
    fabs_user.submit_record(submission)
    fabs_user.upload_and_validate_errors()

    # Update validation rules
    developer.update_validation_rule_table({'CFDA': 'New rule for CFDA validation'})

    # Check submission period
    agency_user.check_submission_period()

    # Submit FABS data
    agency_user.submit_fabs_data({'key': 'value'})

    # Create a content mockup
    ui_user.create_content_mockup()