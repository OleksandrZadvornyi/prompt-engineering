from datetime import datetime
import json

# Models for our application
class Submission:
    def __init__(self, submission_id, publish_status, data, user_id, created_at=None):
        self.submission_id = submission_id
        self.publish_status = publish_status
        self.data = data
        self.user_id = user_id
        self.created_at = created_at or datetime.now()

    def update_status(self, new_status):
        print(f'Updating submission {self.submission_id} status from {self.publish_status} to {new_status}')
        self.publish_status = new_status
    
    def to_dict(self):
        return {
            "submission_id": self.submission_id,
            "publish_status": self.publish_status,
            "data": self.data,
            "user_id": self.user_id,
            "created_at": self.created_at.isoformat()
        }

class User:
    def __init__(self, user_id, user_type):
        self.user_id = user_id
        self.user_type = user_type

class Database:
    def __init__(self):
        self.submissions = {}
        self.flex_fields = []
        self.historical_data = []

    def add_submission(self, submission):
        self.submissions[submission.submission_id] = submission
    
    def update_historical_data(self, data):
        print('Updating historical data.')
        self.historical_data.append(data)

    def get_submission(self, submission_id):
        return self.submissions.get(submission_id)

    def add_flexfield(self, field):
        self.flex_fields.append(field)

# Logger to record events within the application
class Logger:
    def log(self, message):
        print(f'LOG: {message}')

# Main application logic
class Application:
    def __init__(self):
        self.db = Database()
        self.logger = Logger()

    def submit_fabs(self, submission_id, user_id, data):
        submission = Submission(submission_id=submission_id, publish_status='pending', data=data, user_id=user_id)
        self.db.add_submission(submission)
        self.logger.log(f'Submission {submission_id} created for user {user_id}.')

    def publish_submissions(self):
        for submission in self.db.submissions.values():
            if submission.publish_status == 'pending':
                submission.update_status('published')
                self.logger.log(f'Submission {submission.submission_id} published.')

    def upload_file(self, file_path):
        print(f'Uploading file: {file_path}')

    def run_validations(self, submission_id):
        submission = self.db.get_submission(submission_id)
        if submission:
            # Simulated validation logic
            errors = []
            if not submission.data.get("required_field"):
                errors.append("Missing required field.")
            if errors:
                print(f'Validation errors for submission {submission_id}: {errors}')
            else:
                print(f'Submission {submission_id} passed all validations.')
        else:
            print(f'Submission {submission_id} not found.')

    def display_status(self, submission_id):
        submission = self.db.get_submission(submission_id)
        if submission:
            print(json.dumps(submission.to_dict(), indent=4))

# Example of how these classes may be used in the application context
app = Application()

# Simulating the user submissions
app.submit_fabs(submission_id="SUB12345", user_id="user_1", data={"required_field": "some_data"})
app.publish_submissions()
app.display_status("SUB12345")
app.run_validations("SUB12345")
app.upload_file("path/to/file.csv")