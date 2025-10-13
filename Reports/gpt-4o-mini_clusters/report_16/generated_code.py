import datetime

class SubmissionStatus:
    def __init__(self):
        self.submissions = {}

    def submit_data(self, agency_id, data):
        submission_id = len(self.submissions) + 1
        self.submissions[submission_id] = {
            'agency_id': agency_id,
            'data': data,
            'status': 'Pending',
            'timestamp': datetime.datetime.now()
        }
        return submission_id

    def update_status(self, submission_id, status):
        if submission_id in self.submissions:
            self.submissions[submission_id]['status'] = status
    
    def get_submission_status(self, submission_id):
        return self.submissions.get(submission_id, None)

class Validator:
    @staticmethod
    def validate_submission(data):
        errors = []
        if not data.get('DUNS'):
            errors.append("DUNS is required.")
        if len(data.get('PPoPZIP', '')) < 5:
            errors.append("PPoPZIP must have at least 5 digits.")
        if 'agency_code' not in data:
            errors.append("Agency code is required.")

        return errors
    
    @staticmethod
    def validate_file_extension(filename):
        if not filename.endswith(('.csv', '.txt')):
            return "Invalid file extension. Only .csv and .txt files are allowed."
        return None

class UserTesting:
    def __init__(self):
        self.test_responses = []
    
    def record_response(self, response):
        self.test_responses.append(response)

    def summarize_tests(self):
        return {i+1: response for i, response in enumerate(self.test_responses)}

class Logger:
    def __init__(self):
        self.logs = []

    def log_event(self, event_message):
        self.logs.append({
            'timestamp': datetime.datetime.now(),
            'message': event_message
        })

    def get_logs(self):
        return self.logs

class Agency:
    def __init__(self, agency_code):
        self.agency_code = agency_code

    def generate_submission(self, data):
        submission_srv = SubmissionStatus()
        submission_id = submission_srv.submit_data(self.agency_code, data)
        return submission_id

    def view_submission(self, submission_id):
        submission_srv = SubmissionStatus()
        return submission_srv.get_submission_status(submission_id)

    def process_file(self, filename):
        validation_error = Validator.validate_file_extension(filename)
        if validation_error:
            return validation_error
        return "File processed successfully."

    def submit_data(self, data):
        validation_errors = Validator.validate_submission(data)
        if validation_errors:
            return validation_errors
        submission_id = self.generate_submission(data)
        return f"Submission ID: {submission_id}"

class FABSUser:
    def __init__(self):
        self.submission_history = {}

    def submit_fabs_data(self, data):
        agency = Agency(data.get('agency_code'))
        response = agency.submit_data(data)
        self.submission_history[data.get('agency_code')] = response
        return response

    def get_submission_status(self, agency_code):
        agency = Agency(agency_code)
        return agency.view_submission(self.submission_history.get(agency_code))

class FABSService:
    def __init__(self):
        self.submission_srv = SubmissionStatus()
        self.logger = Logger()
    
    def update_submission_status(self, submission_id, status):
        self.submission_srv.update_status(submission_id, status)
        self.logger.log_event(f"Updated submission ID {submission_id} to status '{status}'")

    def check_submission_status(self, submission_id):
        return self.submission_srv.get_submission_status(submission_id)

    def log_event(self, event_message):
        self.logger.log_event(event_message)

# Example usage
if __name__ == "__main__":
    # Simulating a submission by an agency user
    agency_user = Agency('XYZ123')
    submission_data = {
        'DUNS': '123456789',
        'PPoPZIP': '12345',
        'agency_code': 'XYZ123',
        'data_field_1': 'Sample data'
    }

    result = agency_user.submit_data(submission_data)
    print(result)

    submission_id = int(result.split(': ')[1])
    submission_status = agency_user.view_submission(submission_id)
    print(submission_status)
    
    # Log event for submission status update
    fabs_service = FABSService()
    fabs_service.update_submission_status(submission_id, 'Completed')
    print(fabs_service.check_submission_status(submission_id))