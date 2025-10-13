from datetime import datetime
from typing import List, Dict, Optional

class Submission:
    def __init__(self, submission_id: str, status: str, upload_date: datetime, error_messages: List[str]):
        self.submission_id = submission_id
        self.status = status
        self.upload_date = upload_date
        self.error_messages = error_messages

    def update_status(self, new_status: str):
        self.status = new_status

    def add_error_message(self, message: str):
        self.error_messages.append(message)

class FileManager:
    def __init__(self):
        self.uploaded_files = []

    def upload_file(self, file_path: str) -> str:
        self.uploaded_files.append(file_path)
        return f"File {file_path} uploaded successfully."


class User:
    def __init__(self, user_id: str):
        self.user_id = user_id

    def validate_submission(self, submission: Submission) -> bool:
        if not submission.error_messages:
            submission.update_status('Validated')
            return True
        else:
            submission.update_status('Error')
            return False

class AgencyUser(User):
    def __init__(self, user_id: str, agency_name: str):
        super().__init__(user_id)
        self.agency_name = agency_name
    
    def receive_updates(self):
        print(f"Agency {self.agency_name} will receive updates to FABS records.")

class FABSUser(User):
    def __init__(self, user_id: str):
        super().__init__(user_id)

    def submit_for_validation(self, submission: Submission):
        print(f"Submitting {submission.submission_id} for validation.")
        return submission


class Developer(User):
    def __init__(self, user_id: str):
        super().__init__(user_id)

    def update_validation_logic(self):
        print("Updated Broker validation rule table.")

class System:
    submissions: Dict[str, Submission]
    file_manager: FileManager
    
    def __init__(self):
        self.submissions = {}
        self.file_manager = FileManager()

    def add_submission(self, submission_id: str, status: str, upload_date: datetime):
        self.submissions[submission_id] = Submission(submission_id, status, upload_date, [])

    def process_deletions(self, date: str):
        print(f"Processing deletions from {date}.")

    def sync_data_loads(self):
        print("Syncing D1 file generation with FPDS data load.")

    def generate_file(self, submission: Submission) -> Optional[str]:
        if submission.status == 'Validated':
            return f"Generated D1 file for {submission.submission_id}."
        else:
            print("Cannot generate file; submission has errors.")
            return None

    def report_to_agencies(self):
        print("Reporting user testing contributions to agencies.")

    def get_file_status(self):
        return {submission_id: submission.status for submission_id, submission in self.submissions.items()}

# Example usage
system = System()
found_sub = system.add_submission('S1', 'Pending', datetime.now())
print(system.get_file_status())
user = FABSUser("user123")
user.submit_for_validation(found_sub)
user.validate_submission(found_sub)

submission_id = "S1"
system.process_deletions("12-19-2017")
system.sync_data_loads()
file_gen_result = system.generate_file(found_sub)
print(file_gen_result)
system.report_to_agencies()