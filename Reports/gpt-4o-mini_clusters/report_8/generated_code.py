from datetime import datetime
import json

class UserStory:
    def __init__(self, user_type, story):
        self.user_type = user_type
        self.story = story
        self.timestamp = datetime.now()

    def __repr__(self):
        return f"{self.user_type}: {self.story} ({self.timestamp})"


class UserStoryManager:
    def __init__(self):
        self.user_stories = []

    def add_user_story(self, user_type, story):
        new_story = UserStory(user_type, story)
        self.user_stories.append(new_story)

    def get_user_stories(self):
        return self.user_stories

    def filter_user_stories_by_user_type(self, user_type):
        return [story for story in self.user_stories if story.user_type == user_type]

    def process_deletions(self, deletion_date):
        # Process deletions logic
        return f"Processed deletions for {deletion_date}"

    def redesign_resources_page(self):
        return "Redesigned the Resources page to match the new Broker design styles."

    def report_user_testing(self):
        return "Reported user testing results to agencies."

    def sync_d1_file_with_fpds(self):
        return "D1 file generation synced with FPDS data load."

    def update_sql_codes(self):
        return "Updated SQL codes for clarity."

    def derive_funding_agency_code(self):
        return "Derived FundingAgencyCode to improve data quality."

    def map_federal_action_obligation(self):
        return "Mapped FederalActionObligation to Atom Feed."

    def validate_ppop_zip(self):
        return "Validated PPoPZIP+4 with Legal Entity ZIP validations."


class UI_Designer(UserStoryManager):
    def move_on_to_round_edits(self, page_name, round_number):
        return f"Moved on to round {round_number} of {page_name} edits for approvals."

    def track_tech_thursday_issues(self):
        return "Tracked issues from Tech Thursday for testing."

    def schedule_user_testing(self):
        return "Scheduled user testing sessions with advance notice."


class Developer(UserStoryManager):
    def log_improvements(self):
        return "Improved logging for better troubleshooting."

    def add_gtas_data_to_db(self):
        return "Added GTAS window data to the database."

    def prevent_double_publishing(self):
        return "Prevented users from double publishing FABS submissions."


class AgencyUser(UserStoryManager):
    def show_submission_periods(self):
        return "Displayed start and end submission periods."

    def include_flexfields(self, count):
        return f"Including {count} flexfields without performance impact."


class FABSUser(UserStoryManager):
    def upload_fabs_file(self):
        return "Uploaded FABS file successfully."

    def download_uploaded_file(self):
        return "Downloaded the uploaded FABS file."


def main():
    manager = UserStoryManager()

    # Clusters as defined
    manager.add_user_story('Data User', 'Process deletions for 12-19-2017')
    manager.add_user_story('UI Designer', 'Redesign Resources page to match Broker styles')
    manager.add_user_story('UI Designer', 'Report user testing to agencies')
    manager.add_user_story('DevOps Engineer', 'Provision useful data via New Relic')
    manager.add_user_story('Broker User', 'Sync D1 file generation with FPDS data load')
    
    # Developers and UI Designers
    dev = Developer()
    dev.add_user_story('Developer', 'Log troubleshooting improvements')
    dev.add_user_story('Developer', 'Manage D Files generation requests and caching')

    ui_designer = UI_Designer()
    ui_designer.move_on_to_round_edits('Help Page', 2)
    
    agency_user = AgencyUser()
    agency_user.include_flexfields(50)

    fabs_user = FABSUser()
    fabs_user.upload_fabs_file()

    # Output all user stories
    for story in manager.get_user_stories():
        print(story)

if __name__ == "__main__":
    main()