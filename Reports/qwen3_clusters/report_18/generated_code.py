class Submission:
    def __init__(self, submission_id, file_type, status="draft"):
        self.submission_id = submission_id
        self.file_type = file_type
        self.status = status
        self.publish_status_change_time = None
        self.gtac_window_start = None
        self.gtac_window_end = None
    
    def update_publish_status(self, new_status):
        self.status = new_status
        self.publish_status_change_time = "Current Timestamp"
        
    def set_gtas_window(self, start_date, end_date):
        self.gtac_window_start = start_date
        self.gtac_window_end = end_date

class ValidationError(Exception):
    def __init__(self, message, error_code):
        super().__init__(message)
        self.error_code = error_code

class FABSValidationError(ValidationError):
    def __init__(self, message, error_code, field_name=None):
        super().__init__(message, error_code)
        self.field_name = field_name

class FABSValidationRule:
    @staticmethod
    def validate_federal_action_obligation(value):
        if value is None:
            raise ValidationError("FederalActionObligation is required", "FABS-001")
        return True
        
    @staticmethod
    def validate_ppop_zip_4(zip_val):
        # Ensure that ZIP code works similarly in both Legal Entity and PPoP
        if not zip_val or len(str(zip_val)) < 5:
            raise ValidationError("PPoPZIP must be at least 5 characters long", "FABS-002")
        return True
        
    @staticmethod
    def validate_funding_agency_code(code):
        # Validate that funding agency codes are present
        if not code:
            raise ValidationError("FundingAgencyCode is required", "FABS-003")
        return True
        
    @staticmethod
    def validate_zero_or_blank_for_loan_records(record):
        # For loan records, accept zero and blank values 
        return True  # Placeholder, real implementation would check record structure
        
    @staticmethod
    def validate_max_length_for_address_line_3(value):
        if value and len(str(value)) > 75:
            raise ValidationError("LegalEntityAddressLine3 must not exceed 75 characters", "FABS-004")
        return True

class BrokerValidator:
    def validate_duns(self, duns, action_type, action_date):
        """Validates DUNS based on Action Type & Dates"""
        # Accept records with ActionTypes B, C, D and DUNS registered in SAM
        # Even if expired, and ActionDates before current registration date but after initial
        if action_type in ['B', 'C', 'D']:
            if self.check_duns_registered_in_sam(duns) or self.is_duns_expired_but_valid(duns, action_date):
                return True
        raise ValidationError("Invalid DUNS registration for this ActionType", "VALIDATION-001")
        
    def check_duns_registered_in_sam(self, duns):
        # Simulate checking DUNS in SAM
        return True
        
    def is_duns_expired_but_valid(self, duns, action_date):
        # Simulate checking if DUNS is expired but within valid range
        return True
        
    def validate_error_messages(self, message_text):
        # Ensures accurate text in error messages
        if not message_text:
            raise ValidationError("Error message text is required", "VALIDATION-002")
        return True

class DataManager:
    def __init__(self):
        self.historical_fabs_data = {}
        self.fpds_data = {}
        self.frec_derivations = {}
        
    def load_historical_fabs_data(self, file_path):
        # Load historical FABS data and derive FREC fields
        self.historical_fabs_data[file_path] = "Loaded Data"
        print(f"Loaded historical FABS data from {file_path}")
        return "Sample processed data with FREC derivations"
        
    def load_fpds_feed_data(self, source_url):
        # Include both extracted historical data and FPDS feed data
        self.fpds_data[source_url] = "Loaded Data"
        print(f"Loaded FPDS feed data from {source_url}")
        return "Processed FPDS data"

class UIComponent:
    def __init__(self, component_type, title):
        self.component_type = component_type
        self.title = title
        self.edit_round = 1
        
    def move_to_next_edit_round(self):
        self.edit_round += 1
        return self.edit_round
        
    def get_updated_date_time(self):
        return "Last Updated: 2024-01-01 12:00:00"

class FABSFileDownloader:
    def download_file(self, submission_id):
        # Return the submitted file for download
        return f"Downloaded FABS file for submission {submission_id}"

class TestEnvironmentManager:
    def __init__(self):
        self.environments = ["Staging", "Dev", "Test"]
        
    def enable_non_prod_feature(self, feature_name, env):
        if env in self.environments:
            return f"{feature_name} is enabled in {env}"
        else:
            return "Environment not supported"

class FieldDeriver:
    def derive_funding_agency_code(self, data):
        """Derive FundingAgencyCode based on other fields"""
        # Simplified derivation logic
        agency_code = data.get('agency_code')
        if agency_code:
            # In real implementation, derive full agency code
            return f"FAC-{agency_code}"
        else:
            raise ValidationError("Unable to derive FundingAgencyCode", "DERIVATION-001")

class ZipCodeValidator:
    @staticmethod
    def validate_ppop_zip_4_only(zip_val):
        """Allow leaving off last 4 digits"""
        if len(str(zip_val)) < 5:
            raise ValidationError("ZIP Code must be at least 5 digits", "ZIP-001")
        return True

# Testing and Mock Examples

def test_submission_updates():
    sub = Submission(1001, "FABS")
    sub.update_publish_status("published")
    assert sub.status == "published"
    
def test_validation_errors():
    try:
        FABSValidationRule.validate_federal_action_obligation(None)
        assert False, "Should have raised exception"
    except ValidationError:
        pass  # Expected
    
    try:
        FABSValidationRule.validate_ppop_zip_4("1234")
        assert False, "Should have raised exception"
    except ValidationError:
        pass  # Expected
        
def test_data_loading():
    dm = DataManager()
    data = dm.load_historical_fabs_data("/data/fabs/historical.csv")
    assert data.startswith("Sample processed")
    
def test_ui_components():
    ui = UIComponent("Help Page", "Edit Help Page")
    assert ui.move_to_next_edit_round() == 2
    assert ui.get_updated_date_time() != ""
    
def test_field_derivation():
    deriver = FieldDeriver()
    result = deriver.derive_funding_agency_code({'agency_code': '123'})
    assert result == "FAC-123"
    
def test_zip_validation():
    ZipCodeValidator.validate_ppop_zip_4_only("12345")
    try:
        ZipCodeValidator.validate_ppop_zip_4_only("1234")
        assert False, "Should have failed"
    except ValidationError:
        pass

if __name__ == "__main__":
    test_submission_updates()
    test_validation_errors()
    test_data_loading()
    test_ui_components()
    test_field_derivation()
    test_zip_validation()
    print("All tests passed.")