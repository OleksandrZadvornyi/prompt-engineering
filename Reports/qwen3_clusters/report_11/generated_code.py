import json
import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

@dataclass
class SubmissionError:
    code: str
    message: str
    field: str = None

@dataclass
class ValidationResult:
    valid: bool
    errors: List[SubmissionError]

@dataclass
class SubmissionMetadata:
    submission_id: str
    agency_code: str
    publish_status: str
    submitted_by: str
    created_at: datetime.datetime
    updated_at: datetime.datetime

class FABSSubmissionManager:
    
    def __init__(self):
        self.submissions: Dict[str, SubmissionMetadata] = {}
        self.validation_rules = {
            'duns': self._validate_duns,
            'popp_zip': self._validate_popp_zip,
            'flexfield': self._validate_flexfield
        }
        
    def validate_submission(self, file_data: Dict) -> ValidationResult:
        """Validate FABS submission data"""
        errors = []
        
        # Check DUNS validity
        if 'DUNS' in file_data:
            duns_valid = self.validation_rules['duns'](file_data['DUNS'])
            if not duns_valid:
                errors.append(SubmissionError('DUNS_INVALID', 'Invalid DUNS number'))
            
        # Validate PPoP ZIP code
        if 'PPoPZIP' in file_data:
            zip_valid = self.validation_rules['popp_zip'](file_data['PPoPZIP'])
            if not zip_valid:
                errors.append(SubmissionError('POPP_ZIP_INVALID', 'Invalid PPoP ZIP code format'))
                
        # Validate flexfield requirements
        if 'flex_fields' in file_data:
            flex_valid = self.validation_rules['flexfield'](file_data['flex_fields'])
            if not flex_valid:
                errors.append(SubmissionError('FLEXFIELD_INVALID', 'Invalid flexfield configuration'))
                
        return ValidationResult(len(errors) == 0, errors)
    
    def _validate_duns(self, duns: str) -> bool:
        """Validate DUNS number according to Business Rules"""
        # Simulate proper DUNS validation
        if duns == "":
            return True  # Allow blank for certain action types
        
        # Ensure DUNS is numeric and right length
        return duns.isdigit() and len(duns) == 9
    
    def _validate_popp_zip(self, zip_code: str) -> bool:
        """Validate PPoP ZIP code format including 5-digit and 9-digit options"""
        if not zip_code:
            return False
            
        # Remove hyphens and spaces for validation
        clean_zip = zip_code.replace('-', '').replace(' ', '')
        
        # Allow 5-digit ZIP, 5+4 ZIP, or 00xxxxx pattern
        if len(clean_zip) == 5 and clean_zip.isdigit():
            return True
        elif len(clean_zip) == 9 and clean_zip.isdigit():
            return True
        elif clean_zip.startswith('00') and len(clean_zip) > 6 and clean_zip[2:].isdigit():
            return True
        else:
            return False
            
    def _validate_flexfield(self, flex_fields: List[Dict]) -> bool:
        """Ensure flexfields meet requirements"""
        try:
            for field in flex_fields:
                if 'required' in field and field.get('required'):
                    if field.get('value') is None or field['value'] == "":
                        return False
            return True
        except Exception:
            return False
            
    def handle_publish(self, submission_id: str, publish_status: str = "published") -> bool:
        """Handle FABS submission publishing logic to prevent duplication."""
        if submission_id in self.submissions:
            # Update timestamp and publish status
            old_status = self.submissions[submission_id].publish_status
            self.submissions[submission_id].publish_status = publish_status
            self.submissions[submission_id].updated_at = datetime.datetime.now()
            
            # Prevent double publishing if already published
            if old_status == "published":
                return False
                
            return True
        return False
    
    def get_submission_details(self, submission_id: str) -> Optional[SubmissionMetadata]:
        """Get full submission details including who created it"""
        return self.submissions.get(submission_id)

class FPDSDataLoader:
    """Handles loading and updating FPDS data"""
    
    def __init__(self):
        self.fpds_config = {
            'daily_update_enabled': True,
            'start_year': 2007
        }
        
    def load_fpds_data(self, year: int = None, quarter: int = None):
        """Load FPDS data from various sources"""
        if not self.fpds_config['daily_update_enabled']:
            raise Exception("FPDS data loading is disabled")
        
        # Simulate loading historical data
        print(f"Loading FPDS data{f' for {year}' if year else ''}")
        
        # Load current month's data and historical data 
        # Based on requirements, this method would call actual data loaders
        return {'loaded_records': 0}
    
    def get_fpds_updates(self):
        """Return recent FPDS data updates"""
        return {
            'last_updated': datetime.datetime.now(),
            'records_processed': 12863,
            'status': 'up_to_date'
        }

class BrokerDataDeriver:
    """Handles data derivation logic for Broker"""
    
    def __init__(self):
        self.derivation_logic = {
            'funding_agency_code': self._derive_funding_agency_code,
            'pop_congressional_district': self._derive_pop_congressional_district,
            'popp_zip4': self._derive_popp_zip4,
            'freccode': self._derive_freccode
        }
        
    def derive_missing_fields(self, submission_data: Dict) -> Dict:
        """Apply all derivation logic to submission data"""
        result = submission_data.copy()
        
        # Apply various fields derivation  
        for derivation_name, derivation_func in self.derivation_logic.items():
            if derivation_name in result or derivation_name not in submission_data:
                try:
                    derived_value = derivation_func(submission_data)
                    result[derivation_name] = derived_value
                except Exception as e:
                    print(f"Derivation failed for {derivation_name}: {str(e)}")
                    
        return result
        
    def _derive_funding_agency_code(self, data: Dict) -> str:
        """Derive FundingAgencyCode from agency codes"""
        if 'agencyCode' in data:
            return f"AGENCY-{data['agencyCode']}"
        return "UNSET"
    
    def _derive_pop_congressional_district(self, data: Dict) -> str:
        """Derive congressional district based on ZIP code"""
        pop_zip = data.get('PPoPZIP', '')
        return f"CD-{pop_zip[:2]}-{pop_zip[-1]}" if pop_zip else "N/A"
        
    def _derive_popp_zip4(self, data: Dict) -> str:
        """Derive full ZIP+4 if available"""
        pop_zip = data.get('PPoPZIP', '')
        if len(pop_zip) in [5, 9]:  # Format like ZIP or ZIP+4
            if len(pop_zip) == 5:
                return f"{pop_zip[:5]}-0000"
            elif len(pop_zip) == 9:
                return f"{pop_zip[:5]}-{pop_zip[5:]}"
        else:
            return pop_zip  # Return original if not valid
    
    def _derive_freccode(self, data: Dict) -> str:
        """Derive FREC code for funding agency"""
        frec_dict = {"00": "FRECCODE-00"}
        return frec_dict.get(data.get('agencyCode', ''), "NO-FREC")

class ValidationRuleUpdater:
    """Manages Broker validation rule updates"""
    
    def __init__(self):
        self.version = "2.12"
        self.rules_table = self.load_rule_table()
        
    def load_rule_table(self) -> Dict[str, Dict]:
        """Simulated rule loading"""
        return {
            "DB-2213": {
                "rule_id": "DB-2213",
                "description": "Updated error handling logic",
                "changed_fields": ["FundingAgencyCode", "PPoPZIP"],
                "status": "updated"
            },
            "CFDA_RULE": {
                "rule_id": "CFDA_RULE",
                "description": "Clearer explanation for CFDA error codes",
                "changed_fields": ["CFDA"]
            }
        }
        
    def update_rules_from_db(self, db_version: str):
        """Update validation rules table version"""
        update_info = self.rules_table.get(db_version)
        if update_info and update_info["status"] != "updated":
            update_info["status"] = "updated"
            print(f"Validation rules updated to {db_version}")
            return True
        return False

class GTASWindowManager:
    """Manage GTAS window operations"""
    
    def __init__(self):
        self.gtas_windows = {}
        self.is_window_open = False
        
    def lock_system_during_gtas(self, start_time: datetime.datetime, end_time: datetime.datetime):
        """Disable publishing during GTAS periods"""
        self.gtas_windows = {
            'start': start_time,
            'end': end_time
        }
        self.is_window_open = True
        print(f"System locked during GTAS period: {start_time} - {end_time}")
        
    def unlock_system_after_gtas(self):
        """Enable publishing after GTAS window"""
        self.is_window_open = False
        print("System unlocked after GTAS period")

class HistoricalDataLoader:
    """Handles loading of FABS history data"""
    
    def __init__(self):
        self.historical_loader = {
            "fabs": self._load_historical_fabs,
            "fpds": self._load_historical_fpds,
            "sample_files": self._load_sample_files
        }
        
    def load_data_source(self, source_type: str, **kwargs) -> Dict:
        """Load historical data from specified source"""
        if source_type in self.historical_loader:
            return self.historical_loader[source_type](**kwargs)
        raise ValueError(f"Unknown data source: {source_type}")
        
    def _load_historical_fabs(self, start_year: int = 2007) -> Dict:
        """Load historical FABS data"""
        records_loaded = 500000  # Placeholder for real count
        return {
            'loaded_count': records_loaded,
            'source': 'historical_fabs',
            'date_range': f"{start_year}-present"
        }
        
    def _load_historical_fpds(self, start_year: int = 2007) -> Dict:
        """Load historical FPDS data"""
        records_loaded = 1000000  # Placeholder for real count
        return {
            'loaded_count': records_loaded,
            'source': 'historical_fpds',
            'date_range': f"{start_year}-present"
        }
        
    def _load_sample_files(self, version: str = "v1.1") -> Dict:
        """Load appropriate sample files"""
        file_name = f"fabs_sample_{version}.csv"
        return {
            'file_name': file_name,
            'version': version,
            'download_url': f'https://example.com/sample/{file_name}'
        }

### Main Application Interface
def main():
    """Demonstrate the functionality based on user stories."""
    
    # Test basic validation
    manager = FABSSubmissionManager()
    
    # Simulate a validation request
    test_data = {
        'DUNS': '123456789',
        'PPoPZIP': '12345-6789',
        'flex_fields': [{'required': True, 'name': 'Field1', 'value': 'TestData'}], 
    }
    
    result = manager.validate_submission(test_data)
    print("Validation Result:", result.valid)
    if not result.valid:
        for err in result.errors:
            print(f"Error: [{err.code}] {err.message}")

    # Test Publishing Logic
    submission_id = "SUB-12345"
    manager.submissions[submission_id] = SubmissionMetadata(
        submission_id="SUB-12345",
        agency_code="TEST",
        publish_status="validating",
        submitted_by="TestUser",
        created_at=datetime.datetime.now(),
        updated_at=datetime.datetime.now()
    )
    
    assert manager.handle_publish(submission_id) == True  # First publish succeeds
    assert manager.handle_publish(submission_id) == False  # Second publish fails due to dup
    
    # Test data derivation
    deriver = BrokerDataDeriver()
    input_data = {
        'agencyCode': '00',
        'PPoPZIP': '12345',
    }
    derived = deriver.derive_missing_fields(input_data)
    print("\nDerived Fields:", derived)
    
    # Test FPDS loader
    fpds_loader = FPDSDataLoader()
    fpds_updates = fpds_loader.get_fpds_updates()
    print(f"\nFPDS Updates: {fpds_updates}")
    
    # Test Historical Data Loader
    hist_loader = HistoricalDataLoader()
    
    fabs_load = hist_loader.load_data_source('fabs', start_year=2007)
    fpds_load = hist_loader.load_data_source('fpds', start_year=2007)
    sample_file = hist_loader.load_data_source('sample_files', version='v1.1')
    
    print(f"\nHistorical Loads:")
    print(f"  FABS: {fabs_load}")
    print(f"  FPDS: {fpds_load}")
    print(f"  Sample File: {sample_file}")
    
    # Test validation rule updating
    validator = ValidationRuleUpdater()
    validator.update_rules_from_db("DB-2213")
    print("\nUpdated validation rules:", validator.rules_table["DB-2213"])

if __name__ == "__main__":
    main()