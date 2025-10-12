import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple
import psycopg2
from fastapi import FastAPI, HTTPException, Request, status
from pydantic import BaseModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Database connection setup
def get_db_connection():
    return psycopg2.connect(
        dbname="broker",
        user="broker_user",
        password="secure_password",
        host="localhost",
        port="5432"
    )

# ----- Data Processing -----
def process_2017_deletions():
    """Process deletions from 12-19-2017"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM submissions 
            WHERE deletion_date = '2017-12-19'
        """)
        conn.commit()
        logger.info("Successfully processed 2017 deletions")
    except Exception as e:
        logger.error(f"Error processing 2017 deletions: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# ----- FABS Submission Handling -----
class PublishStatus(str, Enum):
    UNPUBLISHED = "unpublished"
    PENDING = "pending"
    PUBLISHED = "published"

class FABSSubmission(BaseModel):
    submission_id: str
    publish_status: PublishStatus
    created_by: str
    updated_at: datetime = None

def update_fabs_submission_status(submission_id: str, new_status: PublishStatus):
    """Update FABS submission status and prevent double publishing"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check current status
        cursor.execute("""
            SELECT publish_status FROM fabs_submissions 
            WHERE submission_id = %s FOR UPDATE
        """, (submission_id,))
        current_status = cursor.fetchone()
        
        if not current_status:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Submission not found"
            )
            
        if current_status[0] == PublishStatus.PUBLISHED and new_status == PublishStatus.PUBLISHED:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Submission already published"
            )
        
        # Update status
        cursor.execute("""
            UPDATE fabs_submissions 
            SET publish_status = %s, updated_at = NOW() 
            WHERE submission_id = %s
        """, (new_status.value, submission_id))
        conn.commit()
        
        logger.info(f"Updated submission {submission_id} to {new_status}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating submission status: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# ----- Validation Rules -----
def update_validation_rules(rule_updates: Dict[str, str]):
    """Update validation rules in the database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for rule_id, new_rule in rule_updates.items():
            cursor.execute("""
                UPDATE validation_rules 
                SET rule_definition = %s 
                WHERE rule_id = %s
            """, (new_rule, rule_id))
        
        conn.commit()
        logger.info(f"Updated {len(rule_updates)} validation rules")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating validation rules: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# ----- File Generation -----
def generate_d_file(agency_id: str, force_regenerate: bool = False) -> str:
    """Generate D file from FABS/FPDS data with caching"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check cache first
        cursor.execute("""
            SELECT file_content, generated_at 
            FROM d_file_cache 
            WHERE agency_id = %s 
            ORDER BY generated_at DESC LIMIT 1
        """, (agency_id,))
        cached_file = cursor.fetchone()
        
        # Check if FPDS data has been updated since last generation
        cursor.execute("""
            SELECT MAX(last_updated) FROM fpds_data 
            WHERE agency_id = %s
        """, (agency_id,))
        fpds_update_time = cursor.fetchone()[0]
        
        if not force_regenerate and cached_file:
            cached_time = cached_file[1]
            if not fpds_update_time or fpds_update_time <= cached_time:
                return cached_file[0]
        
        # Generate new file if needed
        cursor.execute("""
            SELECT * FROM fabs_data 
            WHERE agency_id = %s AND publish_status = 'published'
            UNION ALL
            SELECT * FROM fpds_data 
            WHERE agency_id = %s
        """, (agency_id, agency_id))
        
        data = cursor.fetchall()
        file_content = "\n".join([",".join(map(str, row)) for row in data])
        
        # Update cache
        cursor.execute("""
            INSERT INTO d_file_cache (agency_id, file_content, generated_at)
            VALUES (%s, %s, NOW())
        """, (agency_id, file_content))
        conn.commit()
        
        return file_content
    except Exception as e:
        conn.rollback()
        logger.error(f"Error generating D file: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# ----- UI Management -----
class UIChangeRequest(BaseModel):
    page_name: str
    changes: Dict[str, str]
    round_number: int
    requested_by: str

def track_ui_changes(request: UIChangeRequest) -> bool:
    """Track UI change requests and manage rounds of edits"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO ui_change_requests 
            (page_name, changes, round_number, requested_by, request_date)
            VALUES (%s, %s, %s, %s, NOW())
        """, (
            request.page_name, 
            str(request.changes), 
            request.round_number, 
            request.requested_by
        ))
        conn.commit()
        
        logger.info(
            f"Recorded UI change request for {request.page_name} "
            f"(Round {request.round_number})"
        )
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error tracking UI change: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# ----- User Testing -----
class UserTestSession(BaseModel):
    session_id: str
    test_scenarios: List[str]
    participants: List[str]
    scheduled_time: datetime
    requested_by: str

def schedule_user_testing(session: UserTestSession) -> bool:
    """Schedule user testing sessions"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO user_test_sessions 
            (session_id, test_scenarios, participants, scheduled_time, requested_by)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            session.session_id,
            session.test_scenarios,
            session.participants,
            session.scheduled_time,
            session.requested_by
        ))
        conn.commit()
        
        logger.info(f"Scheduled user testing session {session.session_id}")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error scheduling user testing: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# ----- Historical Data Loading -----
def load_historical_fabs_data(start_date: str, end_date: str) -> int:
    """Load historical FABS data with proper derivations"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO published_award_financial_assistance
            SELECT 
                f.*,
                -- Derive FREC codes
                CASE 
                    WHEN f.agency_code LIKE '0%' THEN 'FREC' || SUBSTRING(f.agency_code, 2, 3)
                    ELSE NULL
                END AS frec_code,
                -- Derive office names
                o.office_name
            FROM historical_fabs_data f
            LEFT JOIN office_codes o ON f.office_code = o.office_code
            WHERE f.action_date BETWEEN %s AND %s
        """, (start_date, end_date))
        
        count = cursor.rowcount
        conn.commit()
        
        logger.info(f"Loaded {count} historical FABS records")
        return count
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading historical FABS data: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# ----- System Configuration -----
def configure_new_relic() -> bool:
    """Configure New Relic monitoring across applications"""
    # In a real implementation, this would involve API calls to New Relic
    # For the mock, we'll just log the operation
    logger.info("New Relic configuration updated for all applications")
    return True

# ----- API Endpoints -----
@app.post("/fabs/submissions/{submission_id}/publish")
async def publish_fabs_submission(submission_id: str):
    update_fabs_submission_status(submission_id, PublishStatus.PUBLISHED)
    return {"status": "success"}

@app.post("/ui/changes")
async def request_ui_change(change: UIChangeRequest):
    track_ui_changes(change)
    return {"status": "change recorded"}

@app.post("/testing/schedule")
async def schedule_test_session(session: UserTestSession):
    schedule_user_testing(session)
    return {"status": "scheduled"}

@app.get("/files/d/{agency_id}")
async def get_d_file(agency_id: str, force: bool = False):
    content = generate_d_file(agency_id, force)
    return {"content": content}

@app.post("/data/historical/fabs")
async def load_fabs_history(start_date: str, end_date: str):
    count = load_historical_fabs_data(start_date, end_date)
    return {"records_loaded": count}

# Main execution for one-time tasks
if __name__ == "__main__":
    # Process the 2017 deletions when run directly
    process_2017_deletions()
    # Setup New Relic monitoring
    configure_new_relic()