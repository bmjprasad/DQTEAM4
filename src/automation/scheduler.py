"""
Data Quality Scheduler - Automated data quality checks and monitoring
Similar to Ataccama's scheduling capabilities
"""

from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta
import json
import logging
from dataclasses import dataclass
from enum import Enum
import threading
import time

class ScheduleType(Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    CUSTOM = "custom"

@dataclass
class QualityCheck:
    """Data structure for quality check configuration"""
    name: str
    dataset: str
    rules_config: Dict[str, Any]
    schedule: ScheduleType
    schedule_time: str  # HH:MM format
    enabled: bool = True
    last_run: Optional[datetime] = None
    last_status: Optional[str] = None
    alert_threshold: float = 80.0  # Quality score threshold for alerts

class DataQualityScheduler:
    """
    Scheduler for automated data quality checks
    """
    
    def __init__(self, dq_engine, alert_manager=None):
        """
        Initialize the scheduler
        
        Args:
            dq_engine: DataQualityEngine instance
            alert_manager: AlertManager instance for notifications
        """
        self.dq_engine = dq_engine
        self.alert_manager = alert_manager
        self.quality_checks: List[QualityCheck] = []
        self.running = False
        self.scheduler_thread = None
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
    def add_quality_check(self, check: QualityCheck):
        """Add a quality check to the scheduler"""
        self.quality_checks.append(check)
        self.logger.info(f"Added quality check: {check.name}")
        
    def remove_quality_check(self, check_name: str):
        """Remove a quality check from the scheduler"""
        self.quality_checks = [check for check in self.quality_checks if check.name != check_name]
        self.logger.info(f"Removed quality check: {check_name}")
        
    def start_scheduler(self):
        """Start the automated scheduler"""
        if self.running:
            self.logger.warning("Scheduler is already running")
            return
            
        self.running = True
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop)
        self.scheduler_thread.daemon = True
        self.scheduler_thread.start()
        self.logger.info("Data quality scheduler started")
        
    def stop_scheduler(self):
        """Stop the automated scheduler"""
        self.running = False
        if self.scheduler_thread:
            self.scheduler_thread.join()
        self.logger.info("Data quality scheduler stopped")
        
    def _scheduler_loop(self):
        """Main scheduler loop"""
        while self.running:
            try:
                current_time = datetime.now()
                
                for check in self.quality_checks:
                    if not check.enabled:
                        continue
                        
                    if self._should_run_check(check, current_time):
                        self._run_quality_check(check)
                        
                # Sleep for 1 minute before next check
                time.sleep(60)
                
            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {str(e)}")
                time.sleep(60)
                
    def _should_run_check(self, check: QualityCheck, current_time: datetime) -> bool:
        """Determine if a quality check should run based on schedule"""
        if check.last_run is None:
            return True
            
        schedule_time = datetime.strptime(check.schedule_time, "%H:%M").time()
        current_time_only = current_time.time()
        
        if check.schedule == ScheduleType.DAILY:
            # Run daily at specified time
            return (current_time_only >= schedule_time and 
                    check.last_run.date() < current_time.date())
                    
        elif check.schedule == ScheduleType.WEEKLY:
            # Run weekly on Monday at specified time
            return (current_time.weekday() == 0 and 
                    current_time_only >= schedule_time and
                    check.last_run.date() < current_time.date())
                    
        elif check.schedule == ScheduleType.MONTHLY:
            # Run monthly on 1st at specified time
            return (current_time.day == 1 and 
                    current_time_only >= schedule_time and
                    check.last_run.month < current_time.month)
                    
        return False
        
    def _run_quality_check(self, check: QualityCheck):
        """Run a specific quality check"""
        try:
            self.logger.info(f"Running quality check: {check.name}")
            
            # Load dataset (this would be customized based on your data source)
            df = self._load_dataset(check.dataset)
            
            if df is None:
                self.logger.error(f"Failed to load dataset: {check.dataset}")
                return
                
            # Run validation
            validation_results = self.dq_engine.validate_data(df, check.rules_config)
            
            # Calculate quality score
            quality_metrics = self.dq_engine.metrics.calculate_quality_score(df, validation_results)
            quality_score = quality_metrics['overall_quality_score']['overall_score']
            
            # Update check status
            check.last_run = datetime.now()
            check.last_status = "PASS" if quality_score >= check.alert_threshold else "FAIL"
            
            # Send alert if quality score is below threshold
            if quality_score < check.alert_threshold:
                self._send_alert(check, quality_score, validation_results)
                
            self.logger.info(f"Quality check completed: {check.name} - Score: {quality_score:.1f}")
            
        except Exception as e:
            self.logger.error(f"Error running quality check {check.name}: {str(e)}")
            check.last_status = "ERROR"
            
    def _load_dataset(self, dataset_name: str):
        """Load dataset based on name (customize for your data sources)"""
        try:
            # This is a placeholder - customize based on your data sources
            if dataset_name == "customers":
                return self.dq_engine.spark.table("customers")
            elif dataset_name == "sales":
                return self.dq_engine.spark.table("sales")
            elif dataset_name == "products":
                return self.dq_engine.spark.table("products")
            else:
                # Try to load as table
                return self.dq_engine.spark.table(dataset_name)
        except Exception as e:
            self.logger.error(f"Failed to load dataset {dataset_name}: {str(e)}")
            return None
            
    def _send_alert(self, check: QualityCheck, quality_score: float, validation_results: Dict):
        """Send alert for quality issues"""
        if self.alert_manager:
            alert_message = {
                "check_name": check.name,
                "dataset": check.dataset,
                "quality_score": quality_score,
                "threshold": check.alert_threshold,
                "validation_status": validation_results['summary']['overall_status'],
                "failed_rules": validation_results['summary']['failed_rules'],
                "timestamp": datetime.now().isoformat()
            }
            self.alert_manager.send_alert(alert_message)
            
    def get_check_status(self) -> List[Dict[str, Any]]:
        """Get status of all quality checks"""
        status_list = []
        for check in self.quality_checks:
            status_list.append({
                "name": check.name,
                "dataset": check.dataset,
                "schedule": check.schedule.value,
                "schedule_time": check.schedule_time,
                "enabled": check.enabled,
                "last_run": check.last_run.isoformat() if check.last_run else None,
                "last_status": check.last_status,
                "alert_threshold": check.alert_threshold
            })
        return status_list
        
    def run_check_now(self, check_name: str) -> Dict[str, Any]:
        """Manually run a specific quality check"""
        check = next((c for c in self.quality_checks if c.name == check_name), None)
        if not check:
            return {"error": f"Quality check '{check_name}' not found"}
            
        try:
            self._run_quality_check(check)
            return {
                "status": "success",
                "check_name": check.name,
                "last_run": check.last_run.isoformat(),
                "last_status": check.last_status
            }
        except Exception as e:
            return {"error": f"Failed to run check: {str(e)}"}
            
    def save_configuration(self, file_path: str):
        """Save scheduler configuration to file"""
        config = {
            "quality_checks": [
                {
                    "name": check.name,
                    "dataset": check.dataset,
                    "rules_config": check.rules_config,
                    "schedule": check.schedule.value,
                    "schedule_time": check.schedule_time,
                    "enabled": check.enabled,
                    "alert_threshold": check.alert_threshold
                }
                for check in self.quality_checks
            ]
        }
        
        with open(file_path, 'w') as f:
            json.dump(config, f, indent=2)
            
        self.logger.info(f"Configuration saved to: {file_path}")
        
    def load_configuration(self, file_path: str):
        """Load scheduler configuration from file"""
        try:
            with open(file_path, 'r') as f:
                config = json.load(f)
                
            self.quality_checks = []
            for check_config in config.get("quality_checks", []):
                check = QualityCheck(
                    name=check_config["name"],
                    dataset=check_config["dataset"],
                    rules_config=check_config["rules_config"],
                    schedule=ScheduleType(check_config["schedule"]),
                    schedule_time=check_config["schedule_time"],
                    enabled=check_config.get("enabled", True),
                    alert_threshold=check_config.get("alert_threshold", 80.0)
                )
                self.quality_checks.append(check)
                
            self.logger.info(f"Configuration loaded from: {file_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {str(e)}")