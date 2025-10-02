"""
Automation Manager - Main orchestrator for data quality automation
Combines scheduling and alerting capabilities
"""

from typing import Dict, List, Any, Optional
import json
import logging
from datetime import datetime
import os

from .scheduler import DataQualityScheduler, QualityCheck, ScheduleType
from .alerting import AlertManager, AlertRule, AlertSeverity, AlertChannel
from ..core.data_quality_engine import DataQualityEngine

class AutomationManager:
    """
    Main automation manager that orchestrates data quality checks and alerting
    """
    
    def __init__(self, dq_engine: DataQualityEngine, config_path: Optional[str] = None):
        """
        Initialize the automation manager
        
        Args:
            dq_engine: DataQualityEngine instance
            config_path: Path to configuration file
        """
        self.dq_engine = dq_engine
        
        # Initialize alert manager
        self.alert_manager = AlertManager()
        
        # Initialize scheduler with alert manager
        self.scheduler = DataQualityScheduler(dq_engine, self.alert_manager)
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Load configuration if provided
        if config_path and os.path.exists(config_path):
            self.load_configuration(config_path)
            
    def setup_default_quality_checks(self):
        """Setup default quality checks for common scenarios"""
        # Customer data quality check
        customer_check = QualityCheck(
            name="customer_data_quality",
            dataset="customers",
            rules_config={
                "not_null_constraints": {
                    "type": "not_null",
                    "columns": ["customer_id", "first_name", "last_name", "email"]
                },
                "email_format_validation": {
                    "type": "pattern",
                    "column": "email",
                    "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
                },
                "unique_customer_id": {
                    "type": "unique",
                    "columns": ["customer_id"]
                }
            },
            schedule=ScheduleType.DAILY,
            schedule_time="09:00",
            alert_threshold=85.0
        )
        
        # Sales data quality check
        sales_check = QualityCheck(
            name="sales_data_quality",
            dataset="sales",
            rules_config={
                "not_null_constraints": {
                    "type": "not_null",
                    "columns": ["sale_id", "customer_id", "product_id", "sale_date", "amount"]
                },
                "amount_range_validation": {
                    "type": "range",
                    "column": "amount",
                    "min_value": 0.01,
                    "max_value": 1000000
                },
                "unique_sale_id": {
                    "type": "unique",
                    "columns": ["sale_id"]
                }
            },
            schedule=ScheduleType.DAILY,
            schedule_time="10:00",
            alert_threshold=90.0
        )
        
        # Product data quality check
        product_check = QualityCheck(
            name="product_data_quality",
            dataset="products",
            rules_config={
                "not_null_constraints": {
                    "type": "not_null",
                    "columns": ["product_id", "product_name", "category", "price"]
                },
                "price_range_validation": {
                    "type": "range",
                    "column": "price",
                    "min_value": 0.01,
                    "max_value": 10000
                },
                "unique_product_id": {
                    "type": "unique",
                    "columns": ["product_id"]
                }
            },
            schedule=ScheduleType.WEEKLY,
            schedule_time="08:00",
            alert_threshold=80.0
        )
        
        # Add checks to scheduler
        self.scheduler.add_quality_check(customer_check)
        self.scheduler.add_quality_check(sales_check)
        self.scheduler.add_quality_check(product_check)
        
        self.logger.info("Default quality checks configured")
        
    def setup_default_alert_rules(self):
        """Setup default alert rules"""
        # High severity alert rule
        high_severity_rule = AlertRule(
            name="high_severity_alerts",
            condition="quality_score < 70",
            severity=AlertSeverity.HIGH,
            channels=[AlertChannel.EMAIL, AlertChannel.SLACK],
            cooldown_minutes=30
        )
        
        # Medium severity alert rule
        medium_severity_rule = AlertRule(
            name="medium_severity_alerts",
            condition="quality_score < 85",
            severity=AlertSeverity.MEDIUM,
            channels=[AlertChannel.EMAIL, AlertChannel.LOG],
            cooldown_minutes=60
        )
        
        # Low severity alert rule
        low_severity_rule = AlertRule(
            name="low_severity_alerts",
            condition="quality_score < 95",
            severity=AlertSeverity.LOW,
            channels=[AlertChannel.LOG],
            cooldown_minutes=120
        )
        
        # Add rules to alert manager
        self.alert_manager.add_alert_rule(high_severity_rule)
        self.alert_manager.add_alert_rule(medium_severity_rule)
        self.alert_manager.add_alert_rule(low_severity_rule)
        
        self.logger.info("Default alert rules configured")
        
    def start_automation(self):
        """Start the automation system"""
        self.logger.info("Starting data quality automation system...")
        
        # Start scheduler
        self.scheduler.start_scheduler()
        
        self.logger.info("Data quality automation system started")
        
    def stop_automation(self):
        """Stop the automation system"""
        self.logger.info("Stopping data quality automation system...")
        
        # Stop scheduler
        self.scheduler.stop_scheduler()
        
        self.logger.info("Data quality automation system stopped")
        
    def run_manual_check(self, check_name: str) -> Dict[str, Any]:
        """Run a quality check manually"""
        return self.scheduler.run_check_now(check_name)
        
    def get_system_status(self) -> Dict[str, Any]:
        """Get overall system status"""
        scheduler_status = self.scheduler.get_check_status()
        alert_stats = self.alert_manager.get_alert_statistics()
        
        return {
            "timestamp": datetime.now().isoformat(),
            "scheduler": {
                "running": self.scheduler.running,
                "total_checks": len(scheduler_status),
                "enabled_checks": len([check for check in scheduler_status if check["enabled"]]),
                "checks": scheduler_status
            },
            "alerts": alert_stats,
            "data_quality_engine": {
                "status": "active",
                "spark_session": "active" if self.dq_engine.spark else "inactive"
            }
        }
        
    def configure_email_alerts(self, smtp_host: str, smtp_port: int, username: str, 
                              password: str, from_email: str, to_emails: List[str]):
        """Configure email alerting"""
        email_config = {
            "smtp_host": smtp_host,
            "smtp_port": smtp_port,
            "username": username,
            "password": password,
            "from_email": from_email,
            "to_emails": to_emails
        }
        
        self.alert_manager.config["email"] = email_config
        self.logger.info("Email alerting configured")
        
    def configure_slack_alerts(self, webhook_url: str):
        """Configure Slack alerting"""
        slack_config = {
            "webhook_url": webhook_url
        }
        
        self.alert_manager.config["slack"] = slack_config
        self.logger.info("Slack alerting configured")
        
    def configure_webhook_alerts(self, webhook_url: str):
        """Configure webhook alerting"""
        webhook_config = {
            "url": webhook_url
        }
        
        self.alert_manager.config["webhook"] = webhook_config
        self.logger.info("Webhook alerting configured")
        
    def add_custom_quality_check(self, name: str, dataset: str, rules_config: Dict[str, Any],
                                schedule: ScheduleType, schedule_time: str, 
                                alert_threshold: float = 80.0):
        """Add a custom quality check"""
        check = QualityCheck(
            name=name,
            dataset=dataset,
            rules_config=rules_config,
            schedule=schedule,
            schedule_time=schedule_time,
            alert_threshold=alert_threshold
        )
        
        self.scheduler.add_quality_check(check)
        self.logger.info(f"Custom quality check added: {name}")
        
    def add_custom_alert_rule(self, name: str, condition: str, severity: AlertSeverity,
                             channels: List[AlertChannel], cooldown_minutes: int = 60):
        """Add a custom alert rule"""
        rule = AlertRule(
            name=name,
            condition=condition,
            severity=severity,
            channels=channels,
            cooldown_minutes=cooldown_minutes
        )
        
        self.alert_manager.add_alert_rule(rule)
        self.logger.info(f"Custom alert rule added: {name}")
        
    def get_recent_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent alerts"""
        return self.alert_manager.get_alerts(limit=limit)
        
    def resolve_alert(self, alert_id: str):
        """Resolve an alert"""
        self.alert_manager.resolve_alert(alert_id)
        
    def save_configuration(self, file_path: str):
        """Save complete automation configuration"""
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
                for check in self.scheduler.quality_checks
            ],
            "alert_rules": [
                {
                    "name": rule.name,
                    "condition": rule.condition,
                    "severity": rule.severity.value,
                    "channels": [channel.value for channel in rule.channels],
                    "enabled": rule.enabled,
                    "cooldown_minutes": rule.cooldown_minutes
                }
                for rule in self.alert_manager.alert_rules
            ],
            "channels_config": self.alert_manager.config
        }
        
        with open(file_path, 'w') as f:
            json.dump(config, f, indent=2)
            
        self.logger.info(f"Automation configuration saved to: {file_path}")
        
    def load_configuration(self, file_path: str):
        """Load complete automation configuration"""
        try:
            with open(file_path, 'r') as f:
                config = json.load(f)
                
            # Load quality checks
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
                self.scheduler.add_quality_check(check)
                
            # Load alert rules
            for rule_config in config.get("alert_rules", []):
                rule = AlertRule(
                    name=rule_config["name"],
                    condition=rule_config["condition"],
                    severity=AlertSeverity(rule_config["severity"]),
                    channels=[AlertChannel(ch) for ch in rule_config["channels"]],
                    enabled=rule_config.get("enabled", True),
                    cooldown_minutes=rule_config.get("cooldown_minutes", 60)
                )
                self.alert_manager.add_alert_rule(rule)
                
            # Load channel configuration
            self.alert_manager.config = config.get("channels_config", {})
            
            self.logger.info(f"Automation configuration loaded from: {file_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to load automation configuration: {str(e)}")
            
    def generate_automation_report(self) -> Dict[str, Any]:
        """Generate comprehensive automation report"""
        system_status = self.get_system_status()
        recent_alerts = self.get_recent_alerts(limit=100)
        
        # Calculate alert trends
        alert_trends = {}
        for alert in recent_alerts:
            date = alert["timestamp"][:10]  # Extract date
            if date not in alert_trends:
                alert_trends[date] = {"total": 0, "by_severity": {}}
            alert_trends[date]["total"] += 1
            severity = alert["severity"]
            if severity not in alert_trends[date]["by_severity"]:
                alert_trends[date]["by_severity"][severity] = 0
            alert_trends[date]["by_severity"][severity] += 1
            
        return {
            "report_timestamp": datetime.now().isoformat(),
            "system_status": system_status,
            "recent_alerts": recent_alerts,
            "alert_trends": alert_trends,
            "summary": {
                "total_quality_checks": system_status["scheduler"]["total_checks"],
                "active_checks": system_status["scheduler"]["enabled_checks"],
                "total_alerts": system_status["alerts"]["total_alerts"],
                "unresolved_alerts": system_status["alerts"]["unresolved_alerts"],
                "system_health": "healthy" if system_status["alerts"]["unresolved_alerts"] < 10 else "warning"
            }
        }