"""
Alert Manager - Data quality alerting and notifications
Similar to Ataccama's alerting capabilities
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import json
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dataclasses import dataclass
from enum import Enum
import requests

class AlertSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class AlertChannel(Enum):
    EMAIL = "email"
    SLACK = "slack"
    WEBHOOK = "webhook"
    LOG = "log"

@dataclass
class AlertRule:
    """Data structure for alert rules"""
    name: str
    condition: str  # Quality score threshold or validation failure
    severity: AlertSeverity
    channels: List[AlertChannel]
    enabled: bool = True
    cooldown_minutes: int = 60  # Prevent spam alerts

@dataclass
class Alert:
    """Data structure for alerts"""
    id: str
    rule_name: str
    severity: AlertSeverity
    message: str
    details: Dict[str, Any]
    timestamp: datetime
    resolved: bool = False

class AlertManager:
    """
    Manages data quality alerts and notifications
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the alert manager
        
        Args:
            config: Configuration dictionary for alert channels
        """
        self.config = config or {}
        self.alerts: List[Alert] = []
        self.alert_rules: List[AlertRule] = []
        self.last_alert_times: Dict[str, datetime] = {}
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
    def add_alert_rule(self, rule: AlertRule):
        """Add an alert rule"""
        self.alert_rules.append(rule)
        self.logger.info(f"Added alert rule: {rule.name}")
        
    def send_alert(self, alert_data: Dict[str, Any]):
        """Send alert based on quality check results"""
        try:
            # Determine alert severity based on quality score
            quality_score = alert_data.get('quality_score', 0)
            severity = self._determine_severity(quality_score)
            
            # Create alert message
            message = self._create_alert_message(alert_data)
            
            # Check if alert should be sent (cooldown)
            alert_key = f"{alert_data['check_name']}_{alert_data['dataset']}"
            if self._should_send_alert(alert_key, severity):
                # Send to all configured channels
                for channel in self._get_channels_for_severity(severity):
                    self._send_to_channel(channel, message, alert_data, severity)
                    
                # Record alert
                alert = Alert(
                    id=f"{alert_key}_{datetime.now().timestamp()}",
                    rule_name=alert_key,
                    severity=severity,
                    message=message,
                    details=alert_data,
                    timestamp=datetime.now()
                )
                self.alerts.append(alert)
                self.last_alert_times[alert_key] = datetime.now()
                
                self.logger.info(f"Alert sent: {message}")
            else:
                self.logger.info(f"Alert suppressed due to cooldown: {alert_key}")
                
        except Exception as e:
            self.logger.error(f"Error sending alert: {str(e)}")
            
    def _determine_severity(self, quality_score: float) -> AlertSeverity:
        """Determine alert severity based on quality score"""
        if quality_score >= 90:
            return AlertSeverity.LOW
        elif quality_score >= 80:
            return AlertSeverity.MEDIUM
        elif quality_score >= 70:
            return AlertSeverity.HIGH
        else:
            return AlertSeverity.CRITICAL
            
    def _create_alert_message(self, alert_data: Dict[str, Any]) -> str:
        """Create alert message from quality check data"""
        check_name = alert_data.get('check_name', 'Unknown')
        dataset = alert_data.get('dataset', 'Unknown')
        quality_score = alert_data.get('quality_score', 0)
        threshold = alert_data.get('threshold', 80)
        validation_status = alert_data.get('validation_status', 'Unknown')
        failed_rules = alert_data.get('failed_rules', 0)
        
        message = f"""
🚨 DATA QUALITY ALERT 🚨

Check: {check_name}
Dataset: {dataset}
Quality Score: {quality_score:.1f}% (Threshold: {threshold}%)
Validation Status: {validation_status}
Failed Rules: {failed_rules}

Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Please investigate the data quality issues immediately.
        """.strip()
        
        return message
        
    def _should_send_alert(self, alert_key: str, severity: AlertSeverity) -> bool:
        """Check if alert should be sent based on cooldown"""
        if alert_key not in self.last_alert_times:
            return True
            
        last_alert = self.last_alert_times[alert_key]
        cooldown_minutes = 60  # Default cooldown
        
        # Find matching rule for cooldown
        for rule in self.alert_rules:
            if rule.name == alert_key:
                cooldown_minutes = rule.cooldown_minutes
                break
                
        time_since_last = datetime.now() - last_alert
        return time_since_last.total_seconds() > (cooldown_minutes * 60)
        
    def _get_channels_for_severity(self, severity: AlertSeverity) -> List[AlertChannel]:
        """Get alert channels for severity level"""
        channels = []
        
        # Add channels based on severity
        if severity in [AlertSeverity.HIGH, AlertSeverity.CRITICAL]:
            channels.extend([AlertChannel.EMAIL, AlertChannel.SLACK])
        else:
            channels.append(AlertChannel.LOG)
            
        return channels
        
    def _send_to_channel(self, channel: AlertChannel, message: str, alert_data: Dict, severity: AlertSeverity):
        """Send alert to specific channel"""
        try:
            if channel == AlertChannel.EMAIL:
                self._send_email(message, alert_data)
            elif channel == AlertChannel.SLACK:
                self._send_slack(message, alert_data)
            elif channel == AlertChannel.WEBHOOK:
                self._send_webhook(message, alert_data)
            elif channel == AlertChannel.LOG:
                self._send_log(message, alert_data, severity)
        except Exception as e:
            self.logger.error(f"Failed to send alert via {channel.value}: {str(e)}")
            
    def _send_email(self, message: str, alert_data: Dict):
        """Send email alert"""
        email_config = self.config.get('email', {})
        if not email_config:
            self.logger.warning("Email configuration not found")
            return
            
        try:
            msg = MIMEMultipart()
            msg['From'] = email_config.get('from_email')
            msg['To'] = ', '.join(email_config.get('to_emails', []))
            msg['Subject'] = f"Data Quality Alert: {alert_data.get('check_name', 'Unknown')}"
            
            msg.attach(MIMEText(message, 'plain'))
            
            server = smtplib.SMTP(email_config.get('smtp_host'), email_config.get('smtp_port'))
            server.starttls()
            server.login(email_config.get('username'), email_config.get('password'))
            server.send_message(msg)
            server.quit()
            
            self.logger.info("Email alert sent successfully")
        except Exception as e:
            self.logger.error(f"Failed to send email: {str(e)}")
            
    def _send_slack(self, message: str, alert_data: Dict):
        """Send Slack alert"""
        slack_config = self.config.get('slack', {})
        if not slack_config:
            self.logger.warning("Slack configuration not found")
            return
            
        try:
            webhook_url = slack_config.get('webhook_url')
            if not webhook_url:
                self.logger.warning("Slack webhook URL not configured")
                return
                
            payload = {
                "text": message,
                "username": "Data Quality Bot",
                "icon_emoji": ":warning:"
            }
            
            response = requests.post(webhook_url, json=payload)
            response.raise_for_status()
            
            self.logger.info("Slack alert sent successfully")
        except Exception as e:
            self.logger.error(f"Failed to send Slack message: {str(e)}")
            
    def _send_webhook(self, message: str, alert_data: Dict):
        """Send webhook alert"""
        webhook_config = self.config.get('webhook', {})
        if not webhook_config:
            self.logger.warning("Webhook configuration not found")
            return
            
        try:
            webhook_url = webhook_config.get('url')
            if not webhook_url:
                self.logger.warning("Webhook URL not configured")
                return
                
            payload = {
                "message": message,
                "alert_data": alert_data,
                "timestamp": datetime.now().isoformat()
            }
            
            response = requests.post(webhook_url, json=payload)
            response.raise_for_status()
            
            self.logger.info("Webhook alert sent successfully")
        except Exception as e:
            self.logger.error(f"Failed to send webhook: {str(e)}")
            
    def _send_log(self, message: str, alert_data: Dict, severity: AlertSeverity):
        """Send log alert"""
        log_level = logging.ERROR if severity in [AlertSeverity.HIGH, AlertSeverity.CRITICAL] else logging.WARNING
        self.logger.log(log_level, f"DATA QUALITY ALERT: {message}")
        
    def get_alerts(self, limit: int = 100, severity: Optional[AlertSeverity] = None) -> List[Dict[str, Any]]:
        """Get recent alerts"""
        filtered_alerts = self.alerts
        
        if severity:
            filtered_alerts = [alert for alert in filtered_alerts if alert.severity == severity]
            
        # Sort by timestamp (newest first) and limit
        filtered_alerts.sort(key=lambda x: x.timestamp, reverse=True)
        return [
            {
                "id": alert.id,
                "rule_name": alert.rule_name,
                "severity": alert.severity.value,
                "message": alert.message,
                "timestamp": alert.timestamp.isoformat(),
                "resolved": alert.resolved
            }
            for alert in filtered_alerts[:limit]
        ]
        
    def resolve_alert(self, alert_id: str):
        """Mark alert as resolved"""
        for alert in self.alerts:
            if alert.id == alert_id:
                alert.resolved = True
                self.logger.info(f"Alert resolved: {alert_id}")
                break
                
    def get_alert_statistics(self) -> Dict[str, Any]:
        """Get alert statistics"""
        total_alerts = len(self.alerts)
        unresolved_alerts = len([alert for alert in self.alerts if not alert.resolved])
        
        severity_counts = {}
        for severity in AlertSeverity:
            severity_counts[severity.value] = len([
                alert for alert in self.alerts if alert.severity == severity
            ])
            
        return {
            "total_alerts": total_alerts,
            "unresolved_alerts": unresolved_alerts,
            "severity_breakdown": severity_counts,
            "alert_rules": len(self.alert_rules)
        }
        
    def save_configuration(self, file_path: str):
        """Save alert configuration to file"""
        config = {
            "alert_rules": [
                {
                    "name": rule.name,
                    "condition": rule.condition,
                    "severity": rule.severity.value,
                    "channels": [channel.value for channel in rule.channels],
                    "enabled": rule.enabled,
                    "cooldown_minutes": rule.cooldown_minutes
                }
                for rule in self.alert_rules
            ],
            "channels_config": self.config
        }
        
        with open(file_path, 'w') as f:
            json.dump(config, f, indent=2)
            
        self.logger.info(f"Alert configuration saved to: {file_path}")
        
    def load_configuration(self, file_path: str):
        """Load alert configuration from file"""
        try:
            with open(file_path, 'r') as f:
                config = json.load(f)
                
            # Load alert rules
            self.alert_rules = []
            for rule_config in config.get("alert_rules", []):
                rule = AlertRule(
                    name=rule_config["name"],
                    condition=rule_config["condition"],
                    severity=AlertSeverity(rule_config["severity"]),
                    channels=[AlertChannel(ch) for ch in rule_config["channels"]],
                    enabled=rule_config.get("enabled", True),
                    cooldown_minutes=rule_config.get("cooldown_minutes", 60)
                )
                self.alert_rules.append(rule)
                
            # Load channel configuration
            self.config = config.get("channels_config", {})
            
            self.logger.info(f"Alert configuration loaded from: {file_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to load alert configuration: {str(e)}")