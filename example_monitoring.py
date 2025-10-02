
import json
import requests
from datetime import datetime
from data_quality_pipeline import DataQualityPipeline

class DataQualityMonitor:
    """Data Quality Monitoring and Alerting"""
    
    def __init__(self, webhook_url=None, slack_webhook=None):
        self.webhook_url = webhook_url
        self.slack_webhook = slack_webhook
        self.alert_thresholds = {
            'critical': 90,  # Alert if quality < 90%
            'warning': 95   # Alert if quality < 95%
        }
    
    def check_quality_thresholds(self, results):
        """Check results against quality thresholds"""
        alerts = []
        
        for result in results:
            if result['dimension'] == 'accuracy':
                quality_pct = result.get('accuracy_percentage', 100)
            elif result['dimension'] == 'completeness':
                quality_pct = 100 - result.get('null_percentage', 0)
            else:
                continue
            
            if quality_pct < self.alert_thresholds['critical']:
                alerts.append({
                    'severity': 'critical',
                    'rule_id': result['rule_id'],
                    'table': result['table_name'],
                    'quality_pct': quality_pct,
                    'message': f"CRITICAL: {result['rule_id']} quality is {quality_pct}%"
                })
            elif quality_pct < self.alert_thresholds['warning']:
                alerts.append({
                    'severity': 'warning',
                    'rule_id': result['rule_id'],
                    'table': result['table_name'],
                    'quality_pct': quality_pct,
                    'message': f"WARNING: {result['rule_id']} quality is {quality_pct}%"
                })
        
        return alerts
    
    def send_alert(self, alert):
        """Send alert to monitoring systems"""
        print(f"[{alert['severity'].upper()}] {alert['message']}")
        
        # Send to webhook
        if self.webhook_url:
            try:
                requests.post(self.webhook_url, json=alert, timeout=10)
            except Exception as e:
                print(f"Failed to send webhook alert: {e}")
        
        # Send to Slack
        if self.slack_webhook:
            try:
                slack_message = {
                    "text": alert['message'],
                    "color": "danger" if alert['severity'] == 'critical' else "warning"
                }
                requests.post(self.slack_webhook, json=slack_message, timeout=10)
            except Exception as e:
                print(f"Failed to send Slack alert: {e}")
    
    def monitor_data_quality(self, dq_pipeline):
        """Monitor data quality and send alerts"""
        results = dq_pipeline.get_all_results()
        alerts = self.check_quality_thresholds(results)
        
        for alert in alerts:
            self.send_alert(alert)
        
        return len(alerts)

# Example usage
def run_monitoring_example():
    """Example monitoring setup"""
    
    # Initialize monitoring
    monitor = DataQualityMonitor(
        webhook_url="https://your-monitoring-system.com/webhook",
        slack_webhook="https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
    )
    
    # Run data quality checks (in real scenario, this would be in your pipeline)
    # dq_pipeline = DataQualityPipeline(spark)
    # ... execute rules ...
    
    # Monitor and alert
    # alert_count = monitor.monitor_data_quality(dq_pipeline)
    # print(f"Sent {alert_count} alerts")
    