import json
from typing import Optional
import boto3
from botocore.client import BaseClient

def create_event_bus(client: BaseClient, event_bus_name: str) -> Optional[str]:
    """
    Create a new EventBridge event bus.

    Args:
        client: Boto3 EventBridge client
        event_bus_name: Name of the event bus to create

    Returns:
        The ARN of the created event bus if successful, None otherwise
    """
    try:
        response = client.create_event_bus(Name=event_bus_name)
        event_bus_arn = response['EventBusArn']
        print(f"Event Bus created successfully: {event_bus_arn}")
        return event_bus_arn
    except Exception as e:
        print(f"Error creating Event Bus: {e}")
        return None

def list_event_buses(client: BaseClient) -> None:
    """
    List all EventBridge event buses.

    Args:
        client: Boto3 EventBridge client
    """
    try:
        event_buses = client.list_event_buses()
        print("\nInstalled Event Buses:")
        for bus in event_buses['EventBuses']:
            print(f"- Name: {bus['Name']}, ARN: {bus['Arn']}")
    except Exception as e:
        print(f"Error listing Event Buses: {e}")

def create_event_rule(client: BaseClient, rule_name: str, event_bus_name: str) -> Optional[str]:
    """
    Create a new EventBridge event rule.

    Args:
        client: Boto3 EventBridge client
        rule_name: Name of the event rule to create
        event_bus_name: Name of the event bus to associate the rule with

    Returns:
        The ARN of the created event rule if successful, None otherwise
    """
    try:
        response = client.put_rule(
            Name=rule_name,
            EventBusName=event_bus_name,
            EventPattern=json.dumps({
                "source": ["aws.s3"],
                "detail-type": ["AWS API Call via CloudTrail"]
            })
        )
        rule_arn = response['RuleArn']
        print(f"Event Rule created successfully: {rule_arn}")
        return rule_arn
    except Exception as e:
        print(f"Error creating Event Rule: {e}")
        return None

def list_event_rules(client: BaseClient, event_bus_name: str) -> None:
    """
    List all EventBridge event rules for a specific event bus.

    Args:
        client: Boto3 EventBridge client
        event_bus_name: Name of the event bus to list rules for
    """
    try:
        rules = client.list_rules(EventBusName=event_bus_name)
        print("\nInstalled Event Rules:")
        for rule in rules['Rules']:
            print(f"- Name: {rule['Name']}, ARN: {rule['Arn']}")
    except Exception as e:
        print(f"Error listing Event Rules: {e}")

def delete_event_bus(client: BaseClient, event_bus_arn: str) -> bool:
    """
    Delete an EventBridge event bus.

    Args:
        client: Boto3 EventBridge client
        event_bus_arn: ARN of the event bus to delete

    Returns:
        bool indicating success or failure
    """
    try:
        client.delete_event_bus(EventBusName=event_bus_arn)
        print(f"Event Bus deleted successfully: {event_bus_arn}")
        return True
    except Exception as e:
        print(f"Error deleting Event Bus: {e}")
        return False

def delete_event_rule(client: BaseClient, rule_name: str, event_bus_name: str) -> bool:
    """
    Delete an EventBridge event rule.

    Args:
        client: Boto3 EventBridge client
        rule_name: Name of the event rule to delete
        event_bus_name: Name of the event bus the rule is associated with

    Returns:
        bool indicating success or failure
    """
    try:
        client.remove_targets(
            Rule=rule_name,
            EventBusName=event_bus_name,
            Ids=["target-id"]  # replace with your actual target ID
        )
        client.delete_rule(
            Name=rule_name,
            EventBusName=event_bus_name
        )
        print(f"Event Rule deleted successfully: {rule_name}")
        return True
    except Exception as e:
        print(f"Error deleting Event Rule: {e}")
        return False
