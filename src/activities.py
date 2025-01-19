import json
from typing import Optional, Dict
from botocore.client import BaseClient

def create_activity(client: BaseClient, activity_name: str) -> Optional[str]:
    """
    Create a new activity.

    Args:
        client: Boto3 Step Functions client
        activity_name: The name of the activity to create

    Returns:
        The ARN of the created activity if successful, None otherwise
    """
    try:
        response = client.create_activity(name=activity_name)
        activity_arn = response["activityArn"]
        print(f"Activity created successfully: {activity_arn}")
        return activity_arn
    except Exception as e:
        print(f"Error creating Activity: {e}")
        return None

def describe_activity(client: BaseClient, activity_name: str) -> Optional[str]:
    """
    Describe an activity by name and return its ARN.

    Args:
        client: Boto3 Step Functions client
        activity_name: The name of the activity to describe

    Returns:
        The ARN of the activity if found, None otherwise
    """
    try:
        # List all activities
        activities = client.list_activities()
        for activity in activities.get("activities", []):
            if activity["name"] == activity_name:
                # Describe the specific activity by ARN
                response = client.describe_activity(activityArn=activity["activityArn"])
                print(f"\nActivity Description for '{activity_name}':")
                print(json.dumps(response, indent=2))
                return activity["activityArn"]

        print(f"Activity '{activity_name}' not found.")
        return None
    except Exception as e:
        print(f"Error describing Activity: {e}")
        return None

def list_activities(client: BaseClient) -> None:
    """
    List all activities.

    Args:
        client: Boto3 Step Functions client
    """
    try:
        response = client.list_activities()
        print("\nInstalled Activities:")
        for activity in response.get("activities", []):
            print(f"- Name: {activity['name']}, ARN: {activity['activityArn']}")
    except Exception as e:
        print(f"Error listing Activities: {e}")

def delete_activity(client: BaseClient, activity_arn: str) -> bool:
    """
    Delete an existing activity.

    Args:
        client: Boto3 Step Functions client
        activity_arn: The ARN of the activity to delete

    Returns:
        bool indicating success or failure
    """
    try:
        client.delete_activity(activityArn=activity_arn)
        print(f"Activity deleted successfully: {activity_arn}")
        return True
    except Exception as e:
        print(f"Error deleting Activity: {e}")
        return False
