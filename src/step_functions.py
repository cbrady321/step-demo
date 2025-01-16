# step_functions.py
import json
from typing import Dict, List, Optional

from botocore.client import BaseClient


def get_step_function_adl(client: BaseClient, state_machine_arn: str) -> Optional[Dict]:
    """
    Retrieve the Amazon States Language (ASL/ADL) definition for a step function.

    Args:
        client: Boto3 step functions client
        state_machine_arn: ARN of the state machine to retrieve

    Returns:
        Dict containing the ASL definition if successful, None otherwise
    """
    try:
        response = client.describe_state_machine(
            stateMachineArn=state_machine_arn
        )
        definition = json.loads(response['definition'])
        print(f"Retrieved ASL definition for {state_machine_arn}:")
        print(json.dumps(definition, indent=2))
        return definition
    except Exception as e:
        print(f"Error retrieving state machine definition: {e}")
        return None


def update_step_function(client: BaseClient, state_machine_arn: str, executor_role_arn: str) -> bool:
    """
    Update an existing step function to add a new state.

    Args:
        client: Boto3 step functions client
        state_machine_arn: ARN of the state machine to update
        executor_role_arn: ARN of the role to execute the state machine

    Returns:
        bool indicating success or failure
    """
    try:
        # First get the current definition
        current_definition = get_step_function_adl(client, state_machine_arn)
        if not current_definition:
            return False

        # Add new state after HelloWorld
        updated_definition = {
            "Comment": "An updated Hello World example",
            "StartAt": "HelloWorld",
            "States": {
                "HelloWorld": {
                    "Type": "Pass",
                    "Result": "Hello, World!",
                    "Next": "GoodbyeWorld"
                },
                "GoodbyeWorld": {
                    "Type": "Pass",
                    "Result": "Goodbye, World!",
                    "End": True
                }
            }
        }

        # Update the state machine
        response = client.update_state_machine(
            stateMachineArn=state_machine_arn,
            definition=json.dumps(updated_definition),
            roleArn=executor_role_arn
        )
        print(f"State Machine updated successfully: {state_machine_arn}")
        return True
    except Exception as e:
        print(f"Error updating State Machine: {e}")
        return False


def create_step_function(client: BaseClient, state_machine_name: str, executor_role_arn: str) -> Optional[str]:
    """
    Create a new step function with a simple Hello World state.

    Args:
        client: Boto3 step functions client
        state_machine_name: Name for the new state machine
        executor_role_arn: ARN of the role to execute the state machine

    Returns:
        The ARN of the created state machine if successful, None otherwise
    """
    asl_definition = {
        "Comment": "A simple Hello World example",
        "StartAt": "HelloWorld",
        "States": {
            "HelloWorld": {
                "Type": "Pass",
                "Result": "Hello, World!",
                "End": True
            }
        }
    }

    try:
        response = client.create_state_machine(
            name=state_machine_name,
            definition=json.dumps(asl_definition),
            roleArn=executor_role_arn,
            type="STANDARD"
        )
        state_machine_arn = response['stateMachineArn']
        print(f"State Machine created successfully: {state_machine_arn}")
        return state_machine_arn
    except client.exceptions.StateMachineAlreadyExists:
        print("State Machine already exists.")
        return None
    except Exception as e:
        print(f"Error creating State Machine: {e}")
        return None


def list_step_functions(client: BaseClient) -> None:
    """
    List all available step functions.

    Args:
        client: Boto3 step functions client
    """
    try:
        state_machines = client.list_state_machines()
        print("\nInstalled State Machines:")
        for sm in state_machines['stateMachines']:
            print(f"- Name: {sm['name']}, ARN: {sm['stateMachineArn']}")
    except Exception as e:
        print(f"Error listing State Machines: {e}")

# step_functions.py
# Add this new function at the bottom of the file

def delete_step_function(client: BaseClient, state_machine_arn: str) -> bool:
    """
    Delete an existing step function.

    Args:
        client: Boto3 step functions client
        state_machine_arn: ARN of the state machine to delete

    Returns:
        bool indicating success or failure
    """
    try:
        client.delete_state_machine(
            stateMachineArn=state_machine_arn
        )
        print(f"State Machine deleted successfully: {state_machine_arn}")
        return True
    except Exception as e:
        print(f"Error deleting State Machine: {e}")
        return False