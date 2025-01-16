# cleanup.py
import asyncio
from typing import List
import boto3
from botocore.client import BaseClient


def cleanup_resources(
        stepfunctions_client: BaseClient,
        eventbridge_client: BaseClient,
        project_prefix: str = "demo-"
) -> None:
    """
    Cleanup all resources created by our demo project.

    Args:
        stepfunctions_client: Boto3 Step Functions client
        eventbridge_client: Boto3 EventBridge client
        project_prefix: Prefix used to identify our demo resources
    """
    # Cleanup Step Functions and Activities
    try:
        # List and delete state machines
        state_machines = stepfunctions_client.list_state_machines()
        for sm in state_machines['stateMachines']:
            if sm['name'].startswith(project_prefix):
                stepfunctions_client.delete_state_machine(
                    stateMachineArn=sm['stateMachineArn']
                )
                print(f"Deleted state machine: {sm['name']}")

        # List and delete activities
        activities = stepfunctions_client.list_activities()
        for activity in activities['activities']:
            if activity['name'].startswith(project_prefix):
                stepfunctions_client.delete_activity(
                    activityArn=activity['activityArn']
                )
                print(f"Deleted activity: {activity['name']}")
    except Exception as e:
        print(f"Error cleaning up Step Functions resources: {e}")

    # Cleanup EventBridge
    try:
        # List and delete rules first
        buses = eventbridge_client.list_event_buses()
        for bus in buses['EventBuses']:
            if bus['Name'].startswith(project_prefix):
                # Delete all rules associated with the bus
                rules = eventbridge_client.list_rules(EventBusName=bus['Name'])
                for rule in rules['Rules']:
                    # Remove targets first
                    targets = eventbridge_client.list_targets_by_rule(
                        Rule=rule['Name'],
                        EventBusName=bus['Name']
                    )
                    if targets['Targets']:
                        target_ids = [t['Id'] for t in targets['Targets']]
                        eventbridge_client.remove_targets(
                            Rule=rule['Name'],
                            EventBusName=bus['Name'],
                            Ids=target_ids
                        )
                    # Delete rule
                    eventbridge_client.delete_rule(
                        Name=rule['Name'],
                        EventBusName=bus['Name']
                    )
                    print(f"Deleted rule: {rule['Name']}")

                # Delete event bus
                eventbridge_client.delete_event_bus(Name=bus['Name'])
                print(f"Deleted event bus: {bus['Name']}")
    except Exception as e:
        print(f"Error cleaning up EventBridge resources: {e}")


# cleanup.py

async def cleanup_executions(
        stepfunctions_client: BaseClient,
        state_machine_arn: str
) -> None:
    """
    Cleanup all executions for a given state machine.

    Args:
        stepfunctions_client: Boto3 Step Functions client
        state_machine_arn: ARN of the state machine
    """
    try:
        # List and stop all running executions
        response = stepfunctions_client.list_executions(
            stateMachineArn=state_machine_arn,
            statusFilter='RUNNING'
        )

        for execution in response['executions']:
            try:
                stepfunctions_client.stop_execution(
                    executionArn=execution['executionArn']
                )
                print(f"Stopped execution: {execution['executionArn']}")
            except Exception as e:
                print(f"Error stopping execution {execution['executionArn']}: {e}")

        # Wait for executions to complete
        await asyncio.sleep(5)

    except Exception as e:
        print(f"Error cleaning up executions: {e}")


async def cleanup_all_resources(
        stepfunctions_client: BaseClient,
        eventbridge_client: BaseClient,
        project_prefix: str = "demo-"
) -> None:
    """
    Cleanup all resources created by our demo project.

    Args:
        stepfunctions_client: Boto3 Step Functions client
        eventbridge_client: Boto3 EventBridge client
        project_prefix: Prefix used to identify our demo resources
    """
    # First cleanup any running executions
    state_machines = stepfunctions_client.list_state_machines()
    for sm in state_machines['stateMachines']:
        if sm['name'].startswith(project_prefix):
            await cleanup_executions(stepfunctions_client, sm['stateMachineArn'])

    # Then proceed with regular cleanup
    cleanup_resources(stepfunctions_client, eventbridge_client, project_prefix)