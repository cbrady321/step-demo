# hello_world.py
import asyncio
import json
from typing import Optional, Dict
import boto3
from botocore.client import BaseClient


def create_hello_world_stepfunction(
        client: BaseClient,
        activity_arn: str,
        state_machine_name: str,
        executor_role_arn: str
) -> Optional[str]:
    """
    Create a hello world step function that uses an activity.

    Args:
        client: Boto3 Step Functions client
        activity_arn: ARN of the activity to use
        state_machine_name: Name for the state machine
        executor_role_arn: ARN of the execution role

    Returns:
        The ARN of the created state machine if successful, None otherwise
    """
    asl_definition = {
        "Comment": "A hello world example using an activity",
        "StartAt": "HelloWorldActivity",
        "States": {
            "HelloWorldActivity": {
                "Type": "Task",
                "Resource": activity_arn,
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
        print(f"Hello World State Machine created: {state_machine_arn}")
        return state_machine_arn
    except Exception as e:
        print(f"Error creating Hello World State Machine: {e}")
        return None


async def activity_worker(client: BaseClient, activity_arn: str) -> None:
    """
    Worker that listens for activity tasks and executes them.

    Args:
        client: Boto3 Step Functions client
        activity_arn: ARN of the activity to poll
    """
    while True:
        try:
            # Get activity task
            response = await asyncio.to_thread(
                client.get_activity_task,
                activityArn=activity_arn
            )

            if 'taskToken' in response:
                print("Executing Hello World activity...")

                # Send task success
                await asyncio.to_thread(
                    client.send_task_success,
                    taskToken=response['taskToken'],
                    output=json.dumps({"message": "Hello World!"})
                )
                print("Activity executed successfully")

            # Small delay to prevent tight polling
            await asyncio.sleep(1)

        except Exception as e:
            print(f"Error in activity worker: {e}")
            await asyncio.sleep(5)  # Longer delay on error


# hello_world.py

def start_hello_world_execution(client: BaseClient, state_machine_arn: str) -> Optional[str]:
    """
    Start an execution of the hello world step function.

    Args:
        client: Boto3 Step Functions client
        state_machine_arn: ARN of the state machine to execute

    Returns:
        The execution ARN if successful, None otherwise
    """
    try:
        response = client.start_execution(
            stateMachineArn=state_machine_arn,
            input=json.dumps({"data": "Hello World!"})
        )
        execution_arn = response['executionArn']
        print(f"Started execution: {execution_arn}")
        return execution_arn
    except Exception as e:
        print(f"Error starting execution: {e}")
        return None


async def run_hello_world_demo(
        client: BaseClient,
        activity_arn: str,
        state_machine_name: str,
        executor_role_arn: str
) -> None:
    """
    Run the complete hello world demo with activity worker.

    Args:
        client: Boto3 Step Functions client
        activity_arn: ARN of the activity to use
        state_machine_name: Name for the state machine
        executor_role_arn: ARN of the execution role
    """
    # Create the state machine
    state_machine_arn = create_hello_world_stepfunction(
        client,
        activity_arn,
        state_machine_name,
        executor_role_arn
    )
    if not state_machine_arn:
        return

    try:
        # Start the activity worker
        worker_task = asyncio.create_task(activity_worker(client, activity_arn))

        # Start the execution
        execution_arn = start_hello_world_execution(client, state_machine_arn)
        if not execution_arn:
            worker_task.cancel()
            return

        # Wait for execution to complete (with timeout)
        timeout = 60  # seconds
        start_time = asyncio.get_event_loop().time()

        while True:
            if asyncio.get_event_loop().time() - start_time > timeout:
                print("Execution timed out")
                break

            response = await asyncio.to_thread(
                client.describe_execution,
                executionArn=execution_arn
            )

            if response['status'] in ['SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED']:
                print(f"Execution completed with status: {response['status']}")
                if 'output' in response:
                    print(f"Execution output: {response['output']}")
                break

            await asyncio.sleep(2)

    except Exception as e:
        print(f"Error in hello world demo: {e}")
    finally:
        # Cleanup
        if worker_task:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass