import asyncio
import os

import boto3
from pyhocon import ConfigFactory

from src.definitions import CONFIG_PATH
from src.hello_world import run_hello_world_demo
from src.step_functions import (
    create_step_function, delete_step_function,
    get_step_function_adl, list_step_functions,
    update_step_function
)
from src.event_bridges import (
    create_event_bus, list_event_buses,
    create_event_rule, list_event_rules,
    delete_event_bus, delete_event_rule
)
from src.activities import (
    create_activity, describe_activity,
    list_activities, delete_activity
)

conf = ConfigFactory.parse_file(os.path.join(CONFIG_PATH, 'config.conf'))


async def demo_hello_world(stepfunctions, executor_role_arn):
    """
    Run the hello world demo with activity.

    Args:
        stepfunctions: Boto3 Step Functions client
        executor_role_arn: ARN of the execution role
    """
    activity_name = "demo-hello-world-activity"
    state_machine_name = "demo-hello-world-machine"

    # Create activity
    activity_arn = create_activity(stepfunctions, activity_name)
    if not activity_arn:
        return

    await run_hello_world_demo(
        stepfunctions,
        activity_arn,
        state_machine_name,
        executor_role_arn
    )


def demo_step_functions(stepfunctions, executor_role_arn):
    """
    Demonstrates Step Functions CRUD operations.

    Args:
        stepfunctions: Boto3 Step Functions client
        executor_role_arn: ARN of the role to execute the state machine
    """
    state_machine_name = "HelloWorldStateMachine"

    # Create the step function
    state_machine_arn = create_step_function(stepfunctions, state_machine_name, executor_role_arn)
    if not state_machine_arn:
        return

    # List all state machines
    list_step_functions(stepfunctions)

    # Get the ADL definition
    print("\nRetrieving initial ADL definition:")
    get_step_function_adl(stepfunctions, state_machine_arn)

    # Update the state machine
    print("\nUpdating state machine with new state:")
    if update_step_function(stepfunctions, state_machine_arn, executor_role_arn):
        # Get the updated ADL definition to verify the update
        print("\nVerifying update - retrieving updated ADL definition:")
        get_step_function_adl(stepfunctions, state_machine_arn)

        # Delete the state machine
        print("\nDeleting state machine:")
        if delete_step_function(stepfunctions, state_machine_arn):
            # List state machines to verify deletion
            print("\nVerifying deletion - listing remaining state machines:")
            list_step_functions(stepfunctions)

def demo_event_bridges(eventbridge):
    """
    Demonstrates EventBridge CRUD operations.

    Args:
        eventbridge: Boto3 EventBridge client
    """
    event_bus_name = "MyEventBus"
    event_rule_name = "MyEventRule"

    # Create event bus
    event_bus_arn = create_event_bus(eventbridge, event_bus_name)
    if not event_bus_arn:
        return

    # List all event buses
    list_event_buses(eventbridge)

    # Create event rule
    rule_arn = create_event_rule(eventbridge, event_rule_name, event_bus_name)
    if not rule_arn:
        return

    # List event rules
    list_event_rules(eventbridge, event_bus_name)

    # Delete event rule
    print("\nDeleting event rule:")
    if delete_event_rule(eventbridge, event_rule_name, event_bus_name):
        print("\nVerifying event rule deletion:")
        list_event_rules(eventbridge, event_bus_name)

    # Delete event bus
    print("\nDeleting event bus:")
    if delete_event_bus(eventbridge, event_bus_arn):
        print("\nVerifying event bus deletion:")
        list_event_buses(eventbridge)

def demo_activities(stepfunctions):
    """
    Demonstrates Step Functions Activity CRUD operations.

    Args:
        stepfunctions: Boto3 Step Functions client
    """
    activity_name = "MySampleActivity"

    # Create activity
    activity_arn = create_activity(stepfunctions, activity_name)
    if not activity_arn:
        return

    # List activities
    list_activities(stepfunctions)

    # Describe the newly created activity
    describe_activity(stepfunctions, activity_arn)

    # Delete activity
    print("\nDeleting activity:")
    if delete_activity(stepfunctions, activity_arn):
        print("\nVerifying activity deletion:")
        list_activities(stepfunctions)

async def amain() -> None:
    # Initialize AWS session using the system's default credentials
    session = boto3.Session()
    stepfunctions = session.client("stepfunctions")
    eventbridge = session.client("events")

    executor_role_arn = conf['executor_role_arn']

    # Run Step Functions demo
    print("\nStarting Step Functions demo:")
    demo_step_functions(stepfunctions, executor_role_arn)

    # Run EventBridge demo
    print("\nStarting EventBridge demo:")
    demo_event_bridges(eventbridge)

    # Run Activities demo
    print("\nStarting Activities demo:")
    demo_activities(stepfunctions)

    # Run Hello World demo
    print("\nStarting Hello World demo:")
    await demo_hello_world(stepfunctions, executor_role_arn)

def main() -> None:
    asyncio.run(amain())

if __name__ == "__main__":
    main()
