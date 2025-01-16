import json
import os

import boto3
from pyhocon import ConfigFactory

from definitions import CONFIG_PATH

conf = ConfigFactory.parse_file(os.path.join(CONFIG_PATH, 'config.conf'))


def main():
    # Initialize AWS session using the system's default credentials
    session = boto3.Session()

    # Step Functions client
    stepfunctions = session.client("stepfunctions")

    # Define the Step Function ASL (Amazon States Language) script
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

    # Create the Step Function State Machine
    state_machine_name = "HelloWorldStateMachine"
    role_arn = conf['role_arn']  #"arn:aws:iam::581316237707:role/service-role/StepFunctions-MyStateMachine-6yrzpv66q-role-rkge0wdso"

    try:
        response = stepfunctions.create_state_machine(
            name=state_machine_name,
            definition=json.dumps(asl_definition),
            roleArn=role_arn,
            type="STANDARD"
        )
        print(f"State Machine created successfully: {response['stateMachineArn']}")
    except stepfunctions.exceptions.StateMachineAlreadyExists:
        print("State Machine already exists.")
    except Exception as e:
        print(f"Error creating State Machine: {e}")
        return

    # Verify that the State Machine is installed
    try:
        state_machines = stepfunctions.list_state_machines()
        print("Installed State Machines:")
        for sm in state_machines['stateMachines']:
            print(f"- Name: {sm['name']}, ARN: {sm['stateMachineArn']}")
    except Exception as e:
        print(f"Error listing State Machines: {e}")


if __name__ == "__main__":
    main()
