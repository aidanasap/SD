import random
from models.status import StepStatus

def reserve_inventory():
    return StepStatus.SUCCESS if random.choice([True, False]) else StepStatus.FAILED

def release_inventory():
    return StepStatus.SUCCESS
