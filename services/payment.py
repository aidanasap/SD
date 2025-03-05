import random
from models.status import StepStatus

def process_payment():
    return StepStatus.SUCCESS if random.choice([True, False]) else StepStatus.FAILED

def compensate_payment():
    return StepStatus.SUCCESS
