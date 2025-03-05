class SagaOrchestrator:
    def __init__(self):
        self.steps = []
        self.compensation = []
    
    def add_step(self, action, compensation):
        self.steps.append((action, compensation))
    
    def execute(self):
        for action, compensation in self.steps:
            status = action()
            if status.value == "SUCCESS":
                self.compensation.append(compensation)
            else:
                self.rollback()
                return "Transaction Failed"
        return "Transaction Successful"
    
    def rollback(self):
        while self.compensation:
            compensation = self.compensation.pop()
            compensation()
