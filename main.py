from saga.orchestrator import SagaOrchestrator
from services.payment import process_payment, compensate_payment
from services.inventory import reserve_inventory, release_inventory
from services.shipping import schedule_shipping, cancel_shipping

saga = SagaOrchestrator()
saga.add_step(process_payment, compensate_payment)
saga.add_step(reserve_inventory, release_inventory)
saga.add_step(schedule_shipping, cancel_shipping)

result = saga.execute()
print(result)