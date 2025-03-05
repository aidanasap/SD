This project implements Saga Pattern for e-commerce checkout.

Components:

Saga Orchestrator: Manages transaction steps and rollback logic.

Payment Service: Handles payment processing and compensation.

Inventory Service: Manages inventory reservation and release.

Shipping Service: Handles shipment scheduling and cancellation.

Status Model: Defines step success and failure states.

Workflow:

Steps Execution: Each step is executed in sequence.

Failure Handling: If a step fails, all previously completed steps are rolled back.

Saga Orchestrator: Coordinates the execution and compensation logic.