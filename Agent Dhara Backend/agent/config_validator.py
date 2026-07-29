import os
import logging

logger = logging.getLogger("agent.config_validator")

def validate_fabric_config() -> bool:
    """
    Validates that necessary environment variables for Fabric mirroring and execution are present and not empty.
    Logs warning messages for any missing config rather than crashing, to avoid breaking startup.
    Returns True if all configurations are present, False otherwise.
    """
    warnings = []
    
    workspace = os.getenv("FABRIC_WORKSPACE_ID") or os.getenv("FABRIC_WORKSPACE_NAME")
    lakehouse = os.getenv("FABRIC_LAKEHOUSE_NAME") or os.getenv("FABRIC_LAKEHOUSE_ID")
    
    if not workspace:
        warnings.append("FABRIC_WORKSPACE_ID or FABRIC_WORKSPACE_NAME is missing.")
    if not lakehouse:
        warnings.append("FABRIC_LAKEHOUSE_NAME or FABRIC_LAKEHOUSE_ID is missing.")
        
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    storage_container = os.getenv("AZURE_ASSESSMENT_CONTAINER") or os.getenv("AZURE_STORAGE_CONTAINER")
    storage_conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    
    if not storage_conn and not (storage_account and (os.getenv("AZURE_STORAGE_ACCOUNT_KEY") or os.getenv("AZURE_STORAGE_KEY"))):
        warnings.append("Azure Storage credentials are not fully configured (missing connection string or account name/key).")
    if not storage_container:
        warnings.append("AZURE_ASSESSMENT_CONTAINER or AZURE_STORAGE_CONTAINER is missing.")
         
    if warnings:
        logger.warning("Fabric / Storage configuration validation warnings:")
        for w in warnings:
            logger.warning(f"  - {w}")
        return False
        
    logger.info("Fabric / Storage configurations successfully validated.")
    return True
