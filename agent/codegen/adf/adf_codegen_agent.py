from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from agent.compiler.base.codegen import BaseCodegenAgent
from agent.compiler.events.event_bus import CompilerEventType, EventBus
from agent.compiler.models.artifacts import CodeArtifact
from agent.compiler.models.context import CompilerContext
from agent.compiler.models.physical import PhysicalPlan


class ADFCodegenAgent(BaseCodegenAgent):
    """Generates enterprise Azure Data Factory (ADF) pipeline JSON definitions with Linked Services, Datasets, Activities, and Triggers."""

    def generate(self, physical_plan: PhysicalPlan) -> CodeArtifact:
        clean_plan_id = re.sub(r"[^a-zA-Z0-9_]", "_", physical_plan.plan_id)
        now_iso = datetime.now(timezone.utc).isoformat()

        activities: List[Dict[str, Any]] = []

        # 1. Validation Activity for each dataset
        for ds in physical_plan.dataset_names:
            clean_ds = re.sub(r"[^a-zA-Z0-9_]", "_", ds)
            activities.append({
                "name": f"Validate_{clean_ds}_Arrival",
                "type": "Validation",
                "dependsOn": [],
                "userProperties": [],
                "typeProperties": {
                    "dataset": {
                        "referenceName": f"ds_{clean_ds}_source",
                        "type": "DatasetReference"
                    },
                    "timeout": "00:15:00",
                    "sleep": 10,
                    "minimumSize": 1024
                }
            })

        # 2. Copy Activities (Source -> Staging / Sink)
        for ds in physical_plan.dataset_names:
            clean_ds = re.sub(r"[^a-zA-Z0-9_]", "_", ds)
            steps = physical_plan.get_steps_for_dataset(ds)

            activities.append({
                "name": f"Copy_{clean_ds}_To_Staging",
                "type": "Copy",
                "dependsOn": [
                    {
                        "activity": f"Validate_{clean_ds}_Arrival",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "policy": {
                    "timeout": "01:00:00",
                    "retry": 2,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": False,
                    "secureInput": False
                },
                "userProperties": [],
                "typeProperties": {
                    "source": {
                        "type": "DelimitedTextSource",
                        "storeSettings": {
                            "type": "AzureBlobFSReadSettings",
                            "recursive": True,
                            "enablePartitionDiscovery": False
                        },
                        "formatSettings": {
                            "type": "DelimitedTextReadSettings"
                        }
                    },
                    "sink": {
                        "type": "AzureSqlSink",
                        "writeBehavior": "insert",
                        "sqlWriterUseTableLock": False,
                        "tableOption": "autoCreate",
                        "preCopyScript": f"TRUNCATE TABLE staging_{clean_ds}"
                    },
                    "enableStaging": False
                },
                "inputs": [
                    {
                        "referenceName": f"ds_{clean_ds}_source",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": f"ds_staging_{clean_ds}",
                        "type": "DatasetReference"
                    }
                ]
            })

            # 3. Transform Execution Activity (Stored Procedure or Synapse Notebook Activity)
            activities.append({
                "name": f"Execute_{clean_ds}_Transformation",
                "type": "SqlServerStoredProcedure",
                "dependsOn": [
                    {
                        "activity": f"Copy_{clean_ds}_To_Staging",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "policy": {
                    "timeout": "00:30:00",
                    "retry": 1,
                    "retryIntervalInSeconds": 15
                },
                "userProperties": [],
                "typeProperties": {
                    "storedProcedureName": f"sp_clean_{clean_ds}",
                    "storedProcedureParameters": {
                        "PipelineRunId": {
                            "value": "@pipeline().RunId",
                            "type": "String"
                        },
                        "ExecutionTime": {
                            "value": "@pipeline().TriggerTime",
                            "type": "DateTime"
                        }
                    }
                },
                "linkedServiceName": {
                    "referenceName": "ls_azure_sql_target",
                    "type": "LinkedServiceReference"
                }
            })

        # Complete ADF Pipeline Definition JSON structure
        pipeline_json = {
            "name": f"pl_{clean_plan_id}",
            "properties": {
                "description": f"ADF Pipeline generated by Agent Dhara Compiler for {clean_plan_id}",
                "activities": activities,
                "parameters": {
                    "DataFactoryEnvironment": {
                        "type": "string",
                        "defaultValue": "Production"
                    },
                    "MaxConcurrency": {
                        "type": "int",
                        "defaultValue": 4
                    }
                },
                "variables": {
                    "ExecutionStatus": {
                        "type": "String",
                        "defaultValue": "Initializing"
                    }
                },
                "annotations": [
                    "AgentDhara",
                    "CompilerGenerated",
                    clean_plan_id
                ]
            }
        }

        # Linked services and datasets companion manifest
        bundle = {
            "pipeline": pipeline_json,
            "linked_services": [
                {
                    "name": "ls_azure_blob_source",
                    "type": "AzureBlobFS",
                    "typeProperties": {
                        "url": "https://storageaccount.dfs.core.windows.net"
                    }
                },
                {
                    "name": "ls_azure_sql_target",
                    "type": "AzureSqlDatabase",
                    "typeProperties": {
                        "connectionString": "@{linkedService().DBConnString}"
                    }
                }
            ],
            "datasets": [
                {
                    "name": f"ds_{re.sub(r'[^a-zA-Z0-9_]', '_', ds)}_source",
                    "type": "DelimitedText",
                    "linkedServiceName": {"referenceName": "ls_azure_blob_source", "type": "LinkedServiceReference"}
                }
                for ds in physical_plan.dataset_names
            ],
            "triggers": [
                {
                    "name": f"trig_daily_{clean_plan_id}",
                    "type": "ScheduleTrigger",
                    "typeProperties": {
                        "recurrence": {
                            "frequency": "Day",
                            "interval": 1,
                            "startTime": now_iso,
                            "timeZone": "UTC"
                        }
                    },
                    "pipelines": [
                        {
                            "pipelineReference": {
                                "referenceName": f"pl_{clean_plan_id}",
                                "type": "PipelineReference"
                            }
                        }
                    ]
                }
            ]
        }

        raw_content = json.dumps(bundle, indent=2)

        artifact = CodeArtifact(
            artifact_id=f"art_{physical_plan.plan_id}_adf",
            name=f"adf_pipeline_{clean_plan_id}.json",
            language="json",
            content=raw_content,
            file_extension=".json",
            is_entrypoint=True,
            metadata={
                "engine": "adf",
                "activity_count": len(activities),
                "pipeline_name": f"pl_{clean_plan_id}",
                "datasets_count": len(physical_plan.dataset_names),
            }
        )
        return artifact

    def execute(self, context: CompilerContext) -> CompilerContext:
        if not context.physical_plan:
            raise ValueError("Cannot execute ADFCodegenAgent without a PhysicalPlan.")

        artifact = self.generate(context.physical_plan)
        context.code_artifact = artifact

        EventBus.get_instance().emit(
            event_type=CompilerEventType.CODE_GENERATED,
            session_id=context.session_id,
            payload={
                "artifact_id": artifact.artifact_id,
                "name": artifact.name,
                "engine": "adf",
                "activities": artifact.metadata.get("activity_count", 0),
            },
            source_agent=self.__class__.__name__,
        )
        return context
