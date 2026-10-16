"""Base Agent Interface - All agents in Agent Dhara implement this contract."""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TypeVar, Generic
from datetime import datetime
import uuid

T = TypeVar('T')
InputT = TypeVar('InputT')
OutputT = TypeVar('OutputT')


@dataclass
class AgentMetadata:
    """Metadata about agent execution."""
    agent_name: str
    execution_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    duration_ms: Optional[int] = None
    version: str = "1.0.0"
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class AgentInput(Generic[InputT]):
    """Standardized input for all agents."""
    data: InputT
    context: Dict[str, Any] = field(default_factory=dict)
    session_id: Optional[str] = None
    request_id: Optional[str] = None
    trace_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get from context with fallback."""
        return self.context.get(key, default)


@dataclass
class AgentOutput(Generic[OutputT]):
    """Standardized output for all agents."""
    data: OutputT
    metadata: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    metrics: Dict[str, float] = field(default_factory=dict)


@dataclass
class AgentResult(Generic[OutputT]):
    """Unified result envelope for all agent executions."""
    success: bool
    output: Optional[AgentOutput[OutputT]] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    error_details: Dict[str, Any] = field(default_factory=dict)
    metadata: AgentMetadata = field(default_factory=lambda: AgentMetadata(agent_name="unknown"))
    retryable: bool = False
    fallback_available: bool = False

    @classmethod
    def success_result(
        cls, 
        data: OutputT, 
        metadata: Optional[AgentMetadata] = None,
        warnings: Optional[List[str]] = None,
        metrics: Optional[Dict[str, float]] = None,
        **extra_metadata
    ) -> "AgentResult[OutputT]":
        agent_meta = metadata or AgentMetadata(agent_name="unknown")
        return cls(
            success=True,
            output=AgentOutput(
                data=data,
                warnings=warnings or [],
                metrics=metrics or {},
                metadata=extra_metadata
            ),
            metadata=agent_meta
        )

    @classmethod
    def failure_result(
        cls,
        error: str,
        error_code: Optional[str] = None,
        error_details: Optional[Dict[str, Any]] = None,
        metadata: Optional[AgentMetadata] = None,
        retryable: bool = False,
        fallback_available: bool = False
    ) -> "AgentResult[OutputT]":
        agent_meta = metadata or AgentMetadata(agent_name="unknown")
        return cls(
            success=False,
            error=error,
            error_code=error_code,
            error_details=error_details or {},
            metadata=agent_meta,
            retryable=retryable,
            fallback_available=fallback_available
        )


class BaseAgent(ABC, Generic[InputT, OutputT]):
    """
    Abstract base class for all Agent Dhara agents.
    
    Contract:
    - All agents must implement `execute()` and `validate_input()`
    - Agents are stateless - all state passed via input/output
    - Agents support retry with exponential backoff
    - Agents emit structured logs and metrics
    """
    
    def __init__(
        self, 
        name: str, 
        config: Optional[Dict[str, Any]] = None,
        memory_store: Optional[Any] = None,
        **kwargs
    ):
        self.name = name
        self.config = config or {}
        self.memory = memory_store
        self._metadata = AgentMetadata(agent_name=name)
    
    @property
    @abstractmethod
    def required_inputs(self) -> List[str]:
        """List of required input keys that must be present in input.context"""
        pass
    
    @property
    @abstractmethod
    def provided_outputs(self) -> List[str]:
        """List of output keys this agent produces"""
        pass
    
    @abstractmethod
    async def execute(self, input_data: AgentInput[InputT]) -> AgentResult[OutputT]:
        """
        Main execution logic. Must be implemented by subclass.
        
        Args:
            input_data: Validated input with context
            
        Returns:
            AgentResult with success/output or error
        """
        pass
    
    async def validate_input(self, input_data: AgentInput[InputT]) -> AgentResult[None]:
        """Validate required inputs are present."""
        missing = [key for key in self.required_inputs if key not in input_data.context]
        if missing:
            return AgentResult.failure_result(
                error=f"Missing required inputs: {missing}",
                error_code="VALIDATION_ERROR",
                error_details={"missing": missing},
                retryable=False
            )
        return AgentResult.success_result(None)
    
    async def run_with_retry(
        self, 
        input_data: AgentInput[InputT], 
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0
    ) -> AgentResult[OutputT]:
        """
        Execute with exponential backoff retry.
        
        Args:
            input_data: Input to process
            max_retries: Maximum retry attempts
            base_delay: Initial delay in seconds
            max_delay: Maximum delay cap
            
        Returns:
            AgentResult from successful execution or final failure
        """
        last_error = None
        
        for attempt in range(max_retries + 1):
            self._metadata.execution_id = f"{self.name}_{attempt}_{uuid.uuid4().hex[:8]}"
            
            # Validate input
            validation = await self.validate_input(input_data)
            if not validation.success:
                return AgentResult.failure_result(
                    error=validation.error or "Validation failed",
                    error_code=validation.error_code,
                    error_details=validation.error_details,
                    metadata=self._metadata
                )
            
            try:
                result = await self.execute(input_data)
                result.metadata = self._metadata
                
                if result.success:
                    self._metadata.completed_at = datetime.utcnow()
                    self._metadata.duration_ms = int(
                        (self._metadata.completed_at - self._metadata.started_at).total_seconds() * 1000
                    )
                    return result
                
                # Check if error is retryable
                if not result.retryable or attempt == max_retries:
                    self._metadata.completed_at = datetime.utcnow()
                    self._metadata.duration_ms = int(
                        (self._metadata.completed_at - self._metadata.started_at).total_seconds() * 1000
                    )
                    return result
                
                last_error = result.error
                
            except Exception as e:
                last_error = str(e)
                if attempt == max_retries:
                    return AgentResult.failure_result(
                        error=f"Agent {self.name} failed after {max_retries} retries: {e}",
                        error_code="EXECUTION_ERROR",
                        error_details={"exception": str(e), "attempt": attempt},
                        metadata=self._metadata,
                        retryable=False
                    )
            
            # Exponential backoff with jitter
            import random
            delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
            import asyncio
            await asyncio.sleep(delay)
        
        return AgentResult.failure_result(
            error=f"Max retries exceeded. Last error: {last_error}",
            error_code="MAX_RETRIES_EXCEEDED",
            metadata=self._metadata
        )
    
    def get_metadata(self) -> AgentMetadata:
        return self._metadata


# Type aliases for common agent patterns
AssessmentInput = Dict[str, Any]
AssessmentOutput = Dict[str, Any]

PlanInput = Dict[str, Any]
PlanOutput = Dict[str, Any]

CodegenInput = Dict[str, Any]
CodegenOutput = str  # Generated code

ValidationInput = Dict[str, Any]
ValidationOutput = Dict[str, Any]

ExecutionInput = Dict[str, Any]
ExecutionOutput = Dict[str, Any]