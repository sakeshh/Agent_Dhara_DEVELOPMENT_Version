"""Logging Setup - Structured logging with context propagation."""
from __future__ import annotations
import logging
import logging.config
import sys
import os
import json
from datetime import datetime
from typing import Dict, Any, Optional
from contextvars import ContextVar
from pythonjsonlogger import jsonlogger


# Context variables for request tracing
request_id_var: ContextVar[Optional[str]] = ContextVar('request_id', default=None)
session_id_var: ContextVar[Optional[str]] = ContextVar('session_id', default=None)
pipeline_id_var: ContextVar[Optional[str]] = ContextVar('pipeline_id', default=None)
user_id_var: ContextVar[Optional[str]] = ContextVar('user_id', default=None)
trace_id_var: ContextVar[Optional[str]] = ContextVar('trace_id', default=None)
span_id_var: ContextVar[Optional[str]] = ContextVar('span_id', default=None)


class ContextFilter(logging.Filter):
    """Add context variables to log records."""
    
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get()
        record.session_id = session_id_var.get()
        record.pipeline_id = pipeline_id_var.get()
        record.user_id = user_id_var.get()
        record.trace_id = trace_id_var.get()
        record.span_id = span_id_var.get()
        return True


class JsonFormatter(jsonlogger.JsonFormatter):
    """JSON formatter with context."""
    
    def add_fields(self, log_record: Dict[str, Any], record: logging.LogRecord, message_dict: Dict[str, Any]) -> None:
        super().add_fields(log_record, record, message_dict)
        
        # Add standard fields
        log_record['timestamp'] = datetime.utcnow().isoformat() + 'Z'
        log_record['level'] = record.levelname
        log_record['logger'] = record.name
        log_record['module'] = record.module
        log_record['function'] = record.funcName
        log_record['line'] = record.lineno
        
        # Add context fields
        for field in ['request_id', 'session_id', 'pipeline_id', 'user_id', 'trace_id', 'span_id']:
            value = getattr(record, field, None)
            if value:
                log_record[field] = value
        
        # Add extra fields
        if hasattr(record, 'extra_fields'):
            log_record.update(record.extra_fields)


class ConsoleFormatter(logging.Formatter):
    """Human-readable console formatter with colors."""
    
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, '')
        reset = self.RESET
        
        # Build context string
        context_parts = []
        for field in ['request_id', 'session_id', 'pipeline_id', 'user_id']:
            value = getattr(record, field, None)
            if value:
                context_parts.append(f"{field}={value[:8]}")
        
        context_str = f" [{', '.join(context_parts)}]" if context_parts else ""
        
        return (
            f"{color}{datetime.utcnow().strftime('%H:%M:%S')}{reset} "
            f"{color}{record.levelname:<8}{reset} "
            f"{record.name}{context_str} - "
            f"{record.getMessage()}"
        )


def setup_logging(
    level: str = "INFO",
    json_output: bool = False,
    log_file: Optional[str] = None,
    enable_console: bool = True,
) -> None:
    """Configure application logging."""
    
    # Clear existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # Add context filter to root
    root_logger.addFilter(ContextFilter())
    
    handlers = []
    
    # Console handler
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        if json_output:
            console_handler.setFormatter(JsonFormatter())
        else:
            console_handler.setFormatter(ConsoleFormatter())
        handlers.append(console_handler)
    
    # File handler
    if log_file:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=100 * 1024 * 1024,  # 100MB
            backupCount=10
        )
        file_handler.setFormatter(JsonFormatter())
        handlers.append(file_handler)
    
    # Add handlers
    for handler in handlers:
        root_logger.addHandler(handler)
    
    # Configure third-party loggers
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("azure").setLevel(logging.WARNING)
    
    # Create agent logger
    agent_logger = logging.getLogger("agent")
    agent_logger.setLevel(getattr(logging, level.upper()))
    
    return root_logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger with agent namespace."""
    return logging.getLogger(f"agent.{name}")


def set_context(
    request_id: Optional[str] = None,
    session_id: Optional[str] = None,
    pipeline_id: Optional[str] = None,
    user_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    span_id: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """Set context variables. Returns previous values for restoration."""
    previous = {
        'request_id': request_id_var.get(),
        'session_id': session_id_var.get(),
        'pipeline_id': pipeline_id_var.get(),
        'user_id': user_id_var.get(),
        'trace_id': trace_id_var.get(),
        'span_id': span_id_var.get(),
    }
    
    if request_id is not None:
        request_id_var.set(request_id)
    if session_id is not None:
        session_id_var.set(session_id)
    if pipeline_id is not None:
        pipeline_id_var.set(pipeline_id)
    if user_id is not None:
        user_id_var.set(user_id)
    if trace_id is not None:
        trace_id_var.set(trace_id)
    if span_id is not None:
        span_id_var.set(span_id)
    
    return previous


def clear_context() -> None:
    """Clear all context variables."""
    for var in [request_id_var, session_id_var, pipeline_id_var, user_id_var, trace_id_var, span_id_var]:
        var.set(None)


class LogContext:
    """Context manager for structured logging context."""
    
    def __init__(self, **context):
        self.context = context
        self.previous = {}
    
    def __enter__(self):
        self.previous = set_context(**self.context)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore previous context
        for key, value in self.previous.items():
            if value is not None:
                getattr(sys.modules[__name__], f"{key}_var").set(value)
            else:
                getattr(sys.modules[__name__], f"{key}_var").set(None)


def log_with_context(logger: logging.Logger, level: int, message: str, **extra) -> None:
    """Log with additional context fields."""
    logger.log(level, message, extra={'extra_fields': extra})


# Convenience functions
def log_info(logger: logging.Logger, message: str, **extra) -> None:
    log_with_context(logger, logging.INFO, message, **extra)


def log_warning(logger: logging.Logger, message: str, **extra) -> None:
    log_with_context(logger, logging.WARNING, message, **extra)


def log_error(logger: logging.Logger, message: str, **extra) -> None:
    log_with_context(logger, logging.ERROR, message, **extra)


def log_debug(logger: logging.Logger, message: str, **extra) -> None:
    log_with_context(logger, logging.DEBUG, message, **extra)


# Performance logging
class PerformanceLogger:
    """Logger for performance metrics."""
    
    def __init__(self, logger: logging.Logger, operation: str, **context):
        self.logger = logger
        self.operation = operation
        self.context = context
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.utcnow()
        log_info(self.logger, f"Starting {self.operation}", **self.context)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (datetime.utcnow() - self.start_time).total_seconds() * 1000
        
        if exc_type:
            log_error(
                self.logger, 
                f"{self.operation} failed after {duration_ms:.0f}ms",
                error=str(exc_val),
                duration_ms=duration_ms,
                **self.context
            )
        else:
            log_info(
                self.logger,
                f"{self.operation} completed in {duration_ms:.0f}ms",
                duration_ms=duration_ms,
                **self.context
            )
    
    def log_metric(self, name: str, value: float, unit: str = "") -> None:
        """Log a performance metric."""
        log_info(
            self.logger,
            f"{self.operation} - {name}: {value}{unit}",
            metric_name=name,
            metric_value=value,
            metric_unit=unit,
            **self.context
        )


def get_performance_logger(operation: str, **context) -> PerformanceLogger:
    """Get a performance logger for an operation."""
    logger = get_logger("performance")
    return PerformanceLogger(logger, operation, **context)