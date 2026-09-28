from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


# Disallow dangerous SQL injection characters in identifier names
FORBIDDEN_CHARS_PATTERN = re.compile(r"[\x00;'\"]|--|/\*|\*/")


def quote_identifier(value: str) -> str:
    """Safely quotes a single SQL identifier part with square brackets, escaping internal brackets."""
    if not value or not value.strip():
        raise ValueError("Cannot quote an empty identifier.")
    cleaned = value.strip().strip("[]")
    if not cleaned:
        raise ValueError("Identifier cannot be empty or contain only brackets.")
    if FORBIDDEN_CHARS_PATTERN.search(cleaned):
        raise ValueError(f"Unsafe characters detected in identifier: {value!r}")
    return f"[{cleaned.replace(']', ']]')}]"


@dataclass(frozen=True)
class SqlTableIdentifier:
    """Canonical model for a schema-qualified SQL table identifier.
    
    Guarantees no redundant schema wrapping (e.g. [dbo].[dbo.Table]) and provides
    safe derivation of staging and clean table representations.
    """
    schema: str
    table: str
    database: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.schema or not self.schema.strip():
            raise ValueError("Schema name cannot be empty.")
        if not self.table or not self.table.strip():
            raise ValueError("Table name cannot be empty.")
        
        # Verify schema and table do not contain dangerous injection patterns
        if FORBIDDEN_CHARS_PATTERN.search(self.schema):
            raise ValueError(f"Unsafe characters in schema name: {self.schema!r}")
        if FORBIDDEN_CHARS_PATTERN.search(self.table):
            raise ValueError(f"Unsafe characters in table name: {self.table!r}")
        if self.database and FORBIDDEN_CHARS_PATTERN.search(self.database):
            raise ValueError(f"Unsafe characters in database name: {self.database!r}")

    @property
    def base_table(self) -> str:
        """Unquoted raw base table name."""
        return self.table

    @property
    def base_schema(self) -> str:
        """Unquoted raw schema name."""
        return self.schema

    @property
    def qualified_name(self) -> str:
        """Fully bracketed schema-qualified name: [schema].[table] (or [db].[schema].[table])."""
        if self.database:
            return f"{quote_identifier(self.database)}.{quote_identifier(self.schema)}.{quote_identifier(self.table)}"
        return f"{quote_identifier(self.schema)}.{quote_identifier(self.table)}"

    @property
    def staging_name(self) -> str:
        """Fully bracketed staging table name: [schema].[table_Staging]."""
        stg_table = f"{self.table}_Staging"
        if self.database:
            return f"{quote_identifier(self.database)}.{quote_identifier(self.schema)}.{quote_identifier(stg_table)}"
        return f"{quote_identifier(self.schema)}.{quote_identifier(stg_table)}"

    @property
    def clean_name(self) -> str:
        """Fully bracketed clean table name: [schema].[table_Clean]."""
        clean_table = f"{self.table}_Clean"
        if self.database:
            return f"{quote_identifier(self.database)}.{quote_identifier(self.schema)}.{quote_identifier(clean_table)}"
        return f"{quote_identifier(self.schema)}.{quote_identifier(clean_table)}"

    def staging_identifier(self) -> SqlTableIdentifier:
        """Returns a new SqlTableIdentifier for the staging table."""
        return SqlTableIdentifier(schema=self.schema, table=f"{self.table}_Staging", database=self.database)

    def clean_identifier(self) -> SqlTableIdentifier:
        """Returns a new SqlTableIdentifier for the clean table."""
        return SqlTableIdentifier(schema=self.schema, table=f"{self.table}_Clean", database=self.database)

    def __str__(self) -> str:
        return self.qualified_name


def parse_table_identifier(value: str, default_schema: str = "dbo") -> SqlTableIdentifier:
    """Parses a table identifier string into a canonical SqlTableIdentifier.
    
    Accepts:
      - 'Dhara_Employee'           -> [dbo].[Dhara_Employee]
      - 'dbo.Dhara_Employee'       -> [dbo].[Dhara_Employee]
      - '[dbo].[Dhara_Employee]'   -> [dbo].[Dhara_Employee]
      - '[dbo].Dhara_Employee'     -> [dbo].[Dhara_Employee]
      - 'dbo.[Dhara_Employee]'     -> [dbo].[Dhara_Employee]
      - 'db.dbo.Dhara_Employee'    -> [db].[dbo].[Dhara_Employee]
    
    Rejects:
      - Empty strings or strings with only whitespace/brackets
      - Strings with SQL injection characters (quotes, semicolons, comments)
      - Identifiers with >3 parts
    """
    if value is None:
        raise ValueError("Cannot parse None as SQL table identifier.")

    raw = str(value).strip()
    if not raw:
        raise ValueError("SQL table identifier cannot be empty.")

    if FORBIDDEN_CHARS_PATTERN.search(raw):
        raise ValueError(f"Unsafe characters detected in SQL table identifier: {value!r}")

    # Split by '.' taking care of bracketed components if any
    # Simple regex split on dot outside brackets or clean split if well-formed
    # We can split on '.' and clean each part
    parts = [part.strip().strip("[]").strip() for part in raw.split(".")]
    # Filter out empty parts that might arise from malformed input like 'dbo..Employee'
    if any(p == "" for p in parts):
        raise ValueError(f"Malformed SQL table identifier with empty segments: {value!r}")

    if len(parts) == 1:
        table_name = parts[0]
        if not table_name:
            raise ValueError(f"Invalid empty table name in identifier: {value!r}")
        return SqlTableIdentifier(schema=default_schema, table=table_name)

    if len(parts) == 2:
        schema_name, table_name = parts[0], parts[1]
        if not schema_name or not table_name:
            raise ValueError(f"Invalid empty segment in identifier: {value!r}")
        return SqlTableIdentifier(schema=schema_name, table=table_name)

    if len(parts) == 3:
        db_name, schema_name, table_name = parts[0], parts[1], parts[2]
        if not db_name or not schema_name or not table_name:
            raise ValueError(f"Invalid empty segment in 3-part identifier: {value!r}")
        return SqlTableIdentifier(schema=schema_name, table=table_name, database=db_name)

    raise ValueError(f"Unsupported SQL table identifier (maximum 3 parts supported): {value!r}")
