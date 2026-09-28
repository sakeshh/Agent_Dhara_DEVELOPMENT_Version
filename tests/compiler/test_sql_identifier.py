import pytest

from agent.compiler.schema.sql_identifier import (
    SqlTableIdentifier,
    parse_table_identifier,
    quote_identifier,
)


def test_quote_identifier():
    assert quote_identifier("Dhara_Employee") == "[Dhara_Employee]"
    assert quote_identifier("[Dhara_Employee]") == "[Dhara_Employee]"
    assert quote_identifier("Employee Name") == "[Employee Name]"
    assert quote_identifier("Table]Name") == "[Table]]Name]"

    with pytest.raises(ValueError, match="Cannot quote an empty identifier"):
        quote_identifier("")

    with pytest.raises(ValueError, match="Unsafe characters detected"):
        quote_identifier("Table; DROP TABLE Students;--")


def test_single_part_name():
    ident = parse_table_identifier("Dhara_Employee")
    assert ident.schema == "dbo"
    assert ident.table == "Dhara_Employee"
    assert ident.qualified_name == "[dbo].[Dhara_Employee]"
    assert ident.staging_name == "[dbo].[Dhara_Employee_Staging]"
    assert ident.clean_name == "[dbo].[Dhara_Employee_Clean]"
    assert "[dbo].[dbo." not in ident.qualified_name
    assert "[dbo].[dbo." not in ident.staging_name
    assert "[dbo].[dbo." not in ident.clean_name


def test_two_part_names_and_variations():
    cases = [
        "dbo.Dhara_Employee",
        "[dbo].[Dhara_Employee]",
        "[dbo].Dhara_Employee",
        "dbo.[Dhara_Employee]",
        "  dbo . [Dhara_Employee]  ",
        "custom_schema.Dhara_Employee",
        "[custom_schema].[Dhara_Employee]",
    ]
    for case in cases:
        ident = parse_table_identifier(case)
        assert ident.table == "Dhara_Employee"
        assert "[dbo].[dbo." not in ident.qualified_name
        assert "[dbo].[dbo." not in ident.staging_name
        assert "[dbo].[dbo." not in ident.clean_name

    # Check custom schema
    custom_ident = parse_table_identifier("custom_schema.Dhara_Employee")
    assert custom_ident.schema == "custom_schema"
    assert custom_ident.qualified_name == "[custom_schema].[Dhara_Employee]"
    assert custom_ident.staging_name == "[custom_schema].[Dhara_Employee_Staging]"
    assert custom_ident.clean_name == "[custom_schema].[Dhara_Employee_Clean]"


def test_three_part_names():
    ident = parse_table_identifier("analytics_db.dbo.Dhara_Employee")
    assert ident.database == "analytics_db"
    assert ident.schema == "dbo"
    assert ident.table == "Dhara_Employee"
    assert ident.qualified_name == "[analytics_db].[dbo].[Dhara_Employee]"
    assert ident.staging_name == "[analytics_db].[dbo].[Dhara_Employee_Staging]"
    assert ident.clean_name == "[analytics_db].[dbo].[Dhara_Employee_Clean]"
    assert "[dbo].[dbo." not in ident.qualified_name


def test_invalid_and_empty_names():
    with pytest.raises(ValueError, match="SQL table identifier cannot be empty"):
        parse_table_identifier("")

    with pytest.raises(ValueError, match="SQL table identifier cannot be empty"):
        parse_table_identifier("   ")

    with pytest.raises(ValueError):
        parse_table_identifier("[]")

    with pytest.raises(ValueError, match="empty segments"):
        parse_table_identifier("dbo..Dhara_Employee")

    with pytest.raises(ValueError, match="maximum 3 parts supported"):
        parse_table_identifier("server.db.schema.table")


def test_unsafe_injection_rejection():
    dangerous_inputs = [
        "dbo.Dhara_Employee; DROP TABLE users;--",
        "dbo.Dhara_Employee' OR 1=1--",
        "dbo.[Dhara_Employee/*comment*/]",
        "dbo.Dhara_Employee\x00extra",
    ]
    for inp in dangerous_inputs:
        with pytest.raises(ValueError, match="Unsafe characters detected"):
            parse_table_identifier(inp)


def test_staging_and_clean_derivation_objects():
    ident = parse_table_identifier("dbo.Dhara_Employee")
    stg = ident.staging_identifier()
    cln = ident.clean_identifier()

    assert isinstance(stg, SqlTableIdentifier)
    assert stg.table == "Dhara_Employee_Staging"
    assert stg.qualified_name == "[dbo].[Dhara_Employee_Staging]"

    assert isinstance(cln, SqlTableIdentifier)
    assert cln.table == "Dhara_Employee_Clean"
    assert cln.qualified_name == "[dbo].[Dhara_Employee_Clean]"
