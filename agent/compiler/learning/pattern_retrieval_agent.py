"""Pattern Retrieval Agent: Matches dataset column signatures to enterprise domains."""

import re
from typing import Any, Dict, List, Tuple
from agent.compiler.learning.models import PatternMatchRequest, PatternMatchResponse
from agent.compiler.learning.success_scoring import calculate_pattern_match_confidence

PATTERN_CATALOG: Dict[str, Dict[str, Any]] = {
    "Customer Master": {
        "domain": "Customer",
        "keywords": ["customer", "cust", "client", "email", "phone", "first_name", "last_name", "full_name", "address", "zip", "postal", "city", "country"],
        "suggested_actions": [
            "Email Format RFC Validation & Sanitization",
            "Phone Number E.164 Standardization",
            "Customer Entity Deduplication",
            "Whitespace Stripping on Names & Addresses",
        ],
    },
    "Employee Directory": {
        "domain": "Employee",
        "keywords": ["employee", "emp", "staff", "ssn", "national_id", "salary", "department", "dept", "job_title", "hire_date", "manager"],
        "suggested_actions": [
            "National ID / SSN Masking & Validation",
            "Salary Non-Negative Constraint Check",
            "Hire Date ISO8601 Standardization",
            "Department Code Canonicalization",
        ],
    },
    "Invoice Processing": {
        "domain": "Invoice",
        "keywords": ["invoice", "inv", "bill", "vendor", "tax", "subtotal", "total_amount", "due_date", "payment_terms", "currency"],
        "suggested_actions": [
            "Monetary Rounding & Currency Standardization",
            "Tax Calculation Consistency Cross-Check",
            "Due Date vs Invoice Date Chronology Validation",
            "Vendor ID Foreign Key Harmonization",
        ],
    },
    "Sales Orders": {
        "domain": "Orders",
        "keywords": ["order", "ord", "item", "quantity", "qty", "unit_price", "discount", "order_date", "shipping_address", "tracking_number"],
        "suggested_actions": [
            "Quantity & Unit Price Positive Check",
            "Discount Range [0, 1.0] Enclosure",
            "Order Line Deduplication",
            "Shipping Address Country Code Canonicalization",
        ],
    },
    "Product Catalog": {
        "domain": "Products",
        "keywords": ["product", "sku", "barcode", "upc", "ean", "category", "brand", "inventory", "stock_qty", "price", "description"],
        "suggested_actions": [
            "SKU / Barcode Checksum Validation",
            "Category Canonicalization & Hierarchy Check",
            "Stock Quantity Non-Negative Check",
            "Product Description HTML/Tag Stripping",
        ],
    },
    "Financial Ledger": {
        "domain": "Finance",
        "keywords": ["ledger", "account", "gl", "debit", "credit", "balance", "fiscal_year", "period", "transaction_id", "journal"],
        "suggested_actions": [
            "Debit / Credit Balanced Sum Assertion",
            "Fiscal Period Format Standardizer",
            "Zero-Tolerance Null Balance Imputation",
            "High-Precision Decimal Coercion",
        ],
    },
}


class PatternRetrievalAgent:
    """Classifies dataset domain pattern based on column signature matching."""

    def match_pattern(self, request: PatternMatchRequest) -> PatternMatchResponse:
        cols_norm = [c.strip().lower().replace(" ", "_") for c in request.columns]
        
        best_pattern = "Generic Data Profile"
        best_domain = "General"
        best_confidence = 0.0
        best_matched: List[str] = []
        best_actions = ["Whitespace Normalization", "Null Value Imputation", "Duplicate Record Elimination"]

        for pattern_name, spec in PATTERN_CATALOG.items():
            matched: List[str] = []
            exact_matches = 0
            keywords = spec["keywords"]

            for col in cols_norm:
                for kw in keywords:
                    if col == kw or kw in col:
                        matched.append(col)
                        if col == kw:
                            exact_matches += 1
                        break

            if matched:
                conf = calculate_pattern_match_confidence(
                    matched_columns_count=len(matched),
                    total_user_columns=len(cols_norm),
                    exact_matches=exact_matches,
                )
                if conf > best_confidence:
                    best_confidence = conf
                    best_pattern = pattern_name
                    best_domain = spec["domain"]
                    best_matched = matched
                    best_actions = spec["suggested_actions"]

        if best_confidence < 30.0:
            best_confidence = 50.0

        return PatternMatchResponse(
            pattern=best_pattern,
            domain=best_domain,
            confidence=best_confidence,
            matched_columns=best_matched or request.columns[:3],
            suggested_cleaning_actions=best_actions,
        )
