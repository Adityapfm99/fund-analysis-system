"""
Table parsing and classification service
"""
from typing import List, Dict, Any
import re

class TableParser:
    """Classify and parse tables from PDF pages"""

    def classify_table(self, table: List[List[str]]) -> str:
        """
        Classify table type based on header row (robust, flexible, logging)
        Returns: 'capital_calls', 'distributions', 'adjustments', or 'unknown'
        """
        if not table or not table[0]:
            return 'unknown'
        header = [h.lower().replace(' ', '').replace('-', '').replace('_', '') for h in table[0]]
        if (any('call' in h for h in header) or any('callnumber' in h for h in header)) and any('amount' in h for h in header):
            return 'capital_calls'
        if any('distribution' in h for h in header) or any('recallable' in h for h in header) or any('type' in h for h in header):
            return 'distributions'
        if any('adjustment' in h for h in header) or any('contribution' in h for h in header) or any('category' in h for h in header):
            return 'adjustments'
        return 'unknown'

    def parse_table(self, table: List[List[str]], table_type: str) -> List[Dict[str, Any]]:
        """
        Parse table rows into dicts based on type, with header normalization
        """
        if not table or len(table) < 2:
            return []
        header = [h.strip().lower().replace(' ', '_').replace('-', '_') for h in table[0]]
        header_map = {}
        for i, h in enumerate(header):
            if h in ["call_number", "call"]:
                header_map[i] = "call_type"
            elif h == "type":
                if table_type == "distributions":
                    header_map[i] = "distribution_type"
                elif table_type == "adjustments":
                    header_map[i] = "adjustment_type"
                else:
                    header_map[i] = h
            elif h == "amount":
                header_map[i] = "amount"
            elif h == "date":
                header_map[i] = "date"
            elif h == "recallable":
                header_map[i] = "recallable"
            elif h == "description":
                header_map[i] = "description"
            elif h == "category":
                header_map[i] = "category"
            elif h == "contribution_adjustment":
                header_map[i] = "is_contribution_adjustment"
            else:
                header_map[i] = h
        rows = table[1:]
        results = []
        for row in rows:
            row_dict = {header_map[i]: row[i].strip() if i < len(row) else None for i in range(len(header))}
            results.append(row_dict)
        return results
