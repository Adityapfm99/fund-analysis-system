"""
Document processing service using pdfplumber

TODO: Implement the document processing pipeline
- Extract tables from PDF using pdfplumber
- Classify tables (capital calls, distributions, adjustments)
- Extract and chunk text for vector storage
- Handle errors and edge cases
"""
from typing import Dict, List, Any
import pdfplumber
import camelot
from app.core.config import settings
from app.services.table_parser import TableParser
from sqlalchemy.orm import Session
from app.models.transaction import CapitalCall, Distribution, Adjustment
from app.db.session import SessionLocal
from datetime import datetime
import re
from app.services.vector_store import VectorStore

class DocumentProcessor:
    """Process PDF documents and extract structured data"""
    
    def __init__(self):
        self.table_parser = TableParser()
    
    async def process_document(self, file_path: str, document_id: int, fund_id: int) -> Dict[str, Any]:
        db: Session = SessionLocal()
        stats = {"capital_calls": 0, "distributions": 0, "adjustments": 0, "pages": 0}
        try:
            all_tables = []
            with pdfplumber.open(file_path) as pdf:
                stats["pages"] = len(pdf.pages)
                for page_num, page in enumerate(pdf.pages):
                    # Extract tables with pdfplumber
                    tables = page.extract_tables(table_settings={
                        "vertical_strategy": "lines",
                        "horizontal_strategy": "lines",
                        "intersection_tolerance": 5,
                        "snap_tolerance": 3,
                        "join_tolerance": 3,
                        "edge_min_length": 3,
                        "min_words_vertical": 1,
                        "min_words_horizontal": 1
                    }) or []
                    print(f"[DocumentProcessor] Page {page_num+1} tables (pdfplumber): {tables}")
                    all_tables.extend(tables)
            try:
                camelot_tables = camelot.read_pdf(file_path, pages="all")
                for i, table in enumerate(camelot_tables):
                    df = table.df
                    table_data = df.values.tolist()
                    table_data.insert(0, list(df.columns))
                    all_tables.append(table_data)
            except Exception as e:
                print(f"[DocumentProcessor] Camelot error: {e}")
            all_tables = [t for t in all_tables if t and len(t) > 1]
            print(f"[DocumentProcessor] Total tables found: {len(all_tables)}")
            for table in all_tables:
                table_type = self.table_parser.classify_table(table)
                parsed_rows = self.table_parser.parse_table(table, table_type)
                for row in parsed_rows:
                    try:
                        call_type = row.get("call number") or row.get("call_type")
                        type_col = row.get("distribution_type") or row.get("type") or row.get("adjustment_type")                        # Capital Calls: hanya jika call_type ada
                        if table_type == "capital_calls" and call_type:
                            call = CapitalCall(
                                fund_id=fund_id,
                                call_date=self._parse_date(row.get("date")),
                                call_type=call_type,
                                amount=self._parse_amount(row.get("amount")),
                                description=row.get("description"),
                                created_at=datetime.utcnow(),
                            )
                            db.add(call)
                            stats["capital_calls"] += 1
                        elif (
                            table_type == "distributions" and type_col and (not call_type or call_type == "") and not ("adjust" in type_col.lower() or "recallable" in type_col.lower())
                        ) or (
                            type_col and ("return" in type_col.lower() or "income" in type_col.lower())
                        ):
                            dist = Distribution(
                                fund_id=fund_id,
                                distribution_date=self._parse_date(row.get("date")),
                                distribution_type=type_col,
                                is_recallable=self._parse_bool(row.get("recallable")),
                                amount=abs(self._parse_amount(row.get("amount"))),
                                description=row.get("description"),
                                created_at=datetime.utcnow(),
                            )
                            db.add(dist)
                            stats["distributions"] += 1
                        elif type_col and ("adjust" in type_col.lower() or "recallable" in type_col.lower()):
                            adj = Adjustment(
                                fund_id=fund_id,
                                adjustment_date=self._parse_date(row.get("date")),
                                adjustment_type=type_col,
                                category=row.get("category"),
                                amount=self._parse_amount(row.get("amount")),
                                is_contribution_adjustment=self._parse_bool(row.get("is_contribution_adjustment")),
                                description=row.get("description"),
                                created_at=datetime.utcnow(),
                            )
                            db.add(adj)
                            stats["adjustments"] += 1
                    except Exception as e:
                        print(f"[DocumentProcessor] Error inserting row: {e}")
                        continue
            db.commit()
            return {"status": "completed", **stats}
        except Exception as e:
            db.rollback()
            return {"status": "failed", "error": str(e), **stats}
        finally:
            db.close()
    
    def _parse_amount(self, value):
        if not value:
            return 0
        value = value.replace('$', '').replace(',', '').replace(' ', '').replace('−', '-')
        try:
            return float(value)
        except Exception:
            print(f"[DocumentProcessor] Error parsing amount: {value}")
            return 0

    def _parse_date(self, value):
        from datetime import datetime
        if not value:
            return None
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%Y/%m/%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(value, fmt).date()
            except Exception:
                continue
        print(f"[DocumentProcessor] Error parsing date: {value}")
        return None

    def _parse_bool(self, value):
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in ["yes", "true", "1"]

    def _chunk_text(self, text_content: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Chunk text content for vector storage
        
        TODO: Implement intelligent text chunking
        - Split text into semantic chunks
        - Maintain context overlap
        - Preserve sentence boundaries
        - Add metadata to each chunk
        
        Args:
            text_content: List of text content with metadata
            
        Returns:
            List of text chunks with metadata
        """
        # Not used, see process_document for chunking logic
        return text_content
