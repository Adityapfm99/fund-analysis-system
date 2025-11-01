from celery import Celery
from app.core.config import settings
from app.services.document_processor import DocumentProcessor

celery_app = Celery(
    "fund_analysis",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL
)

@celery_app.task(bind=True)
def process_document_task(self, file_path: str, document_id: int, fund_id: int):
    from app.db.session import SessionLocal
    from app.models.document import Document
    db = SessionLocal()
    try:
        from app.services.document_processor import DocumentProcessor
        import asyncio
        processor = DocumentProcessor()
        result = asyncio.run(processor.process_document(file_path, document_id, fund_id))
        document = db.query(Document).filter(Document.id == document_id).first()
        if document:
            document.parsing_status = result["status"]
            if result["status"] == "failed":
                document.error_message = result.get("error")
            db.commit()
        return result
    except Exception as e:
        print(f"[Celery] ERROR: {e}")
        document = db.query(Document).filter(Document.id == document_id).first()
        if document:
            document.parsing_status = "failed"
            document.error_message = str(e)
            db.commit()
        raise
    finally:
        db.close()
