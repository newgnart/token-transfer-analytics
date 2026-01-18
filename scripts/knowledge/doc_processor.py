"""
Document Processor for extracting text and metadata from various document formats.
This module handles document parsing (pdf, word, sheet, etc.), chunking, and metadata extraction
and saving processed documents as markdown and json
"""

from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
import hashlib
import json
from markitdown import MarkItDown
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CHUNKING_CONFIG = {
    "chunk_size": int(os.getenv("CHUNK_SIZE", 3500)),  # ~875 tokens
    "chunk_overlap": int(os.getenv("CHUNK_OVERLAP", 700)),  # ~20% overlap
    "separators": ["\n\n", "\n", ". ", " ", ""],
}


@dataclass
class Document:
    """Represents a parsed document with metadata and content."""

    title: str
    id: str  # document id, unique for each document
    language: str  # vi, en
    content: str
    # stage: int
    file_path: Optional[Path] = None
    file_size: Optional[int] = None
    created_at: Optional[datetime] = None


@dataclass
class TextChunk:
    """Represents a chunk of text with metadata."""

    content: str
    # stage: int
    char_count: int
    word_count: int
    chunk_id: str
    document_id: str


class DocumentParser:
    """Convert document to markdown and extract metadata"""

    def __init__(self):
        self.parser = MarkItDown()

    def parse_document(
        self,
        file_path: Path,
        document_id: Optional[str] = None,
        # stage: int = 1,
    ) -> Document:
        """
        Parse a document file and return a Document object.

        Args:
            file_path: Path to the document file
            document_id: Optional custom document ID, if not provided, it will be auto-generated
            # stage: Stage of the document (used for different stage of the user chat), if not provided, it will be 1

        Returns:
            Document object with parsed content and metadata
        """
        logger.info(f"\n🔍 Parsing document: {file_path}")

        if isinstance(file_path, str):
            file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            logger.info(f"  → Converting to markdown...")
            # Use markitdown to convert document to markdown
            result = self.parser.convert(file_path)
            logger.info(f"  ✓ Content length: {len(result.text_content)} chars")

            # Generate document ID if not provided
            if document_id is None:
                content_hash = hashlib.md5(result.text_content.encode()).hexdigest()[:8]
                document_id = f"{content_hash}"

            logger.info(f"  → Document ID: {document_id}")

            document = Document(
                id=document_id,
                title=result.title or file_path.stem,
                content=result.text_content,
                language="vi",
                # stage=stage,
                file_path=file_path,
                file_size=file_path.stat().st_size,
                created_at=datetime.now(),
            )

            logger.info(f"  ✓ Document created: {document.title}")
            return document

        except Exception as e:
            logger.error(f"  ✗ Error parsing document: {str(e)}")
            raise RuntimeError(f"Failed to parse document {file_path}: {str(e)}")


class TextChunker:
    """Text chunker for breaking content into manageable pieces."""

    def __init__(
        self,
        chunk_size: int = CHUNKING_CONFIG["chunk_size"],
        chunk_overlap: int = CHUNKING_CONFIG["chunk_overlap"],
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    def chunk_document(self, document: Document) -> List[TextChunk]:
        """
        Chunk a document into overlapping text segments.

        Args:
            document: Document object to chunk

        Returns:
            List of TextChunk objects
        """
        logger.info(f"\n📄 Chunking document: {document.title}")

        if not document.content.strip():
            logger.info("  ⚠ Empty content, skipping")
            return []

        chunks = []
        text = document.content.strip()
        chunk_counter = 0

        # Split by paragraphs to maintain context
        paragraphs = text.split("\n\n")
        logger.info(f"  → Found {len(paragraphs)} paragraphs")
        current_chunk = ""

        for paragraph in paragraphs:
            paragraph = paragraph.strip()
            if not paragraph:
                continue

            # Check if adding this paragraph would exceed chunk size
            if (
                len(current_chunk) + len(paragraph) + 2 > self.chunk_size
                and current_chunk
            ):
                # Finalize current chunk
                chunk = self._create_chunk(
                    current_chunk, document.id, document, chunk_counter
                )
                chunks.append(chunk)
                chunk_counter += 1

                # Start new chunk with overlap
                current_chunk = (
                    self._get_overlap_text(current_chunk) + "\n\n" + paragraph
                )
            else:
                if current_chunk:
                    current_chunk += "\n\n" + paragraph
                else:
                    current_chunk = paragraph

        # Add final chunk
        if current_chunk.strip():
            chunk = self._create_chunk(
                current_chunk, document.id, document, chunk_counter
            )
            chunks.append(chunk)

        logger.info(f"  ✓ Created {len(chunks)} chunks")
        return chunks

    def _create_chunk(
        self, content: str, document_id: str, document: Document, chunk_counter: int
    ) -> TextChunk:
        """Create a TextChunk object"""
        word_count = len(content.split())
        chunk_id = f"{document_id}_{chunk_counter}"

        return TextChunk(
            content=content.strip(),
            # stage=document.stage,
            char_count=len(content.strip()),
            word_count=word_count,
            chunk_id=chunk_id,
            document_id=document_id,
        )

    def _get_overlap_text(self, text: str) -> str:
        """Get overlap text from the end of current chunk."""
        if self.chunk_overlap <= 0:
            return ""

        words = text.split()
        overlap_word_count = min(self.chunk_overlap // 5, len(words) // 2)

        if overlap_word_count > 0:
            return " ".join(words[-overlap_word_count:])

        return ""


class DocumentProcessor:
    """High-level document processor combining parsing and chunking."""

    def __init__(
        self,
        output_dir_path: Path,
    ):
        self.parser = DocumentParser()
        self.chunker = TextChunker()
        self.output_dir_path = output_dir_path
        # Ensure output directory exists
        self.output_dir_path.mkdir(parents=True, exist_ok=True)

    def _save_document(self, document: Document) -> Path:
        """Save document metadata to JSON file."""
        logger.info(f"\n💾 Saving document: {document.title}")

        # Create document-specific folder
        doc_folder = self.output_dir_path / f"{document.title}_{document.id}"
        doc_folder.mkdir(exist_ok=True)
        logger.info(f"  → Folder: {doc_folder}")

        # Convert document to dict for JSON serialization (excluding content)
        doc_metadata = asdict(document)
        # Remove content from metadata - store path to content file instead
        doc_metadata.pop("content", None)

        # Convert Path to string for JSON serialization
        if doc_metadata["file_path"]:
            doc_metadata["file_path"] = str(doc_metadata["file_path"])
        # Convert datetime to ISO string
        if doc_metadata["created_at"]:
            doc_metadata["created_at"] = doc_metadata["created_at"].isoformat()

        # Save document metadata
        doc_file = doc_folder / "metadata.json"
        with doc_file.open("w", encoding="utf-8") as f:
            json.dump(doc_metadata, f, indent=2, ensure_ascii=False)
        logger.info(f"  ✓ Saved metadata.json")

        # Save document content as markdown
        content_file = doc_folder / "content.md"
        with content_file.open("w", encoding="utf-8") as f:
            f.write(document.content)
        logger.info(f"  ✓ Saved content.md")

        return doc_folder

    def _save_chunks(self, chunks: List[TextChunk], doc_folder: Path) -> None:
        """Save text chunks to JSON file."""
        # Convert chunks to dict for JSON serialization
        chunks_data = []
        for chunk in chunks:
            chunk_dict = asdict(chunk)
            chunks_data.append(chunk_dict)

        # Save chunks
        chunks_file = doc_folder / "chunks.json"
        with chunks_file.open("w", encoding="utf-8") as f:
            json.dump(chunks_data, f, indent=2, ensure_ascii=False)

    def process_document(
        self,
        file_path: Path,
        document_id: Optional[str] = None,
        # stage: int = 1,
    ) -> Dict[str, Path]:
        """
        Process a document: parse and chunk it.

        Args:
            file_path: Path to the document file
            document_id: Optional custom document ID
            # stage: Stage of the document

        Returns:
            Dictionary mapping file_path to document folder
        """
        document = self.parser.parse_document(file_path, document_id)
        chunks = self.chunker.chunk_document(document)

        doc_folder = self._save_document(document)
        self._save_chunks(chunks, doc_folder)

        result = {file_path.stem: doc_folder}
        return result

    def process_documents(
        self,
        file_paths: List[Path],
        # stage: int = 1
    ) -> Dict[str, Path]:
        """
        Process multiple documents.

        Args:
            file_paths: List of file paths to process
            # stage: Stage of the document
        Returns:
            Dictionary mapping document_id to chunks file path
        """
        results = {}
        failed_count = 0

        for file_path in file_paths:
            try:
                result = self.process_document(
                    file_path,
                    # stage=stage
                )
                results.update(result)
            except Exception as e:
                failed_count += 1

        return results
