import os
import asyncio
import json
from pathlib import Path
from typing import Dict, List
import logging
import pandas as pd

from qdrant_client import models, QdrantClient
from openai import OpenAI

# import ollama
from scripts.knowledge.vector_search import (
    EMBEDDING_MODEL_PROVIDER,
    OPENAI_EMBEDDING_MODEL,
    OLLAMA_EMBEDDING_MODEL,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Get vector dimension based on provider
def get_embedding_model() -> Dict[str, any]:
    """Get the appropriate embedding model configuration based on the provider."""
    if EMBEDDING_MODEL_PROVIDER == "openai":
        return OPENAI_EMBEDDING_MODEL
    elif EMBEDDING_MODEL_PROVIDER == "ollama":
        return OLLAMA_EMBEDDING_MODEL
    else:
        raise ValueError(f"Unsupported embedding provider: {EMBEDDING_MODEL_PROVIDER}")


VECTOR_DIMENSION = get_embedding_model()["DIMENSION"]


def embed_batch_openai(texts: List[str]) -> List[List[float]]:
    """Generate embeddings for a batch of texts using OpenAI."""
    try:
        client = OpenAI(api_key=OPENAI_EMBEDDING_MODEL["API_KEY"])
    except Exception:
        raise ValueError(
            "We are using OPENAI for embedding, please provide OPENAI_API_KEY key in the .env file"
        )
    response = client.embeddings.create(
        input=texts, model=OPENAI_EMBEDDING_MODEL["MODEL"]
    )
    return [item.embedding for item in response.data]


def embed_batch_ollama(texts: List[str]) -> List[List[float]]:
    """Generate embeddings for a batch of texts using Ollama."""
    model = OLLAMA_EMBEDDING_MODEL["MODEL"]

    try:
        import ollama

        result = ollama.embed(model=model, input=texts)
        return result["embeddings"]
    except Exception as e:
        raise ValueError(
            f"Failed to generate embeddings with Ollama model '{model}': {e}"
        )


def embed_batch(texts: List[str]) -> List[List[float]]:
    """Generate embeddings for a batch of texts using configured provider."""
    if EMBEDDING_MODEL_PROVIDER == "openai":
        return embed_batch_openai(texts)
    elif EMBEDDING_MODEL_PROVIDER == "ollama":
        return embed_batch_ollama(texts)
    else:
        raise ValueError(f"Unsupported embedding provider: {EMBEDDING_MODEL_PROVIDER}")


def embed_text(text: str) -> List[float]:
    """Generate embedding for a single text using configured provider."""
    return embed_batch([text])[0]


async def load_processed_doc(
    _dir: Path,
    qdrant_client: QdrantClient,
    collection_name: str,
    resume: bool = True,
    batch_size: int = 10,
) -> Dict[str, any]:
    """Load processed document (a single processed doc) and upsert chunks to Qdrant vector database.

    Workflow:
        1. Read doc_metadata and chunks_data from files
        2. Check which chunks already exist in Qdrant (if resume=True)
        3. Only load chunks that don't exist
        4. Generate embeddings in batches and upsert to Qdrant immediately

    Args:
        _dir: Directory containing processed document (metadata.json, chunks.json)
        qdrant_client: Initialized Qdrant client
        collection_name: Name of the Qdrant collection
        resume: If True, skip chunks that already exist in Qdrant (default: True)
        batch_size: Number of chunks to batch for embedding generation

    Returns:
        Dict with stats: {
            "document_id": str,
            "total_chunks": int,
            "processed_chunks": int,
            "skipped_chunks": int,
            "success": bool
        }
    """
    # ========== STEP 0: Ensure Qdrant collection exists ==========
    collection_exists = False
    try:
        qdrant_client.get_collection(collection_name)
        logger.info(f"  ✓ Collection '{collection_name}' exists")
        collection_exists = True
    except Exception:
        # Collection doesn't exist, create it
        logger.info(f"  📦 Creating collection '{collection_name}'...")
        qdrant_client.create_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(
                size=VECTOR_DIMENSION,
                distance=models.Distance.COSINE,
            ),
        )
        logger.info(f"  ✨ Created collection with dimension {VECTOR_DIMENSION}")

    # ========== STEP 0.1: Ensure payload index exists for chunk_id ==========
    if collection_exists:
        try:
            # Create index on chunk_id for efficient filtering
            qdrant_client.create_payload_index(
                collection_name=collection_name,
                field_name="chunk_id",
                field_schema=models.PayloadSchemaType.KEYWORD,
            )
            logger.info(f"  🔍 Created payload index on 'chunk_id'")
        except Exception as e:
            # Index might already exist or creation failed
            if "already exists" in str(e).lower() or "existing" in str(e).lower():
                logger.debug(f"  ℹ️  Index on 'chunk_id' already exists")
            else:
                logger.debug(f"  ℹ️  Payload index note: {e}")

    # ========== STEP 1: Read document metadata and chunks ==========
    if not _dir.exists():
        raise FileNotFoundError(f"Directory not found: {str(_dir)}")

    metadata_file = _dir / "metadata.json"
    chunks_file = _dir / "chunks.json"

    if not (metadata_file.exists() and chunks_file.exists()):
        logger.warning(f"⚠️  Skipping {_dir.name} - missing metadata or chunks file")
        return {
            "document_id": _dir.name,
            "total_chunks": 0,
            "processed_chunks": 0,
            "skipped_chunks": 0,
            "success": False,
        }

    # Read metadata
    with open(metadata_file, "r", encoding="utf-8") as f:
        doc_metadata = json.load(f)

    # Read chunks
    with open(chunks_file, "r", encoding="utf-8") as f:
        chunks_data = json.load(f)

    document_id = doc_metadata["id"]
    logger.info(f"📄 Document: {document_id} ({len(chunks_data)} chunks)")

    # ========== STEP 2: Check which chunks already exist in Qdrant ==========
    existing_chunk_ids = set()

    if resume:
        logger.info(f"  🔍 Checking existing chunks in Qdrant...")
        try:
            # Build list of chunk_ids to check
            chunk_ids_to_check = []
            for idx, chunk in enumerate(chunks_data):
                chunk_id = chunk.get("chunk_id", f"{document_id}_{idx}")
                chunk_ids_to_check.append(chunk_id)

            # Use scroll with chunk_id filter (more efficient than document_id)
            # We check in batches to avoid large filter queries
            batch_size = 100
            for i in range(0, len(chunk_ids_to_check), batch_size):
                batch_chunk_ids = chunk_ids_to_check[i : i + batch_size]

                # Query Qdrant for these specific chunk_ids
                scroll_result = qdrant_client.scroll(
                    collection_name=collection_name,
                    limit=len(batch_chunk_ids),
                    scroll_filter=models.Filter(
                        should=[
                            models.FieldCondition(
                                key="chunk_id",
                                match=models.MatchValue(value=cid),
                            )
                            for cid in batch_chunk_ids
                        ]
                    ),
                    with_payload=["chunk_id"],
                    with_vectors=False,
                )
                points, _ = scroll_result

                # Collect existing chunk IDs
                for point in points:
                    chunk_id = point.payload.get("chunk_id", "")
                    if chunk_id:
                        existing_chunk_ids.add(chunk_id)

            logger.info(f"  ✓ Found {len(existing_chunk_ids)} existing chunks")
        except Exception as e:
            logger.warning(f"  ⚠️  Could not check existing chunks: {e}")
            logger.debug(f"  Continuing without deduplication check")

    # ========== STEP 3: Filter out chunks that already exist ==========
    chunks_to_load = []
    skipped_count = 0

    for idx, chunk in enumerate(chunks_data):
        # Use chunk_id from chunk data (already generated by doc_processor)
        chunk_id = chunk.get(
            "chunk_id", f"{document_id}_{idx}"
        )  # Fallback for backward compatibility

        if resume and chunk_id in existing_chunk_ids:
            skipped_count += 1
        else:
            chunks_to_load.append((idx, chunk, chunk_id))

    logger.info(f"  📊 To load: {len(chunks_to_load)}, To skip: {skipped_count}")

    if not chunks_to_load:
        logger.info(f"✅ Document {document_id}: All chunks already exist")
        return {
            "document_id": document_id,
            "total_chunks": len(chunks_data),
            "processed_chunks": 0,
            "skipped_chunks": skipped_count,
            "success": True,
        }

    # ========== STEP 4: Generate embeddings and upsert to Qdrant ==========
    # Get starting point ID for new vectors
    try:
        collection_info = qdrant_client.get_collection(collection_name)
        next_id = collection_info.points_count
    except Exception:
        next_id = 0

    processed_count = 0

    # Process chunks in batches
    for batch_start in range(0, len(chunks_to_load), batch_size):
        batch_end = min(batch_start + batch_size, len(chunks_to_load))
        batch_chunks = chunks_to_load[batch_start:batch_end]

        try:
            # Extract texts for batch embedding
            texts = [chunk["content"] for _, chunk, _ in batch_chunks]

            # Generate embeddings for the batch
            vectors = embed_batch(texts)

            # Create points for this batch
            points = []
            for i, (idx, chunk, chunk_id) in enumerate(batch_chunks):
                points.append(
                    models.PointStruct(
                        id=next_id + processed_count + i,
                        vector=vectors[i],
                        payload={
                            "document_id": document_id,
                            "stage": doc_metadata.get("stage"),
                            "content": chunk["content"],
                            "chunk_id": chunk_id,
                            "char_count": chunk["char_count"],
                            "word_count": chunk["word_count"],
                        },
                    )
                )

            # Upsert batch immediately after embedding
            qdrant_client.upsert(collection_name=collection_name, points=points)
            processed_count += len(batch_chunks)
            logger.info(f"  ✓ Batch uploaded: {processed_count}/{len(chunks_to_load)}")

        except Exception as e:
            logger.error(f"  ❌ Error processing batch {batch_start}-{batch_end}: {e}")

            # Handle rate limits
            if "quota" in str(e).lower() or "rate" in str(e).lower():
                logger.info("  ⏳ Rate limit hit, waiting 60 seconds...")
                await asyncio.sleep(60)
            continue

    logger.info(
        f"✅ Document {document_id}: {processed_count} processed, {skipped_count} skipped"
    )

    return {
        "document_id": document_id,
        "total_chunks": len(chunks_data),
        "processed_chunks": processed_count,
        "skipped_chunks": skipped_count,
        "success": True,
    }


async def load_processed_docs(
    processed_docs_path: Path,
    qdrant_client: QdrantClient,
    collection_name: str,
    resume: bool = True,
    batch_size: int = 10,
) -> Dict[str, any]:
    """Load all processed documents from processed_docs_path to Qdrant.

    Args:
        processed_docs_path: Path to processed documents directory
        qdrant_client: Initialized Qdrant client
        collection_name: Name of the Qdrant collection
        resume: If True, skip chunks that already exist in Qdrant (default: True)
        batch_size: Number of chunks to batch for embedding generation

    Returns:
        Dict with overall stats: {
            "total_documents": int,
            "successful_documents": int,
            "failed_documents": int,
            "total_chunks_processed": int,
            "total_chunks_skipped": int,
            "document_results": List[Dict]
        }
    """
    # Get all document directories
    if not processed_docs_path.exists():
        raise FileNotFoundError(
            f"Processed docs directory not found: {processed_docs_path}"
        )

    doc_dirs = [d for d in processed_docs_path.iterdir() if d.is_dir()]
    logger.info(f"\n📚 Found {len(doc_dirs)} document directories")

    if not doc_dirs:
        logger.warning("⚠️  No document directories found")
        return {
            "total_documents": 0,
            "successful_documents": 0,
            "failed_documents": 0,
            "total_chunks_processed": 0,
            "total_chunks_skipped": 0,
            "document_results": [],
        }

    # Process each document
    document_results = []
    successful_count = 0
    failed_count = 0
    total_processed = 0
    total_skipped = 0

    for idx, doc_dir in enumerate(doc_dirs, 1):
        logger.info(f"\n[{idx}/{len(doc_dirs)}] Processing {doc_dir.name}...")

        try:
            result = await load_processed_doc(
                _dir=doc_dir,
                qdrant_client=qdrant_client,
                collection_name=collection_name,
                resume=resume,
                batch_size=batch_size,
            )

            document_results.append(result)

            if result["success"]:
                successful_count += 1
                total_processed += result["processed_chunks"]
                total_skipped += result["skipped_chunks"]
            else:
                failed_count += 1

        except Exception as e:
            logger.error(f"❌ Failed to process {doc_dir.name}: {e}")
            import traceback

            logger.error(f"Traceback:\n{traceback.format_exc()}")
            failed_count += 1
            document_results.append(
                {
                    "document_id": doc_dir.name,
                    "total_chunks": 0,
                    "processed_chunks": 0,
                    "skipped_chunks": 0,
                    "success": False,
                    "error": str(e),
                }
            )

    df = pd.DataFrame(document_results)
    df.to_csv(processed_docs_path / "embed_load_results_summary.csv", index=False)

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"🎉 SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"📁 Total documents: {len(doc_dirs)}")
    logger.info(f"✅ Successful: {successful_count}")
    logger.info(f"❌ Failed: {failed_count}")
    logger.info(f"📦 Total chunks processed: {total_processed}")
    logger.info(f"⏭️  Total chunks skipped: {total_skipped}")
    logger.info(f"{'='*60}\n")

    return {
        "total_documents": len(doc_dirs),
        "successful_documents": successful_count,
        "failed_documents": failed_count,
        "total_chunks_processed": total_processed,
        "total_chunks_skipped": total_skipped,
        "document_results": document_results,
    }
