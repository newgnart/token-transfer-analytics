from typing import Optional, List
import os

# from mcp.server.fastmcp import FastMCP
from qdrant_client import models, QdrantClient
from openai import OpenAI
from scripts.utils.database_client import get_qdrant_client


OPENAI_EMBEDDING_MODEL = {
    "API_KEY": os.getenv("OPENAI_EMBEDDING_API_KEY") or os.getenv("OPENAI_API_KEY"),
    "MODEL": "text-embedding-3-large",
    "DIMENSION": 3072,
    "BATCH_SIZE": int(os.getenv("OPENAI_EMBEDDING_BATCH_SIZE", 10)),
    "MAX_RETRIES": int(os.getenv("OPENAI_EMBEDDING_MAX_RETRIES", 3)),
    "TIMEOUT": int(os.getenv("OPENAI_EMBEDDING_TIMEOUT", 30)),
}

OLLAMA_EMBEDDING_MODEL = {
    "HOST": os.getenv("OLLAMA_HOST", "http://localhost:11434"),
    "MODEL": os.getenv("OLLAMA_EMBEDDING_MODEL", "embeddinggemma"),
    "DIMENSION": int(os.getenv("OLLAMA_EMBEDDING_DIMENSION", 768)),
    "BATCH_SIZE": int(os.getenv("OLLAMA_EMBEDDING_BATCH_SIZE", 10)),
    "MAX_RETRIES": int(os.getenv("OLLAMA_EMBEDDING_MAX_RETRIES", 3)),
    "TIMEOUT": int(os.getenv("OLLAMA_EMBEDDING_TIMEOUT", 30)),
}

EMBEDDING_MODEL_PROVIDER = os.getenv("EMBEDDING_MODEL_PROVIDER", "openai")


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


# def embed_batch_ollama(texts: List[str]) -> List[List[float]]:
#     """Generate embeddings for a batch of texts using Ollama."""
#     model = OLLAMA_EMBEDDING_MODEL["MODEL"]

#     try:
#         result = ollama.embed(model=model, input=texts)
#         return result["embeddings"]
#     except Exception as e:
#         raise ValueError(
#             f"Failed to generate embeddings with Ollama model '{model}': {e}"
#         )


def embed_batch(texts: List[str]) -> List[List[float]]:
    """Generate embeddings for a batch of texts using configured provider."""
    if EMBEDDING_MODEL_PROVIDER == "openai":
        return embed_batch_openai(texts)
    # elif EMBEDDING_MODEL_PROVIDER == "ollama":
    #     return embed_batch_ollama(texts)
    else:
        raise ValueError(f"Unsupported embedding provider: {EMBEDDING_MODEL_PROVIDER}")


def embed_text(text: str) -> List[float]:
    """Generate embedding for a single text using configured provider."""
    return embed_batch([text])[0]


def search_qdrant_vectordb(
    query: str,
    collection_name: str,
    limit=5,
    document_filter=None,
    language_filter=None,
    stage_filter=None,
    qdrant_client: Optional[QdrantClient] = None,
):
    """Search vectors in Qdrant - content is stored directly in Qdrant payload."""

    qdrant_client = qdrant_client or get_qdrant_client()

    # Build filter for Qdrant search
    filter_conditions = []

    if document_filter:
        filter_conditions.append(
            models.FieldCondition(
                key="document_id", match=models.MatchValue(value=document_filter)
            )
        )

    if language_filter:
        filter_conditions.append(
            models.FieldCondition(
                key="language", match=models.MatchValue(value=language_filter)
            )
        )

    if stage_filter:
        # Match any of the provided stages
        for stage in stage_filter:
            filter_conditions.append(
                models.FieldCondition(key="stage", match=models.MatchValue(value=stage))
            )

    search_filter = None
    if filter_conditions:
        search_filter = models.Filter(must=filter_conditions)

    # Search vectors in Qdrant
    hits = qdrant_client.query_points(
        collection_name=collection_name,
        query=embed_text(query),
        query_filter=search_filter,
        limit=limit,
        with_payload=True,  # Get content directly from payload
    ).points

    if not hits:
        return []

    # Extract results directly from Qdrant payload
    results = []
    for hit in hits:
        results.append(
            {
                "score": hit.score,
                "document_id": hit.payload.get("document_id"),
                "document_title": hit.payload.get("document_title"),
                "stage": hit.payload.get("stage"),
                "chunk_id": hit.payload.get("chunk_id"),
                "content": hit.payload.get("content", ""),  # Content stored in Qdrant
                "language": hit.payload.get("language"),
                "char_count": hit.payload.get("char_count"),
                "word_count": hit.payload.get("word_count"),
            }
        )

    return results
