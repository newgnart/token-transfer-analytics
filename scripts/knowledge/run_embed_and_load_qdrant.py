#!/usr/bin/env python3
"""
Script for loading processed documents directly into Qdrant
"""
import asyncio, os
from pathlib import Path
import logging
from dotenv import load_dotenv

load_dotenv()
from scripts.utils.database_client import get_qdrant_client
from scripts.knowledge.embed_and_load_qdrant import load_processed_docs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    logger.info("Loading processed documents to Qdrant ===")
    qdrant_client = get_qdrant_client()
    await load_processed_docs(
        processed_docs_path=Path(".data/documents/processed_docs"),
        qdrant_client=qdrant_client,
        collection_name=os.getenv("QDRANT_KNOWLEDGE_COLLECTION")
        or "open_stablecoin_index",
        resume=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
