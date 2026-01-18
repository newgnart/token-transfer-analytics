#!/usr/bin/env python3
"""
Script for chunking knowledge documents.

"""
from pathlib import Path
import logging
import argparse, json
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from scripts.knowledge.doc_processor import DocumentProcessor


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_docs(raw_docs_path: Path, processed_docs_path: Path):
    doc_processor = DocumentProcessor(output_dir_path=processed_docs_path)

    file_paths = list(raw_docs_path.glob("*.pdf"))
    file_paths += list(raw_docs_path.glob("*.docx"))

    results = doc_processor.process_documents(file_paths)

    return results


def processed_metadata():
    # Define paths
    processed_docs_dir = Path(".data/documents/processed_docs")
    output_csv = processed_docs_dir / "processed_docs.csv"

    # Check if directory exists
    if not processed_docs_dir.exists():
        print(f"Error: Directory {processed_docs_dir} does not exist")
        return

    # Collect all metadata
    metadata_list = []

    # Loop through all subdirectories
    for doc_dir in processed_docs_dir.iterdir():
        if not doc_dir.is_dir():
            continue

        metadata_file = doc_dir / "metadata.json"

        # Check if metadata.json exists
        if not metadata_file.exists():
            print(f"Warning: {metadata_file} not found, skipping {doc_dir.name}")
            continue

        # Read metadata
        try:
            with open(metadata_file, "r", encoding="utf-8") as f:
                metadata = json.load(f)
                metadata_list.append(metadata)
                print(f"Loaded metadata from {doc_dir.name}")
        except json.JSONDecodeError as e:
            print(f"Error reading {metadata_file}: {e}")
        except Exception as e:
            print(f"Unexpected error reading {metadata_file}: {e}")

    # Create DataFrame
    if not metadata_list:
        print("No metadata files found")
        return

    df = pd.DataFrame(metadata_list)
    if "file_size" in df.columns:
        df["file_size"] = df["file_size"].apply(
            lambda x: f"{x / (1024 * 1024):.2f} MB" if pd.notna(x) else None
        )

    # Save to CSV
    df.to_csv(output_csv, index=False, encoding="utf-8")
    print(f"\nSuccessfully exported {len(metadata_list)} documents to {output_csv}")
    print(f"\nDataFrame shape: {df.shape}")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nFirst few rows:")
    print(df.head())


def main():
    args = argparse.ArgumentParser(description="Process documents into chunks")
    args.add_argument(
        "-i",
        "--raw-docs-path",
        type=Path,
        default=Path(".data/documents/raw_docs"),
        help="Path to the raw documents directory",
    )
    args.add_argument(
        "-o",
        "--processed-docs-path",
        type=Path,
        default=Path(".data/documents/processed_docs"),
        help="Path to the output directory for processed documents",
    )
    args = args.parse_args()

    process_docs(args.raw_docs_path, args.processed_docs_path)


if __name__ == "__main__":
    main()
    # processed_metadata()
