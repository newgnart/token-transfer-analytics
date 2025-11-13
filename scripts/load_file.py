import argparse, os
from dotenv import load_dotenv
import polars as pl
import dlt

load_dotenv()
from utils.database_client import SnowflakeClient, PostgresClient


def load_dataframe(
    client: SnowflakeClient,
    df: pl.DataFrame,
    schema: str,
    table_name: str,
    write_disposition: str = "append",
    primary_key: list[str] = None,
    **kwargs,
):
    """
    Load Polars DataFrame directly to the database using DLT.

    Args:
        df: Polars DataFrame to load
        schema: Target schema name
        table_name: Target table name
        write_disposition: How to handle existing data ("append", "replace", "merge")
        primary_key: List of column names to use as primary key for merge operations.
                    Required when write_disposition="merge".
                    Example: ["contract_address", "chain"]

    Returns:
        DLT pipeline run result
    """
    # Validate merge requirements
    if write_disposition == "merge" and not primary_key:
        raise ValueError(
            "primary_key must be specified when write_disposition='merge'. "
            "Example: primary_key=['contract_address', 'chain']"
        )

    # Convert DataFrame to list of dicts for DLT
    data = df.to_dicts()

    # Create a DLT resource from the data
    resource = dlt.resource(data, name=table_name)

    # Apply primary key hint for merge operations
    if primary_key:
        resource.apply_hints(primary_key=primary_key)

    # Create pipeline with destination-specific configuration
    pipeline = dlt.pipeline(
        pipeline_name="dataframe_loader",
        destination=client.get_dlt_destination(),
        dataset_name=schema,
    )

    # Load data
    result = pipeline.run(
        resource,
        table_name=table_name,
        write_disposition=write_disposition,
    )

    return result


def upload_to_snowflake_stage(
    client: SnowflakeClient,
    file_path: str,
    schema: str,
    stage_name: str,
    # create_if_not_exists: bool = True,
):
    """
    Upload a file to a Snowflake stage.

    Args:
        client: SnowflakeClient instance
        file_path: Path to the file to upload
        stage_name: Name of the Snowflake stage (e.g., 'my_stage')
        schema: Schema name to set context.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    file_name = os.path.basename(file_path)

    print(f"Uploading {file_path} to Snowflake stage {stage_name}...")

    with client.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(f"USE SCHEMA {schema};")
            cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name};")
            # PUT command uploads local file to stage
            put_command = f"PUT file://{file_path} @{stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            print(f"Executing: {put_command}")
            cursor.execute(put_command)

            result = cursor.fetchall()
            if result:
                print(f"Upload result: {result}")

            print(f"✓ Successfully uploaded {file_name} to stage {stage_name}")

        except Exception as e:
            print(f"✗ Error uploading file to stage: {str(e)}")
            raise
        finally:
            cursor.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-f",
        "--file_path",
        type=str,
        help="File path",
        required=True,
    )
    parser.add_argument(
        "-c",
        "--client",
        type=str,
        help="Client name, either 'snowflake' or 'postgres'",
        required=True,
    )
    # warehouse/database is configured in the client
    parser.add_argument(
        "-s",
        "--schema",
        type=str,
        help="Schema name (not required when using --stage)",
        required=False,
    )
    parser.add_argument(
        "-t",
        "--table",
        type=str,
        help="Table name (not required when using --stage)",
        required=False,
    )
    parser.add_argument(
        "-w",
        "--write_disposition",
        type=str,
        help="Write disposition, either 'append' or 'replace' or 'merge'",
        default="append",
    )
    parser.add_argument(
        "-k",
        "--primary_key",
        type=str,
        help="Comma-separated list of column names to use as primary key for merge. Required when -w merge. Example: 'contract_address,chain'",
        default=None,
    )
    parser.add_argument(
        "--stage",
        action="store_true",
        help="Upload file to Snowflake stage instead of loading into table. Only works with -c snowflake. Requires --stage_name.",
    )
    parser.add_argument(
        "--stage_name",
        type=str,
        help="Snowflake stage name (e.g., 'my_stage' or 'my_database.my_schema.my_stage')",
        default=None,
    )

    args = parser.parse_args()
    if args.client == "snowflake":
        client = SnowflakeClient().from_env()
    elif args.client == "postgres":
        client = PostgresClient.from_env()
    else:
        raise ValueError(
            f"Invalid client: {args.client}, use 'snowflake' or 'postgres', or implement new client"
        )

    # Handle Snowflake stage upload
    if args.stage:
        if args.client != "snowflake":
            raise ValueError("--stage option only works with -c snowflake")
        if not args.stage_name:
            raise ValueError("--stage_name is required when using --stage")

        upload_to_snowflake_stage(
            client=client,
            file_path=args.file_path,
            schema=args.schema,
            stage_name=args.stage_name,
        )
        return

    if args.file_path.endswith(".csv"):
        df = pl.read_csv(args.file_path)
    elif args.file_path.endswith(".parquet"):
        df = pl.read_parquet(args.file_path)
    else:
        raise ValueError(
            f"Invalid file extension: {args.file_path}, use 'csv' or 'parquet'"
        )

    # Parse primary key if provided
    primary_key = None
    if args.primary_key:
        primary_key = [col.strip() for col in args.primary_key.split(",")]

    load_dataframe(
        client=client,
        df=df,
        schema=args.schema,
        table_name=args.table,
        write_disposition=args.write_disposition,
        primary_key=primary_key,
    )


if __name__ == "__main__":
    main()
