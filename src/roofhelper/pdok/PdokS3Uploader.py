from datetime import datetime
import os
from pathlib import Path
import logging
import boto3
from botocore.client import Config

from roofhelper.defautlogging import setup_logging
from roofhelper.pdok.UploadResult import UploadResult

log = setup_logging()

# Enable verbose logging for boto3/botocore and urllib3
logging.getLogger('botocore').setLevel(logging.DEBUG)
logging.getLogger('botocore.credentials').setLevel(logging.INFO)  # Reduce credential spam
logging.getLogger('urllib3.connectionpool').setLevel(logging.DEBUG)
logging.getLogger('urllib3.util.retry').setLevel(logging.DEBUG)


class PdokS3Uploader:
    """Handles S3 file uploads for PDOK delivery, we might have to add s3 as file handler later."""

    def __init__(self, endpoint: str, access_key: str, secret_key: str):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self._s3_client = None

    from botocore.client import BaseClient

    def upload_file(self, geopackage_file: Path, s3_prefix: str, expected_gpkg_name: str) -> UploadResult:
        """Upload a geopackage file to S3 and return upload results."""
        log.info(f"Uploading {geopackage_file} to {self.endpoint}")

        try:
            date_marker: str = datetime.now().strftime("%Y%m%d%H%M%S")
            s3_destination: str = f"{s3_prefix}/rel{date_marker}/{expected_gpkg_name}"
            trigger_update_path: str = f"{s3_prefix}/rel{date_marker}"

            log.info("Starting file upload...")
            session = boto3.session.Session() # type: ignore

            s3_client = session.client(
                service_name='s3',
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                endpoint_url=self.endpoint,
            )

            s3_client.upload_file(
                str(geopackage_file),
                "deliveries",
                s3_destination
            )

            log.info(f"Done uploading {geopackage_file} to {self.endpoint}/{s3_destination}")

            return UploadResult(
                s3_upload_path=trigger_update_path,
                s3_destination=s3_destination,
                date_marker=date_marker,
                success=True
            )
        except Exception as e:
            error_msg = f"Failed to upload {geopackage_file}: {str(e)}"
            log.error(error_msg)
            log.error(f"Exception type: {type(e).__name__}")
            # Log the full exception for debugging
            import traceback
            log.error(f"Full traceback: {traceback.format_exc()}")
            return UploadResult(
                s3_upload_path="",
                s3_destination="",
                date_marker="",
                success=False,
                error_message=error_msg
            )
