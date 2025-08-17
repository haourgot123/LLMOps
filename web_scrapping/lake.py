import os
import json
import logging
from typing import Optional, List, Dict, Any, Union, BinaryIO
from datetime import datetime, timedelta
from pathlib import Path

try:
    from minio import Minio
    from minio.error import S3Error
    from minio.commonconfig import Tags
    from minio.lifecycleconfig import LifecycleConfig, Rule, Status, Expiration, Transition
except ImportError:
    print("MinIO client not installed. Please run: pip install minio")
    Minio = None
    S3Error = None

class MinioDataLake:
    """
    A comprehensive class for interacting with MinIO data lake.
    Provides methods for bucket management, file operations, and data handling.
    """
    
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = True,
        region: Optional[str] = None,
        http_client: Optional[Any] = None
    ):
        """
        Initialize MinIO client connection.
        
        Args:
            endpoint: MinIO server endpoint (e.g., 'localhost:9000')
            access_key: Access key for authentication
            secret_key: Secret key for authentication
            secure: Use HTTPS if True, HTTP if False
            region: AWS region (optional)
            http_client: Custom HTTP client (optional)
        """
        if Minio is None:
            raise ImportError("MinIO client library not installed")
            
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.region = region
        
        try:
            self.client = Minio(
                endpoint=endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
                region=region,
                http_client=http_client
            )
            # Test connection
            self.client.list_buckets()
            logging.info(f"Successfully connected to MinIO at {endpoint}")
        except S3Error as e:
            logging.error(f"Failed to connect to MinIO: {e}")
            raise
    
    def create_bucket(
        self, 
        bucket_name: str, 
        location: str = "us-east-1",
        object_lock: bool = False
    ) -> bool:
        """
        Create a new bucket in MinIO.
        
        Args:
            bucket_name: Name of the bucket to create
            location: Location constraint for the bucket
            object_lock: Enable object lock for versioning
            
        Returns:
            True if bucket created successfully, False otherwise
        """
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name, location=location, object_lock=object_lock)
                logging.info(f"Bucket '{bucket_name}' created successfully")
                return True
            else:
                logging.info(f"Bucket '{bucket_name}' already exists")
                return True
        except S3Error as e:
            logging.error(f"Failed to create bucket '{bucket_name}': {e}")
            return False
    
    def delete_bucket(self, bucket_name: str, force: bool = False) -> bool:
        """
        Delete a bucket from MinIO.
        
        Args:
            bucket_name: Name of the bucket to delete
            force: Force deletion even if bucket contains objects
            
        Returns:
            True if bucket deleted successfully, False otherwise
        """
        try:
            if force:
                # Remove all objects first
                objects = self.client.list_objects(bucket_name, recursive=True)
                for obj in objects:
                    self.client.remove_object(bucket_name, obj.object_name)
            
            self.client.remove_bucket(bucket_name)
            logging.info(f"Bucket '{bucket_name}' deleted successfully")
            return True
        except S3Error as e:
            logging.error(f"Failed to delete bucket '{bucket_name}': {e}")
            return False
    
    def list_buckets(self) -> List[str]:
        """
        List all buckets in MinIO.
        
        Returns:
            List of bucket names
        """
        try:
            buckets = self.client.list_buckets()
            return [bucket.name for bucket in buckets]
        except S3Error as e:
            logging.error(f"Failed to list buckets: {e}")
            return []
    
    def upload_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: Union[str, Path],
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Upload a file to MinIO bucket.
        
        Args:
            bucket_name: Name of the target bucket
            object_name: Name to give the object in MinIO
            file_path: Path to the local file
            content_type: MIME type of the file
            metadata: Custom metadata for the object
            tags: Tags for the object
            
        Returns:
            True if upload successful, False otherwise
        """
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                logging.error(f"File not found: {file_path}")
                return False
            
            # Auto-detect content type if not provided
            if content_type is None:
                content_type = self._get_content_type(file_path)
            
            # Upload file
            self.client.fput_object(
                bucket_name,
                object_name,
                str(file_path),
                content_type=content_type,
                metadata=metadata,
                tags=Tags(tags) if tags else None
            )
            
            logging.info(f"File uploaded successfully: {object_name} -> {bucket_name}")
            return True
            
        except S3Error as e:
            logging.error(f"Failed to upload file: {e}")
            return False
    
    def upload_data(
        self,
        bucket_name: str,
        object_name: str,
        data: Union[str, bytes, BinaryIO],
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Upload data directly to MinIO bucket.
        
        Args:
            bucket_name: Name of the target bucket
            object_name: Name to give the object in MinIO
            data: Data to upload (string, bytes, or file-like object)
            content_type: MIME type of the data
            metadata: Custom metadata for the object
            tags: Tags for the object
            
        Returns:
            True if upload successful, False otherwise
        """
        try:
            if isinstance(data, str):
                data = data.encode('utf-8')
                if content_type is None:
                    content_type = 'text/plain'
            
            if content_type is None:
                content_type = 'application/octet-stream'
            
            # Upload data
            self.client.put_object(
                bucket_name,
                object_name,
                data,
                length=len(data) if hasattr(data, '__len__') else None,
                content_type=content_type,
                metadata=metadata,
                tags=Tags(tags) if tags else None
            )
            
            logging.info(f"Data uploaded successfully: {object_name} -> {bucket_name}")
            return True
            
        except S3Error as e:
            logging.error(f"Failed to upload data: {e}")
            return False
    
    def download_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: Union[str, Path]
    ) -> bool:
        """
        Download a file from MinIO bucket.
        
        Args:
            bucket_name: Name of the source bucket
            object_name: Name of the object to download
            file_path: Local path where to save the file
            
        Returns:
            True if download successful, False otherwise
        """
        try:
            file_path = Path(file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            self.client.fget_object(bucket_name, object_name, str(file_path))
            logging.info(f"File downloaded successfully: {object_name} -> {file_path}")
            return True
            
        except S3Error as e:
            logging.error(f"Failed to download file: {e}")
            return False
    
    def get_object(
        self,
        bucket_name: str,
        object_name: str,
        start: Optional[int] = None,
        length: Optional[int] = None
    ) -> Optional[bytes]:
        """
        Get object data from MinIO bucket.
        
        Args:
            bucket_name: Name of the source bucket
            object_name: Name of the object to get
            start: Start byte position
            length: Number of bytes to read
            
        Returns:
            Object data as bytes, or None if failed
        """
        try:
            response = self.client.get_object(bucket_name, object_name, start, length)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except S3Error as e:
            logging.error(f"Failed to get object: {e}")
            return None
    
    def list_objects(
        self,
        bucket_name: str,
        prefix: str = "",
        recursive: bool = True,
        start_after: str = ""
    ) -> List[Dict[str, Any]]:
        """
        List objects in a bucket.
        
        Args:
            bucket_name: Name of the bucket
            prefix: Filter objects by prefix
            recursive: List objects recursively
            start_after: Start listing after this key
            
        Returns:
            List of object information dictionaries
        """
        try:
            objects = self.client.list_objects(
                bucket_name,
                prefix=prefix,
                recursive=recursive,
                start_after=start_after
            )
            
            object_list = []
            for obj in objects:
                object_list.append({
                    'name': obj.object_name,
                    'size': obj.size,
                    'last_modified': obj.last_modified,
                    'etag': obj.etag,
                    'content_type': getattr(obj, 'content_type', None)
                })
            
            return object_list
            
        except S3Error as e:
            logging.error(f"Failed to list objects: {e}")
            return []
    
    def delete_object(self, bucket_name: str, object_name: str) -> bool:
        """
        Delete an object from MinIO bucket.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object to delete
            
        Returns:
            True if deletion successful, False otherwise
        """
        try:
            self.client.remove_object(bucket_name, object_name)
            logging.info(f"Object deleted successfully: {object_name}")
            return True
        except S3Error as e:
            logging.error(f"Failed to delete object: {e}")
            return False
    
    def copy_object(
        self,
        source_bucket: str,
        source_object: str,
        dest_bucket: str,
        dest_object: str
    ) -> bool:
        """
        Copy an object from one location to another.
        
        Args:
            source_bucket: Source bucket name
            source_object: Source object name
            dest_bucket: Destination bucket name
            dest_object: Destination object name
            
        Returns:
            True if copy successful, False otherwise
        """
        try:
            self.client.copy_object(
                dest_bucket,
                dest_object,
                f"{source_bucket}/{source_object}"
            )
            logging.info(f"Object copied successfully: {source_object} -> {dest_object}")
            return True
        except S3Error as e:
            logging.error(f"Failed to copy object: {e}")
            return False
    
    def get_object_metadata(
        self,
        bucket_name: str,
        object_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get metadata for an object.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object
            
        Returns:
            Dictionary containing object metadata, or None if failed
        """
        try:
            stat = self.client.stat_object(bucket_name, object_name)
            return {
                'size': stat.size,
                'last_modified': stat.last_modified,
                'etag': stat.etag,
                'content_type': stat.content_type,
                'metadata': stat.metadata
            }
        except S3Error as e:
            logging.error(f"Failed to get object metadata: {e}")
            return None
    
    def set_bucket_policy(
        self,
        bucket_name: str,
        policy: str
    ) -> bool:
        """
        Set bucket policy.
        
        Args:
            bucket_name: Name of the bucket
            policy: JSON policy string
            
        Returns:
            True if policy set successfully, False otherwise
        """
        try:
            self.client.set_bucket_policy(bucket_name, policy)
            logging.info(f"Bucket policy set successfully for {bucket_name}")
            return True
        except S3Error as e:
            logging.error(f"Failed to set bucket policy: {e}")
            return False
    
    def get_bucket_policy(self, bucket_name: str) -> Optional[str]:
        """
        Get bucket policy.
        
        Args:
            bucket_name: Name of the bucket
            
        Returns:
            Policy string, or None if failed
        """
        try:
            policy = self.client.get_bucket_policy(bucket_name)
            return policy
        except S3Error as e:
            logging.error(f"Failed to get bucket policy: {e}")
            return None
    
    def set_bucket_lifecycle(
        self,
        bucket_name: str,
        rules: List[Dict[str, Any]]
    ) -> bool:
        """
        Set bucket lifecycle configuration.
        
        Args:
            bucket_name: Name of the bucket
            rules: List of lifecycle rules
            
        Returns:
            True if lifecycle set successfully, False otherwise
        """
        try:
            lifecycle_rules = []
            for rule_config in rules:
                rule = Rule(
                    status=Status.ENABLED,
                    rule_id=rule_config.get('id', f"rule_{len(lifecycle_rules)}"),
                    expiration=Expiration(days=rule_config.get('expiration_days')),
                    transition=Transition(
                        days=rule_config.get('transition_days'),
                        storage_class=rule_config.get('storage_class', 'STANDARD_IA')
                    ) if rule_config.get('transition_days') else None
                )
                lifecycle_rules.append(rule)
            
            config = LifecycleConfig(lifecycle_rules)
            self.client.set_bucket_lifecycle(bucket_name, config)
            logging.info(f"Bucket lifecycle set successfully for {bucket_name}")
            return True
        except S3Error as e:
            logging.error(f"Failed to set bucket lifecycle: {e}")
            return False
    
    def generate_presigned_url(
        self,
        bucket_name: str,
        object_name: str,
        method: str = "GET",
        expires: timedelta = timedelta(hours=1),
        response_headers: Optional[Dict[str, str]] = None
    ) -> Optional[str]:
        """
        Generate a presigned URL for an object.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object
            method: HTTP method ('GET', 'PUT', 'POST', 'DELETE')
            expires: Expiration time for the URL
            response_headers: Response headers to include
            
        Returns:
            Presigned URL string, or None if failed
        """
        try:
            url = self.client.presigned_url(
                method,
                bucket_name,
                object_name,
                expires=expires,
                response_headers=response_headers
            )
            return url
        except S3Error as e:
            logging.error(f"Failed to generate presigned URL: {e}")
            return None
    
    def _get_content_type(self, file_path: Path) -> str:
        """
        Auto-detect content type based on file extension.
        
        Args:
            file_path: Path to the file
            
        Returns:
            MIME content type string
        """
        extension = file_path.suffix.lower()
        content_types = {
            '.txt': 'text/plain',
            '.html': 'text/html',
            '.htm': 'text/html',
            '.css': 'text/css',
            '.js': 'application/javascript',
            '.json': 'application/json',
            '.xml': 'application/xml',
            '.csv': 'text/csv',
            '.md': 'text/markdown',
            '.py': 'text/x-python',
            '.java': 'text/x-java-source',
            '.cpp': 'text/x-c++src',
            '.c': 'text/x-csrc',
            '.h': 'text/x-chdr',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.gif': 'image/gif',
            '.pdf': 'application/pdf',
            '.zip': 'application/zip',
            '.tar': 'application/x-tar',
            '.gz': 'application/gzip'
        }
        return content_types.get(extension, 'application/octet-stream')
    
    def close(self):
        """Close the MinIO client connection."""
        if hasattr(self, 'client'):
            self.client = None
        logging.info("MinIO client connection closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Example usage and utility functions
def create_minio_client_from_env() -> Optional[MinioDataLake]:
    """
    Create MinIO client from environment variables.
    
    Returns:
        MinioDataLake instance or None if environment variables are missing
    """
    endpoint = os.getenv('MINIO_ENDPOINT')
    access_key = os.getenv('MINIO_ACCESS_KEY')
    secret_key = os.getenv('MINIO_SECRET_KEY')
    secure = os.getenv('MINIO_SECURE', 'true').lower() == 'true'
    region = os.getenv('MINIO_REGION')
    
    if not all([endpoint, access_key, secret_key]):
        logging.error("Missing required MinIO environment variables")
        return None
    
    try:
        return MinioDataLake(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region
        )
    except Exception as e:
        logging.error(f"Failed to create MinIO client: {e}")
        return None


def upload_json_data(
    client: MinioDataLake,
    bucket_name: str,
    object_name: str,
    data: Dict[str, Any],
    **kwargs
) -> bool:
    """
    Upload JSON data to MinIO.
    
    Args:
        client: MinioDataLake instance
        bucket_name: Target bucket name
        object_name: Target object name
        data: JSON data to upload
        **kwargs: Additional arguments for upload_data
        
    Returns:
        True if upload successful, False otherwise
    """
    json_string = json.dumps(data, indent=2, ensure_ascii=False)
    return client.upload_data(
        bucket_name,
        object_name,
        json_string,
        content_type='application/json',
        **kwargs
    )


def download_json_data(
    client: MinioDataLake,
    bucket_name: str,
    object_name: str
) -> Optional[Dict[str, Any]]:
    """
    Download and parse JSON data from MinIO.
    
    Args:
        client: MinioDataLake instance
        bucket_name: Source bucket name
        object_name: Source object name
        
    Returns:
        Parsed JSON data or None if failed
    """
    data = client.get_object(bucket_name, object_name)
    if data is None:
        return None
    
    try:
        return json.loads(data.decode('utf-8'))
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON data: {e}")
        return None


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # Create client from environment variables
    minio_client = create_minio_client_from_env()
    
    if minio_client:
        try:
            # Example operations
            bucket_name = "test-bucket"
            
            # Create bucket
            minio_client.create_bucket(bucket_name)
            
            # Upload a test file
            test_data = {"message": "Hello MinIO!", "timestamp": datetime.now().isoformat()}
            minio_client.upload_data(bucket_name, "test.json", json.dumps(test_data))
            
            # List objects
            objects = minio_client.list_objects(bucket_name)
            print(f"Objects in bucket: {objects}")
            
            # Download and verify
            downloaded_data = minio_client.get_object(bucket_name, "test.json")
            if downloaded_data:
                print(f"Downloaded data: {downloaded_data.decode('utf-8')}")
            
        except Exception as e:
            logging.error(f"Example execution failed: {e}")
        finally:
            minio_client.close()
    else:
        print("Please set MINIO_ENDPOINT, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY environment variables")
