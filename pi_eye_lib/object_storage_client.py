
import datetime
import io
import logging
import os
import tempfile

import numpy as np
from minio import Minio

log = logging.getLogger('OSClient')


def split_dirs(dir):
    tail, head = os.path.split(dir)
    if tail != "":
        return split_dirs(tail) + [head]
    return [head]


class ObjectStorageClient:
    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def storeArray(self, name, array, bucket=None):
        pass

    def store(self, filepath, bucket=None):
        location = [filepath, bucket]
        return location

    def load(self, location):
        filepath, _ = location
        return filepath

    def cleanup_buckets(self):
        pass


class MinioClient(ObjectStorageClient):
    def __init__(self):
        ObjectStorageClient.__init__(self)
        self.buckets = {
            os.getenv("MINIO_BUCKET_RAW", "raw_images"): os.getenv("MINIO_BUCKET_RAW_CLEANUP", 3600),
            os.getenv("MINIO_BUCKET_PROCESSED", "processed_images"): os.getenv("MINIO_BUCKET_PROCESSED_CLEANUP", 3600),
            os.getenv("MINIO_BUCKET_TEMP", "temp"): os.getenv("MINIO_BUCKET_TEMP_CLEANUP", 24 * 3600)
        }

        minio_secure = True if os.getenv("MINIO_SECURE", "") == "true" else False
        self.minio = Minio(os.getenv("MINIO_HOST", "minio:9000"),
                           access_key=os.getenv("MINIO_ACCESS_KEY", "minio"),
                           secret_key=os.getenv("MINIO_SECRET_KEY", "minio123"),
                           secure=minio_secure)

        for bucket in self.buckets:
            if not self.minio.bucket_exists(bucket):
                self.minio.make_bucket(bucket)

        # self.tmp_dir = tempfile.TemporaryDirectory(dir="/tmp/minio")

    # def __enter__(self):
    #     return self

    # def __exit__(self, exc_type, exc_value, traceback):
    #     self.tmp_dir.cleanup()
    #     self.tmp_dir = None

    def storyArray(self, name, array, bucket=os.getenv("MINIO_BUCKET_TEMP", "temp")):
        if bucket not in self.buckets:
            raise Exception("Invalid bucket name: {}".format(bucket))
        if not isinstance(array, np.ndarray):
            raise Exception("Arrays must be type numpy.ndarray, not {}".format(type(array)))

        buf = array.tobytes()
        self.minio.put_object(bucket, name, io.BytesIO(buf), len(buf))

        log.debug("Put array '{}' into bucket {}".format(name, bucket))

    # def get_tmp_dir(self):
    #     return self.tmp_dir.name

    # def get_key_from_path(self, path):
    #     return "-".join(split_dirs(path))

    # def store(self, filepath, bucket=os.getenv("MINIO_BUCKET_TEMP", "celerytemp")):
    #     if bucket not in self.buckets:
    #         raise Exception("Invalid bucket name %s" % bucket)
    #     if not os.path.exists(filepath):
    #         raise Exception("Cannot store %s (does not exists)" % (filepath))

    #     tmp_dir = self.get_tmp_dir()
    #     relpath = os.path.relpath(filepath, tmp_dir)
    #     if relpath.startswith(".."):
    #         log.warning("file \'%s\' not in tmpdir \'%s\'. will not be cleaned up" % (filepath, tmp_dir))

    #     relpath = relpath.replace("..", "__")
    #     key = self.get_key_from_path(relpath)

    #     etag = self.minio.fput_object(bucket, key, filepath)
    #     log.info("Stored file %s with etag=%s to %s" % (filepath, etag, bucket))

    #     osc_id = [bucket, relpath]
    #     return osc_id

    # def load(self, osc_id):
    #     bucket, relpath = osc_id
    #     filepath = os.path.join(self.get_tmp_dir(), relpath)

    #     if os.path.exists(filepath):
    #         log.warn("Not loading %s (already exists)" % (filepath))
    #     else:
    #         key = self.get_key_from_path(relpath)
    #         log.info("Loading %s" % (filepath))
    #         self.minio.fget_object(bucket, key, filepath)
    #     return filepath

    def cleanup_buckets(self):
        for bucket in self.buckets:
            minutes = self.buckets[bucket] / 60
            log.info(f"Cleanup files in bucket {bucket} older than %s" % (datetime.datetime.now() - datetime.timedelta(minutes=minutes)))
            # minio-py does not provide a convenient api to delete older files, thus using the mc client for now
            os.system(f"mc rm --recursive --force --older-than {minutes}m minio/{bucket}/")
