import gzip
import time
import random
import os
import json
import requests
import datetime
import mimetypes
import boto3

class EasyS3():

    def __init__(self, bucket_name, service_name, region_name=None, aws_access_key_id=None, aws_secret_access_key=None):
        """
        S3를 파일을 핸들링하는 것처럼 쉽게 사용 할 수 있게 해주는 모듈입니다. 파일 리스트 읽기, 파일 쓰기, 파일 읽기 등의 기능이 포함되어 있습니다.
        """

        self.bucket_name = bucket_name
        self.service_name = service_name
        self.region_name = region_name

        self._s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

    def make_bucket(self, bucket_name):
        """
        버킷을 만들어주는 함수입니다.
        """
        buckets = [bucket["Name"]
                   for bucket in self._s3_client.list_buckets()["Buckets"]]

        if not bucket_name in buckets:
            self._s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    "LocationConstraint": self.region_name
                }
            )

    # SAVE
    def save(self, path, value, options={}):
        """
            options
                public (Boolean, default: False)
                ymd (Boolean, default: False)
                compress_type:
                    gzip
        """

        public = options.get("public", False)
        ymd = bool(options.get("ymd", False))
        random = options.get("random", False)
        compress_type = options.get("compress_type", None)
        full_path = self._get_full_path(path, ymd, self.service_name)

        return self._data_transform_put_file(full_path, value, public, random, compress_type=compress_type)

    def save_by_full_path(self, full_path, value, options={}):
        public = options.get("public", False)
        random = options.get("random", False)
        compress_type = options.get("compress_type", None)

        return self._data_transform_put_file(full_path, value, public, random, compress_type=compress_type)

    def save_cache(self, path, value, cache_time):
        full_path = self._get_cache_full_path(path)
        data = self._make_cache_file(value, float(cache_time))

        return self._data_transform_put_file(full_path, data, False, False)

    def save_uri(self, path, uri, options={}):
        """
            options
                public (Boolean, default: False)
                ymd (Boolean, default: False)
                save_always (Boolean, default: True)
        """
        save_always = options.get("save_always", True)

        try:
            resp = requests.get(uri)
            if resp.status_code != 200:
                raise ValueError("status code is %s." % (resp.status_code))

        except Exception as e:
            if not save_always:
                raise Exception(e)
            fb = str(e).encode()
        else:
            fb = resp.text.encode()

        return self.save(path, fb, options)

    def load_by_full_path(self, full_path):
        return self._load_file(full_path)

    # LOAD
    def load(self, path):
        full_path = self._get_full_path(path, False, self.service_name)
        print(full_path)
        return self._load_file(full_path)

    def load_cache(self, name):
        full_path = self._get_cache_full_path(name, self.service_name)

        # 파일이 존재하는지 확인
        try:
            data = self._load_file(full_path)
        except:
            return None

        # 만료되었으면 None 을 반환한다.
        if self._is_expired(data):
            return None

        return data["value"]

    # LIST
    def list_objects(self, path, load=False):
        result = []
        full_path = self._get_full_path(
            path, service_name=self.service_name, kind="dir")
        print("listing .. ", full_path)
        lists = self._get_all_s3_objects(
            Delimiter=full_path, Prefix=full_path)
        for item in lists:
            if item["Size"] == 0:
                continue
            result.append(item)

        if load:
            new_result = []
            for full_path in result:
                new_result.append({
                    "key": full_path,
                    "data": self.load_by_full_path(full_path)
                })
            result = new_result

        return result

    def list_directory_names(self, path):
        temp = {}
        full_path = self._get_full_path(path, service_name=self.service_name, kind="dir")
        for p in self.list_objects(path, service_name=self.service_name):
            dirname = os.path.dirname(p["Key"])
            temp[dirname] = dirname[len(full_path) + 1:]

            
        result = []
        for key in temp:
            short_key = temp[key]
            result.append({
                "key": key,
                "short_key": short_key
            })

        return result


    def _get_random_string(self, length=10):
        random_box = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
        random_box_length = len(random_box)
        result = ""
        for _ in range(length):
            result += random_box[int(random.random()*random_box_length)]

        return result
        
    def _make_cache_file(self, value, cache_time: float):
        return {
            "value": value,
            "cache_time": cache_time,
            "put_time": time.time()
        }

    def _is_expired(self, data):
        cache_time = data["cache_time"]
        put_time = data["put_time"]
        if cache_time == -1:
            return False

        if (time.time() - put_time) > cache_time:
            return True

        return False

    def _get_all_s3_objects(self, **base_kwargs):
        continuation_token = None
        base_kwargs["Bucket"] = self.bucket_name
        while True:
            list_kwargs = dict(MaxKeys=1000, **base_kwargs)
            if continuation_token:
                list_kwargs['ContinuationToken'] = continuation_token
            response = self._s3_client.list_objects_v2(**list_kwargs)
            yield from response.get('Contents', [])
            if not response.get('IsTruncated'):  # At the end of the list?
                break
            continuation_token = response.get('NextContinuationToken')



    def _put_file(self, full_path, data, public, random, binary_content_type=False, compress_type=None):

        if random:
            _, ext = os.path.splitext(full_path)
            dirname = os.path.dirname(full_path)
            filename = self._get_random_string() + ext
            full_path = dirname + "/" + filename

        if full_path == "":
            raise ValueError("full path is empty.")

        if public:
            ACL = "public-read"
        else:
            ACL = "private"
        binary = self._to_binary(data)
        content_type = ""
        if binary_content_type == False:
            content_type, _ = mimetypes.guess_type(full_path)
            if content_type == None:
                content_type = "binary/octet-stream"

        
        if compress_type == "gz":
            binary = gzip.compress(binary)

        self._s3_client.put_object(Bucket=self.bucket_name,
                            Body=binary, Key=full_path, ACL=ACL, ContentType=content_type)

        object_uri = f"https://{self.bucket_name}.s3.{self.region_name}.amazonaws.com/{full_path}"

        return object_uri


    def _load_file(self, full_path):

        readed = self._s3_client.get_object(
            Bucket=self.bucket_name, Key=full_path)["Body"].read()

        _, ext = os.path.splitext(full_path)
        if ext == ".gz":
            readed = gzip.decompress(readed)

        try:
            encoded = readed.decode("utf-8")
            try:
                return json.loads(encoded)
            except:
                return encoded
        except:
            return readed

    def _to_binary(self, value):
        binary = b""
        if isinstance(value, bytes):
            binary = value
        elif isinstance(value, str):
            binary = value.encode("utf-8")
        else:
            binary = json.dumps(value, ensure_ascii=False,
                                default=str).encode("utf-8")

        return binary


    def _make_valid_path(self, path):
        if not isinstance(path, str):
            raise ValueError(f"path is not str. path type is {type(path)}")
        if len(path) == 0:
            raise ValueError("path's length is zero.")

        path = path.replace("\\", "/")
        path = path.replace("//", "/")

        if path == "/":
            raise ValueError("invalid path '/'")

        if path[0] == "/":
            path = path[1:]

        return path

    def _get_full_path(self, path, ymd=False, kind="file"):

        ymd_str = ""
        if ymd:
            ymd_str = "%s/" % datetime.datetime.now().strftime("%Y-%m-%d")

        if kind == "file":
            if len(path) == 0:
                raise ValueError("path's length is zero.")

            if path == "/":
                raise ValueError("path is slash.")

            if path[0] == "/":
                path = path[1:]

        elif kind == "dir":
            pass

        return self._make_valid_path(f"default/{self.service_name}/{ymd_str}{path}")

    def _get_cache_full_path(self, path):

        return self._make_valid_path(f"cache/{self.service_name}/{path}")

    def _make_parquet(self, data):
        if not os.path.isdir("/tmp/parquet"):
            os.mkdir("/tmp/parquet")
        filename = f"/tmp/parquet/{uuid.uuid1()}.parquet"

        df = pd.DataFrame(data, index=range(len(data)))
        # df.to_parquet(filename, index=False, compression="GZIP")

        fpwrite(filename, df, compression="GZIP")

        with open(filename, "rb") as fp:
            parquet = fp.read()

        os.unlink(filename)

        return parquet


    def _data_transform_put_file(self, full_path, value, public, random, binary_content_type=False, compress_type=None):
        _, ext = os.path.splitext(full_path)

        if ext == ".parquet":
            if isinstance(value, dict):
                value = [value]

            if not isinstance(value, list):
                raise ValueError(f"parquet value's instance must be list. value type is {type(value)}")
            
            value = self._make_parquet(value)

        else:
            return self._put_file(full_path, value, public, random, binary_content_type=binary_content_type, compress_type=compress_type)

