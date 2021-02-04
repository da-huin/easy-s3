import uuid
import gzip
import time
import random
import os
import json
import datetime
import mimetypes
import boto3

class EasyS3():
    """
    This package helps you use S3 easily. You can use following functions.

    Parameters
    ----------

    * `bucket_name`: str

        Your Bucket Name

    * `service_name`: str

        The service name serves as a detailed classification within the bucket.
        In this situaion, accounts and orders and items are service name.
        1. default/accounts/Your File Path
        2. default/orders/Your File Path
        3. default/items/Your File Path        

    * `region_name`: str

        Your Region Name

    * `aws_access_key_id`: str

        Your AWS ACCESS KEY ID

    * `aws_secret_access_key`: str

        Your AWS SECRET ACCESS KEY
    """

    def __init__(self, bucket_name: str, service_name: str, region_name: str=None, aws_access_key_id: str=None, aws_secret_access_key: str=None):

        self.bucket_name = bucket_name
        self.service_name = service_name
        self.region_name = region_name

        self._s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

        if not os.path.isdir("/tmp/parquet"):
            os.mkdir("/tmp/parquet")


    def save(self, path: str, value, options: dict={}, is_full_path: bool=False):
        """Use this function to save data into S3. 
        
        The saved path is as follows.

        `default/Your Service Name/Y-M-D/Your File Path`

        Parameters
        ----------
        * `(required) path`: str

            ```
            foo/bar/hello.json
            ```

        * `(required) value`: dict | list | str | bytes | int | float | ...

            ```python
            {"hello": "world", "yellow", "banana"}
            ```

        * `options`: dict
            
            Parameters
            
            * `public`: bool (default: False)
                
                if this value is True, anyone can access it.

            * `ymd`: bool (default: False)

                if this value is True, ymd is entered automatically. 

                ```
                default/Your Service Name/20-08-24/Your File Path
                ```

            * `compress_type`: str (default: None)

                currently only gzip is supported.

            ```python
            {
                "public": True,
                "ymd": True,
                "compress_type": "gzip"
            }
            ```
        
        * `is_full_path`: bool

        Returns
        -------

        * URL of saved file : `str`   
        
        Examples
        --------
        data = {"name": "apple", "price": "120"}
        options = {
                "public": True,
                "ymd": False,
                "compress_type": "gzip"
            }
        url = es.save("food/apple.json", data,
            options=options)

        print(url)

        result:

        * If you want check file, check your bucket in [S3 console](https://s3.console.aws.amazon.com/s3/home).

        ```
        https://test-bucket-725.s3.ap-northeast-2.amazonaws.com/default/items/food/apple.json
        ```

     
        """

        public = options.get("public", False)
        ymd = bool(options.get("ymd", False))
        random = options.get("random", False)
        compress_type = options.get("compress_type", None)
        full_path = path if is_full_path else self._get_full_path(path, ymd)
        

        return self._put_file_with_transform(full_path, value, public, random, compress_type=compress_type)

    def load(self, path: str, is_full_path: False):
        """
        Use this function to load data from S3. 

        The loaded path is as follows.

        `default/Your Service Name/Your File Path`

        Parameters
        ----------

        * `(required) path`: str

            ```
            foo/bar/hello.json
            ```

        * `is_full_path`: bool

        Returns
        -------

        * loaded data : `dict` | `list` | `str`

        Examples
        --------

        ```python
        >>> data = es.load("food/apple.json")
        >>> print(data)
        {'name': 'apple', 'price': '120'}
        ```

        """


        full_path = path if is_full_path else self._get_full_path(path, False)

        return self._load_file(full_path)

    def save_cache(self, path: str, value, cache_time: int):
        """

        Use this function to save data that uses the cache.

        Use it when the cost to process is greater than the cost to save and load in S3. 

        The save cache path is as follows.

        ```
        cache/Your Service Name/Your File Path
        ```

        Parameters
        ----------

        * `(required) path`: str

            ```
            foo/bar/hello.json
            ```

        * `(required) value`: dict | list | str | bytes | int | float | ...

            ```python
            {"hello": "world", "yellow", "banana"}
            ```

        * `(required) cache_time`: int
            
            Input the number of seconds you want to cache.

            ```python
            10
            ```

        Returns
        -------

        * URL of saved file : `str`

        Examples
        --------

        with cache_save you can see that file path starts with `cache/...`

        ```python
        >>> url = es.save_cache("food/apple.json", {"name": "apple", "price": "120"}, 10)

        >>> print(url)

        https://test-bucket-725.s3.ap-northeast-2.amazonaws.com/cache/items/food/apple.json
        ```

        If open saved file, You can see that it is saved in the format below. Click [HERE](#load_cache) for more information.

        ```python
        {
            "value": {
                "name": "apple",
                "price": "120"
            },
            "cache_time": 10,
            "put_time": 1596727712.0505128
        }
        ```

        """        
        full_path = self._get_cache_full_path(path)
        data = self._make_cache_file(value, float(cache_time))

        return self._put_file_with_transform(full_path, data, False, False)

    def load_cache(self, path: str):
        """
        Use this function to load cache data from S3.

        Use it when the cost to process is greater than the cost to save and load in S3.

        The loaded path is as follows.

        `cache/Your Service Name/Your File Path`

        Parameters
        ----------

        * `(required) path`: str

            ```
            foo/bar/hello.json
            ```

        Returns
        -------

        * loaded data : `dict` | `list` | `str` | `None`  

        Examples
        --------

        ```python

        import time
        import random

        while True:
            print("\n=== Press any key to get started. ===")
            input()
            path = "food/apple.json"
            data = es.load_cache(path)

            if data == None:
                working_time = random.randint(0, 4)

                print(f"working for {working_time} seconds ...")

                time.sleep(working_time)

                data = {"name": "apple", "price": "120"}
                es.save_cache(path, data, cache_time=5)

                print("working complete!")
            else:
                print("cached!")

            print(data)
        ```

        ```bash
        === Press any key to get started. ===

        working for 2 seconds ...
        working complete!
        {'name': 'apple', 'price': '120'}

        === Press any key to get started. ===

        cached!
        {'name': 'apple', 'price': '120'}

        === Press any key to get started. ===

        cached!
        {'name': 'apple', 'price': '120'}

        === Press any key to get started. ===

        working for 1 seconds ...
        working complete!
        {'name': 'apple', 'price': '120'}

        ```
      
        """
        full_path = self._get_cache_full_path(path)

        try:
            data = self._load_file(full_path)
        except:
            return None

        if self._is_expired(data):
            return None

        return data["value"]

    # LIST
    def list_objects(self, path: str, load: bool=False):
        """

        Use this function to list directory names.

        The list path is as follows.

        `default/Your Service Name/Your Directory Path`

        Parameters
        ----------

        * `(required) path`: str

            ```
            foo/bar/
            ```

        Returns
        -------

        list of directory names: `list`   

        Examples
        --------

        ```python
        >>> print(es.listdir("/"))

        ['default/items/2020-08-06/food', 'default/items/2020-08-07/food', 'default/items/food']
        ```
     
        """        
        result = []
        full_path = self._get_full_path(
            path, kind="dir")
        lists = self._get_all_s3_objects(
            Delimiter=full_path, Prefix=full_path)
        for item in lists:
            if item["Size"] == 0:
                continue
            result.append(item["Key"])

        if load:
            new_result = []
            for full_path in result:
                new_result.append({
                    "key": full_path,
                    "data": self._load_file(full_path)
                })
            result = new_result

        return result

    def listdir(self, path: str):
        """
        Use this function to list directory names.

        The list path is as follows.

        ```
        default/Your Service Name/Your Directory Path
        ```

        **Parameters**

        * `(required) path`: str

            ```
            foo/bar/
            ```

        **Returns**

        list of directory names: `list`

        **Examples**

        ```python
        >>> print(es.listdir("/"))

        ['default/items/2020-08-06/food', 'default/items/2020-08-07/food', 'default/items/food']
        ```        
        """        
        return list(set([os.path.dirname(fullpath) for fullpath in self.list_objects(path)]))


    def _get_random_string(self, length: int=10):
        random_box = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
        random_box_length = len(random_box)
        result = ""
        for _ in range(length):
            result += random_box[int(random.random()*random_box_length)]

        return result
        
    def _make_cache_file(self, value, cache_time: int):
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
        """
        If the extention is .parquet, you need the Fastparquet package.
        """
        readed = self._s3_client.get_object(
            Bucket=self.bucket_name, Key=full_path)["Body"].read()

        _, ext = os.path.splitext(full_path)

        if ext == ".gz":
            readed = gzip.decompress(readed)
            _, ext = os.path.splitext(os.path.splitext(full_path))


        if ext == ".parquet":
            from fastparquet import ParquetFile

            filename = f"/tmp/parquet/{uuid.uuid1()}.parquet"
            with open(filename, "wb") as fp:
                fp.write(readed)

            readed = ParquetFile(filename).to_pandas()

            os.unlink(filename)
        else:
            try:
                encoded = readed.decode("utf-8")
                try:
                    readed = json.loads(encoded)
                except:
                    readed = encoded
            except:
                pass

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
        import pandas as pd
        from fastparquet import write

        filename = f"/tmp/parquet/{uuid.uuid1()}.parquet"

        df = pd.DataFrame(data, index=range(len(data)))
        # df.to_parquet(filename, index=False, compression="GZIP")

        write(filename, df, compression="GZIP")

        with open(filename, "rb") as fp:
            parquet = fp.read()

        os.unlink(filename)

        return parquet

    def _put_file_with_transform(self, full_path, value, public, random, binary_content_type=False, compress_type=None):
        _, ext = os.path.splitext(full_path)

        if ext == ".parquet":
            if isinstance(value, dict):
                value = [value]

            if not isinstance(value, list):
                raise ValueError(f"parquet value's instance must be list. value type is {type(value)}")
            
            value = self._make_parquet(value)

        else:
            return self._put_file(full_path, value, public, random, binary_content_type=binary_content_type, compress_type=compress_type)




