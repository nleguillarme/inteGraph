from pathlib import Path


def ensure_path(path):
    if isinstance(path, str):
        return Path(path)
    else:
        return path


# def cached_path(url_or_filename, cache_dir=None):
#     """
#     Given something that might be a URL (or might be a local path),
#     determine which. If it's a URL, download the file and cache it, and
#     return the path to the cached file. If it's already a local path,
#     make sure the file exists and then return the path.
#     """
#     # if cache_dir is None:
#     #     cache_dir = DATASET_CACHE

#     parsed = urlparse(url_or_filename)

#     if parsed.scheme in ("http", "https"):
#         # URL, so get it from the cache (downloading if necessary)
#         return get_from_cache(url_or_filename, cache_dir=cache_dir)
#     elif os.path.exists(url_or_filename):
#         # File, and it exists.
#         return url_or_filename
#     elif parsed.scheme == "":
#         # File, but it doesn't exist.
#         raise FileNotFoundError("file {} not found".format(url_or_filename))
#     else:
#         # Something unknown
#         raise ValueError(
#             "unable to parse {} as a URL or as a local path".format(url_or_filename)
#         )


# def url_to_filename(url, etag=None):
#     """
#     Convert `url` into a hashed filename in a repeatable way.
#     If `etag` is specified, append its hash to the url's, delimited
#     by a period.
#     """

#     last_part = url.split("/")[-1]
#     url_bytes = url.encode("utf-8")
#     url_hash = sha256(url_bytes)
#     filename = url_hash.hexdigest()

#     if etag:
#         etag_bytes = etag.encode("utf-8")
#         etag_hash = sha256(etag_bytes)
#         filename += "." + etag_hash.hexdigest()

#     filename += "." + last_part
#     return filename


# # def filename_to_url(filename: str, cache_dir: str = None) -> Tuple[str, str]:
# #     """
# #     Return the url and etag (which may be ``None``) stored for `filename`.
# #     Raise ``FileNotFoundError`` if `filename` or its stored metadata do not exist.
# #     """
# #     if cache_dir is None:
# #         cache_dir = DATASET_CACHE
# #
# #     cache_path = os.path.join(cache_dir, filename)
# #     if not os.path.exists(cache_path):
# #         raise FileNotFoundError("file {} not found".format(cache_path))
# #
# #     meta_path = cache_path + ".json"
# #     if not os.path.exists(meta_path):
# #         raise FileNotFoundError("file {} not found".format(meta_path))
# #
# #     with open(meta_path) as meta_file:
# #         metadata = json.load(meta_file)
# #     url = metadata["url"]
# #     etag = metadata["etag"]
# #
# #     return url, etag


# def http_get(url, temp_file):
#     req = requests.get(url, stream=True)
#     for chunk in req.iter_content(chunk_size=1024):
#         if chunk:  # filter out keep-alive new chunks
#             temp_file.write(chunk)


# def get_from_cache(url, name=None, cache_dir=None):
#     """
#     Given a URL, look for the corresponding dataset in the local cache.
#     If it's not there, download it. Then return the path to the cached file.
#     """

#     os.makedirs(cache_dir, exist_ok=True)

#     response = requests.head(url, allow_redirects=True)
#     if response.status_code != 200:
#         raise IOError(
#             "HEAD request failed for url {} with status code {}".format(
#                 url, response.status_code
#             )
#         )
#     etag = response.headers.get("ETag")

#     url_for_filename = url if not name else url + f"/{name}"
#     filename = url_to_filename(url_for_filename, etag)

#     # get cache path to put the file
#     cache_path = os.path.join(cache_dir, filename)

#     if not os.path.exists(cache_path):
#         # Download to temporary file, then copy to cache dir once finished.
#         # Otherwise you get corrupt cache entries if the download gets interrupted.
#         with tempfile.NamedTemporaryFile() as temp_file:  # type: IO
#             logger.info(
#                 f"{url_for_filename} not found in cache, downloading to {temp_file.name}"
#             )

#             # GET file object
#             http_get(url, temp_file)

#             # we are copying the file before closing it, so flush to avoid truncation
#             temp_file.flush()
#             # shutil.copyfileobj() starts at the current position, so go to the start
#             temp_file.seek(0)

#             logger.info(
#                 f"Finished download, copying {temp_file.name} to cache at {cache_path}"
#             )
#             with open(cache_path, "wb") as cache_file:
#                 shutil.copyfileobj(temp_file, cache_file)

#             meta = {"url": url, "etag": etag}
#             meta_path = cache_path + ".json"
#             with open(meta_path, "w") as meta_file:
#                 json.dump(meta, meta_file)

#     return cache_path


# def clean_dir(dir):
#     if os.path.exists(dir):
#         shutil.rmtree(dir)
#     os.makedirs(dir)


# def move_file_to_dir(file, dir):
#     os.makedirs(dir, exist_ok=True)
#     os.replace(file, os.path.join(dir, os.path.basename(file)))


# def copy_file_to_dir(file, dir):
#     os.makedirs(dir, exist_ok=True)
#     shutil.copy(file, dir)


# def get_basename_and_extension(file_path):
#     basename = os.path.splitext(os.path.basename(file_path))
#     return basename[0], basename[1]


# # def pdf_2_text(url):
# #     data = parser.from_file(url)
# #     return data["content"]


# def split_file(f_in, prefix, suffix, max_size=1e6):
#     size_in_kb = os.path.getsize(f_in) / 1000.0
#     nb = ceil(1.0 * size_in_kb / max_size)
#     subprocess.call(
#         [
#             "split",
#             "-n",
#             "l/" + str(nb),
#             "-a",
#             str(len(str(nb))),
#             "-d",
#             "--additional-suffix={}".format(suffix),
#             f_in,
#             prefix,
#         ]
#     )


# def decompress_file(f_in, delete_archive=False):
#     f_out = None
#     if tarfile.is_tarfile(f_in):
#         f_out = decompress_tar(f_in)
#     elif f_in.endswith(".gz"):
#         f_out = decompress_gz(f_in)
#     else:
#         extension = pathlib.Path(f_in).suffixes
#         raise NotImplementedError(
#             "Cannot decompress files with extension {}".format(extension)
#         )
#     if delete_archive:
#         delete_file(f_in)
#     return f_out


# def decompress_tar(f_in):
#     try:
#         print(f_in)
#         if f_in.endswith("tar.gz"):
#             print("tar.gz")
#             tar = tarfile.open(f_in, "r:gz")
#         elif f_in.endswith("tar"):
#             print("tar")
#             tar = tarfile.open(f_in, "r:")
#         f_out = tar.getnames()
#         tar.extractall()
#         tar.close()
#         return f_out
#     except tarfile.ReadError as e:
#         print(e)
#         return None


# def decompress_gz(f_in):
#     basename = os.path.basename(f_in)
#     dirname = os.path.dirname(f_in)
#     f_out = os.path.join(dirname, os.path.splitext(basename)[0])
#     with open(f_out, "wb") as fo, gzip.open(f_in, "rb") as fi:
#         shutil.copyfileobj(fi, fo, length=64 * 2**20)
#         fi.close()
#         fo.close()
#         return f_out


# def delete_file(filepath):
#     try:
#         os.remove(filepath)
#     except:
#         print("Error while deleting file ", filepath)
