"""
This adds the function writestr_par which can be used to write files to a zip file in parallel
It works by changing writestr to only lock after the compression takes place
And also hacks the compressor to make this work

Uage Example:
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []

        for img in enumerate(images):
            fname = f'images/{idx}.raw'
            futures.append(executor.submit(file.writestr, fname ,img.tobytes()))

        concurrent.futures.wait(futures)

"""

import zipfile, time

class EmptyCompressor(object):
    def flush(self):
        return bytes(0)

class ZipFileParallel(zipfile.ZipFile):
    def writestr(self, zinfo_or_arcname, data,
                 compress_type=None, compresslevel=None):
        """Write a file into the archive.  The contents is 'data', which
        may be either a 'str' or a 'bytes' instance; if it is a 'str',
        it is encoded as UTF-8 first.
        'zinfo_or_arcname' is either a ZipInfo instance or
        the name of the file in the archive."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        if not isinstance(zinfo_or_arcname, zipfile.ZipInfo):
            zinfo = zipfile.ZipInfo(filename=zinfo_or_arcname,
                            date_time=time.localtime(time.time())[:6])
            zinfo.compress_type = self.compression
            zinfo._compresslevel = self.compresslevel
            if zinfo.filename[-1] == '/':
                zinfo.external_attr = 0o40775 << 16  # drwxrwxr-x
                zinfo.external_attr |= 0x10  # MS-DOS directory flag
            else:
                zinfo.external_attr = 0o600 << 16  # ?rw-------
        else:
            zinfo = zinfo_or_arcname

        if not self.fp:
            raise ValueError(
                "Attempt to write to ZIP archive that was already closed")

        if compress_type is not None:
            zinfo.compress_type = compress_type

        if compresslevel is not None:
            zinfo._compresslevel = compresslevel

        zinfo.file_size = len(data)  # Uncompressed size
        crc = zipfile.crc32(data, 0)
        # compress data
        compressor = zipfile._get_compressor(zinfo.compress_type, zinfo._compresslevel)
        data = compressor.compress(data)
        data += compressor.flush()

        with self._lock:
            with self.open(zinfo, mode='w') as dest:
                dest._compressor = None # remove the compressor so it doesn't compress again
                dest.write(data)
                dest._crc = crc
                dest._file_size = zinfo.file_size
                dest._compress_size = len(data)
                dest._compressor = EmptyCompressor() # use an empty compressor

