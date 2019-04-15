import io
import gzip
import zlib
import os
import sys

class split_gzip(object):

    def __init__(self,in_byte_stream,out_dir,split_byte_size,chunk_size,*args):

        self._in_io         = in_byte_stream
        self._out_dir       = out_dir
        self._split_size    = split_byte_size
        self._chunk_size    = chunk_size
        if not self._in_io.seekable() \
            and not args:
            raise SyntaxError("Must provide content size when io not seekable")
        elif args and not isinstance(args[0],int):
            raise SyntaxError("Integer should be supplied for content size")
        elif self._in_io.seekable():
            self._in_io_size = self._io_size()
        else:
            self._in_io_size = args[0]
        self._buffer  = io.BytesIO()

    def __iter__(self):
        self._d = zlib.decompressobj(16)
        self._part = 0
        return self

    def __next__(self):
        if self._in_io.tell() ==  self._in_io_size:
            raise StopIteration

        self._part+=1
        out_io = io.BytesIO()
        gz = gzip.GzipFile(fileobj=out_io,mode='wb')

        # If content in buffer write at beginning of out stream
        if self._buffer.tell()!=0:
            gz.write(self._buffer.read())
            self._buffer  = io.BytesIO()

        if (self._in_io_size - self._in_io.tell()) >= self._split_size:
            # Process all but last chunk
            bytes_written=0
            while bytes_written <= self._split_size - self._chunk_size:
                chunk = self._in_io.read(self._chunk_size)
                gz.write(self._d.decompress(chunk))
                bytes_written+=self._chunk_size
            # Handle last chunk
            chunk = self._in_io.read(self._chunk_size)
            chunk = self._d.decompress(chunk).decode('utf-8')
            chunk = chunk.split("\n")
            last_record = chunk.pop()
            chunk = '\n'.join(chunk)
            gz.write(chunk.encode('utf-8'))
            self._buffer.write(last_record.encode('utf-8'))
        else:
            while self._in_io.tell() < self._in_io_size:
                chunk = self._in_io.read(self._chunk_size)
                gz.write(self._d.decompress(chunk))

        if chunk:
            chunk = None

        gz.close()

        out_io.seek(0)

        out_path = os.path.join(self._out_dir,"split_"+str(self._part)+".gz")

        with open(out_path,'wb') as f:
            f.write(out_io.read())

        out_io.close()
        
        return out_path

    def _io_size(self):  
        size = self._in_io.seek(0,2)
        self._in_io.seek(0)
        return size

sp = split_gzip(open('data.gz','rb'),r'c:\users\danie\desktop',20000000,int(sys.argv[1]))
for s in sp:
    print(s)