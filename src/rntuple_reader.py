# ruff: noqa: T201
from __future__ import annotations

from pathlib import Path

from rootfilespec.bootstrap import ROOTFile, TStreamerInfo
from rootfilespec.structutil import ReadBuffer

if __name__ == "__main__":
    initial_read_size = 512
    # path = Path("../TTToSemiLeptonic_UL18JMENanoAOD-zstd.root")
    path = Path("RNTuple.root")
    print(f"Reading {path}")
    with path.open("rb") as filehandle:

        def fetch_data(seek: int, size: int):
            """ Fetches data from a file at a specified position and size.

            Args:
                seek (int): The position in the file to start reading from.
                size (int): The number of bytes to read from the file.

            Returns:
                ReadBuffer: A buffer containing the read data, along with the seek position and an offset of 0.
            """
            print(f"\033[3;33mfetch_data {seek=} {size=}\033[0m")
            filehandle.seek(seek)
            return ReadBuffer(memoryview(filehandle.read(size)), seek, 0)

        # Get TFile Header
        buffer = fetch_data(0, initial_read_size)
        file, _ = ROOTFile.read(buffer)
        print(f"\t{file}\n")

        def fetch_cached(seek: int, size: int):
            print(f"\033[3;33mfetch_cached {seek=} {size=}\033[0m")
            if seek + size <= buffer.__len__():
                return buffer[seek : seek + size]
            print("Didn't find data in initial read buffer, fetching from file")
            return fetch_data(seek, size)

        # Get TFile object (root TDirectory)
        tfile = file.get_TFile(fetch_cached)
        print(f"\t{tfile}\n")

        # usually the directory tkeylist and the streamer info are adjacent at the end of the file
        keylist_start = tfile.rootdir.fSeekKeys
        keylist_stop = keylist_start + tfile.rootdir.header.fNbytesKeys
        print(f"KeyList at {keylist_start}:{keylist_stop}")
        streaminfo_start = file.header.fSeekInfo
        streaminfo_stop = streaminfo_start + file.header.fNbytesInfo
        print(f"StreamerInfo at {streaminfo_start}:{streaminfo_stop}")
        print(f"End of file at {file.header.fEND}")

        # Get TKeyList (List of all TKeys in the TDirectory)
        # abbott pick up here and figure out why it fails during TKeyList
        keylist = tfile.get_KeyList(fetch_data)
        for name, key in keylist.items():
            print(f"{name} ({key.fClassName.fString})")

        streamerinfo = file.get_StreamerInfo(fetch_data)

        # tree = keylist["Events"].read_object(fetch_data, )
        # print(tree)

    for item in streamerinfo.items:
        if isinstance(item, TStreamerInfo):
            print(item.b_named.fName.fString)
            for obj in item.fObjects.objects:
                print(f"  {obj.b_element.b_named.fName.fString}")