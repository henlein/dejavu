import argparse
import json
import sys
from argparse import RawTextHelpFormatter
from os.path import isdir

from dejavu import Dejavu
from dejavu.logic.recognizer.file_recognizer import FileRecognizer
from dejavu.logic.recognizer.microphone_recognizer import MicrophoneRecognizer

#DEFAULT_CONFIG_FILE = "dejavu.cnf.SAMPLE"

config = {
    "database": {
        "host": "127.0.0.1",
        #"user": "xyz",
        #"password": "abc",
        #"database": "dejavu",
    },
    "database_type": "mongodb",
}
djv = Dejavu(config)

print("Process")
#djv.fingerprint_directory("/home/alexander/Downloads/scene_det/", [".mkv"], 1)
djv.fingerprint_file("/home/alexander/Downloads/clips/redbull.mp4")
print("==========================================")
print(djv.db.get_num_fingerprints())

#song = djv.recognize(FileRecognizer, "/home/alexander/Downloads/redbull.mp4")
#print(song)