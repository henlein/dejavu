import time
from functools import wraps

from dejavu import Dejavu
from dejavu.logic.recognizer.clip_recognizer import ClipRecognizer
from dejavu.config import settings

# SETTINGS!!!
# dejavu.config.settings.DEFAULT_FAN_VALUE = 15 !
# dejavu.config.settings.DATABASES += 'mongodb': ("dejavu.database_handler.mongo_database", "MongoDatabase")

# TODO!!!
# - 3 Functions -> Save Clips, Save Videos, Compare
# - Save Hashed Clips in MongoDB
# - Compare Videos with Clips in MongoDB (List with Clips names as parameter)
# - Delete Videos from Mongo or even don't save at all.

def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'Function {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper


@timeit
def prepare_files():
    djv.fingerprint_file("/home/alexander/Downloads/rblzgaming_40318149080_1673450735.033_0.mp3")
    djv.fingerprint_file("/home/alexander/Downloads/rblzgaming_40318149080_1673450735.033_1.mp3")
    djv.fingerprint_file("/home/alexander/Downloads/rblzgaming_40318149080_1673450735.033_2.mp3")
    print(djv.db.get_num_fingerprints())


@timeit
def find_clip():
    song = djv.recognize(ClipRecognizer, "/home/alexander/Downloads/redbull.mp3")
    print(song)


config = {
    "database": {
        "host": "127.0.0.1",
        "port": 27117,
        "user": "xyz",
        "password": "abc",
        "database": "dejavu",
    },
    "database_type": "mongodb",
}

djv = Dejavu(config)

print("Process")
prepare_files()
find_clip()



