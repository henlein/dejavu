import os
import time
from functools import wraps
from itertools import groupby
from dejavu.base_classes.base_database import get_database
import dejavu.logic.fingerprint as djv_fingerprint
import numpy as np
import multiprocessing
import traceback
import sys
import pydub
from timeFunc import get_seconds, get_time_string
from hashlib import sha1



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


class ComDet(object):
    AD_ID = "song_id"
    AD_NAME = 'song_name'
    AD_DURATION = 'duration'
    CONFIDENCE = 'confidence'
    MATCH_TIME = 'match_time'
    OFFSET = 'offset'
    OFFSET_SECS = 'offset_seconds'
    DBFIELD_DURATION = 'duration'
    DFFIELD_FILE_SHA1 = 'file_sha1'

    def __init__(self, config={}):

        config.setdefault('database', {
            "host": "127.0.0.1",
            #"user": "root",
            #"passwd": "pass",
            "db": "dejavu"
        })
        config.setdefault('database_type', 'mongodb')
        config.setdefault('analyze_span', 3)
        config.setdefault('analyze_skip', 1)
        config.setdefault('confidence_thresh', 10)

        self.config = config
        # initialize db
        db_cls = get_database(config.get("database_type", 'None'))

        self.db = db_cls(**config.get("database", {}))
        self.db.setup()

    def get_fingerprinted_ads(self):
        # get ads previously indexed
        self.ads = self.db.get_songs()
        self.adhashes_set = set()  # to know which ones we've computed before
        for ad in self.ads:
            ad_hash = ad[self.DFFIELD_FILE_SHA1]
            self.adhashes_set.add(ad_hash)


    def _get_nprocess(self):
        # Try to use the maximum amount of processes if not given.
        """
        try:
            nprocesses = nprocesses or multiprocessing.cpu_count()
        except NotImplementedError:
            nprocesses = 1
        """
        nprocesses = 1 # if nprocesses <= 0 else nprocesses
        return nprocesses


    def fingerprint_clip(self, input_file_path):

        ext = input_file_path.split('.')[-1]
        audio = pydub.AudioSegment.from_file(input_file_path, ext)
        audio = audio.set_channels(1)

        self.get_fingerprinted_ads()

        ad_name, hashes, ad_hash, duration = _fingerprint_worker(("redbull", audio))

        aid = self.db.insert_song(ad_name, ad_hash, duration)

        self.db.insert_hashes(aid, hashes)
        self.db.set_song_fingerprinted(aid)
        self.get_fingerprinted_ads()


    def fingerprint_file(self, input_file_path, input_labels, nprocesses=None):

        """
            Function: (Part of code majorly borrows from Dejavu)
                Creates fingerprints for input_file with names from labels_file
                Given any input_file_path uses pydub to just read the file
                Supports all formats supported by ffmpeg
            Set a parallel pool of workers where each worker creates a fingerprint for the ad
            specified in the labels file.
            The entire ad is stored in the database as SHA hash which is used to check for duplicate
            entries of the same ad by content.
            Duplicate ads are ignored. Duplicate here refers to content and not the name of the ad
            Inputs:
                input_file_path: Either video or audio file
                input_labels: 2D list of ads where each element of list is
                        [start_time, end_time, ad_name] for that ad.
                        start_time, end_time is in seconds from the start of the audio
        """
        pool = multiprocessing.Pool(nprocesses)

        ads_to_fingerprint = []
        audios_to_fingerprint = []

        ext = input_file_path.split('.')[-1]
        audio = pydub.AudioSegment.from_file(input_file_path, ext)
        audio = audio.set_channels(1)

        self.get_fingerprinted_ads()

        for strt, end, ad_name in input_labels:
            # don't refingerprint already fingerprinted ads
            chunk = audio[strt * 1000:end * 1000]
            ad_hash = _unique_hash(chunk).upper()
            if ad_hash in self.adhashes_set:
                print("%s already fingerprinted, continuing..." % ad_name)
                continue

            ads_to_fingerprint.append(ad_name)
            audios_to_fingerprint.append(chunk)

        worker_input = zip(ads_to_fingerprint, audios_to_fingerprint)

        # Send off our tasks
        iterator = pool.imap_unordered(_fingerprint_worker,
                                       worker_input)

        # Loop till we have all of them
        while True:
            try:
                ad_name, hashes, ad_hash, duration = iterator.next()
            except multiprocessing.TimeoutError:
                continue
            except StopIteration:
                break
            except:
                print("Failed fingerprinting")
                # Print traceback because we can't reraise it here
                traceback.print_exc(file=sys.stdout)
            else:
                aid = self.db.insert_ad(ad_name, ad_hash, duration)

                self.db.insert_hashes(aid, hashes)
                self.db.set_ad_fingerprinted(aid)
                self.get_fingerprinted_ads()

        pool.close()
        pool.join()

    def find_matches(self, samples, Fs=djv_fingerprint.DEFAULT_FS):

        hashes = djv_fingerprint.fingerprint(samples, Fs=Fs)
        return self.db.return_matches(hashes), len(hashes)

    def align_matches(self, matches, dedup_hashes, queried_hashes: int,
                      topn: int = 2):
        """
            Finds hash matches that align in time with other matches and finds
            consensus about which hashes are "true" signal from the audio.
            Returns a dictionary with match information.
        """

        sorted_matches = sorted(matches, key=lambda m: (m[0], m[1]))
        counts = [(*key, len(list(group))) for key, group in groupby(sorted_matches, key=lambda m: (m[0], m[1]))]
        songs_matches = sorted(
            [max(list(group), key=lambda g: g[2]) for key, group in groupby(counts, key=lambda count: count[0])],
            key=lambda count: count[2], reverse=True
        )

        songs_result = []
        for song_id, offset, _ in songs_matches[0:topn]:  # consider topn elements in the result
            ad = self.db.get_song_by_id(song_id)

            nseconds = round(float(offset) / djv_fingerprint.DEFAULT_FS *
                             djv_fingerprint.DEFAULT_WINDOW_SIZE *
                             djv_fingerprint.DEFAULT_OVERLAP_RATIO, 5)
            hashes_matched = dedup_hashes[song_id]
            adname = ad.get(ComDet.AD_NAME, None)
            duration = ad["total_hashes"]
            ad = {
                ComDet.AD_ID: song_id,
                ComDet.AD_NAME: adname,
                ComDet.AD_DURATION: duration,
                "INPUT_CONFIDENCE": round(hashes_matched / queried_hashes, 2),
                # Percentage regarding hashes matched vs hashes fingerprinted in the db.
                "FINGERPRINTED_CONFIDENCE": round(hashes_matched / duration, 2),
                ComDet.OFFSET: offset,
                ComDet.OFFSET_SECS: nseconds}

            songs_result.append(ad)

        return songs_result


    def recognize_segment(self, audio_segment):
        """
            Function:
                Given an audio segment recognize the ad present in it.
            Input:
                audio_segment: Pydub's audio segment
            Returns:
                [name, confidence, offset, duration]
        """
        data = np.fromstring(audio_segment._data, np.int16)
        data = data[0::audio_segment.channels]
        Fs = audio_segment.frame_rate
        (matches, dedup_hashes), count = self.find_matches(data, Fs=Fs)
        return self.align_matches(matches, dedup_hashes, count)

    def recognize_ads_file(self, input_file_path):
        """
            Function:
                Recognize all the commercials present in the file
                1) Convert input_file to wav file
                2) Process the file in chunks
                3) Skip parts based on the offset and duration of recognize_chunk
                4) Append [start_time, end_time, name] of each confident segment

            Input:
                input_file_path: Either video or audio file whose ads have to be detected.
            Returns:
                List of [start_time, end_time, name] of all the ads detected in the file
        """
        #ext = input_file_path.split('.')[-1]
        audio = pydub.AudioSegment.from_file(input_file_path)
        audio = audio.set_channels(1)

        strt = 0
        duration = audio.duration_seconds

        labels = []
        while strt < duration - self.config['analyze_span']:
            end = strt + self.config['analyze_span']
            audio_segment = audio[strt * 1000:end * 1000]
            ad = self.recognize_segment(audio_segment)

            if ad:
                ad = ad[0]
                end = int(strt + ad[ComDet.AD_DURATION])
                end_string = get_time_string(end)
                if strt == end:
                    end += self.config['analyze_skip']
                strt = int(strt - ad[ComDet.OFFSET_SECS])

                strt_string = get_time_string(strt)
                if ad["FINGERPRINTED_CONFIDENCE"] > 0.2:
                    print("Found:", strt_string, end_string, ad[ComDet.AD_NAME], ad["INPUT_CONFIDENCE"], ad["FINGERPRINTED_CONFIDENCE"])
                    labels.append([strt_string, end_string, ad[ComDet.AD_NAME]])

                strt = end
            else:
                strt += self.config['analyze_skip']
        return labels

    def clear_data(self):
        """
            Clear the database of all contents
        """

        self.db.empty()


def _unique_hash(audio):
    # Create a hash of the entire audio
    data = np.fromstring(audio._data, np.int16)
    return sha1(data).hexdigest()


def _fingerprint_worker(input):
    # Pool.imap sends arguments as tuples so we have to unpack
    # them ourself.
    try:
        ad_name, audio = input
    except:
        pass

    data = np.fromstring(audio._data, np.int16)
    channel = data[0::audio.channels]
    Fs = audio.frame_rate

    result = set()

    print("Fingerprinting %s" % (ad_name))
    hashes = djv_fingerprint.fingerprint(channel, Fs=Fs)
    print("Finished %s" % (ad_name))
    result |= set(hashes)
    ad_hash = _unique_hash(audio)
    duration = audio.duration_seconds
    return ad_name, result, ad_hash, duration


def test_generate():
    input_file_path = "/home/alexander/Downloads/clips/redbull.mp4"
    ad_det = ComDet()
    ad_det.fingerprint_clip(input_file_path)


@timeit
def test_recognize():
    #input_file_path = "/home/alexander/Downloads/scene_det/vbl02.mkv"
    input_file_path = "/home/alexander/Downloads/rblzgaming_40318149080_1673450735.033.mp4"
    ad_det = ComDet()
    print(ad_det.recognize_ads_file(input_file_path))


if __name__ == '__main__':
    #test_generate()
    test_recognize()