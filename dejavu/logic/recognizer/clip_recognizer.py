from time import time
from typing import Dict, List, Tuple
from itertools import groupby

import numpy as np

import dejavu.logic.decoder as decoder
from dejavu.base_classes.base_recognizer import BaseRecognizer
from dejavu.config.settings import (ALIGN_TIME, FINGERPRINT_TIME, QUERY_TIME,
                                    RESULTS, TOTAL_TIME)

from dejavu.config.settings import (DEFAULT_OVERLAP_RATIO,
                                    DEFAULT_WINDOW_SIZE, FIELD_FILE_SHA1,
                                    FIELD_TOTAL_HASHES,
                                    FINGERPRINTED_CONFIDENCE,
                                    FINGERPRINTED_HASHES, HASHES_MATCHED,
                                    INPUT_CONFIDENCE, INPUT_HASHES, OFFSET,
                                    OFFSET_SECS, SONG_ID, SONG_NAME)


class ClipRecognizer(BaseRecognizer):
    def __init__(self, dejavu):
        super().__init__(dejavu)

    def recognize_file(self, filename: str) -> Dict[str, any]:
        channels, self.Fs, _ = decoder.read(filename, self.dejavu.limit)
        t = time()
        matches, fingerprint_time, query_time, align_time = self._recognize(*channels)
        t = time() - t

        results = {
            TOTAL_TIME: t,
            FINGERPRINT_TIME: fingerprint_time,
            QUERY_TIME: query_time,
            ALIGN_TIME: align_time,
            RESULTS: matches
        }

        return results

    def _recognize(self, *data) -> Tuple[List[Dict[str, any]], int, int, int]:
        fingerprint_times = []
        hashes = set()  # to remove possible duplicated fingerprints we built a set.
        for channel in data:
            fingerprints, fingerprint_time = self.dejavu.generate_fingerprints(channel, Fs=self.Fs)
            fingerprint_times.append(fingerprint_time)
            hashes |= set(fingerprints)

        matches, dedup_hashes, query_time = self.dejavu.find_matches(hashes)
        t = time()
        final_results = self.align_matches(matches, len(hashes))
        align_time = time() - t

        return final_results, np.sum(fingerprint_times), query_time, align_time


    def align_matches(self, matches: List[Tuple[int, int]], queried_hashes: int) -> List[Dict[str, any]]:

        sorted_matches = sorted(matches, key=lambda m: (m[0], m[1]))
        counts = [(*key, len(list(group))) for key, group in groupby(sorted_matches, key=lambda m: (m[0], m[1]))]

        segment_matches = sorted(counts, key=lambda m: m[2], reverse=True)

        songs_result = []
        for song_id, offset, matches in segment_matches:  # consider topn elements in the result
            if matches < 10:
                break
            song = self.dejavu.db.get_song_by_id(song_id)

            song_name = song.get(SONG_NAME, None)
            song_hashes = song.get(FIELD_TOTAL_HASHES, None)
            nseconds = round(float(offset) / self.Fs * DEFAULT_WINDOW_SIZE * DEFAULT_OVERLAP_RATIO, 5)

            song = {
                SONG_ID: song_id,
                SONG_NAME: song_name.encode("utf8"),
                INPUT_HASHES: queried_hashes,
                FINGERPRINTED_HASHES: song_hashes,
                OFFSET: offset,
                OFFSET_SECS: nseconds,
                "offset_mins": nseconds / 60,
                "matches": matches,
                FIELD_FILE_SHA1: song.get(FIELD_FILE_SHA1, None).encode("utf8")
            }
            songs_result.append(song)
        return songs_result

    def recognize(self, filename: str) -> Dict[str, any]:
        return self.recognize_file(filename)
