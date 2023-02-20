import math
import pydub

input_file_path = "/home/alexander/Downloads/rblzgaming_40318149080_1673450735.033.mp4"

print("Load File ...")
audio = pydub.AudioSegment.from_file(input_file_path)

length = audio.duration_seconds
split = 3600  # 1h
for i in range(math.ceil(length/split)):
    start = split * i * 1000
    end = min(split*(i+1), length) * 1000
    print(start, end)
    audio[start:end].export(f"/home/alexander/Downloads/rblzgaming_40318149080_1673450735.033_{i}.mp3")