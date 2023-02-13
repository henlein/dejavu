from pydub import AudioSegment


mp4_version = AudioSegment.from_file("/home/alexander/Downloads/scene_det/vbl01.mkv")
mp4_version.export("/home/alexander/Downloads/scene_det/vbl01.mp3", format="mp3")

mp4_version = AudioSegment.from_file("/home/alexander/Downloads/scene_det/vbl02.mkv")
mp4_version.export("/home/alexander/Downloads/scene_det/vbl02.mp3", format="mp3")

mp4_version = AudioSegment.from_file("/home/alexander/Downloads/scene_det/vbl03.mkv")
mp4_version.export("/home/alexander/Downloads/scene_det/vbl03.mp3", format="mp3")


mp4_version = AudioSegment.from_file("/home/alexander/Downloads/redbull.mp4")
mp4_version.export("/home/alexander/Downloads/redbull.mp3", format="mp3")