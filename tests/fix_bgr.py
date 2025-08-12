import av
import numpy as np
import os
import glob
import sys
from fractions import Fraction
from concurrent.futures import ThreadPoolExecutor, as_completed

if len(sys.argv) < 2:
    print("Usage: python fix_videos.py /path/or/video.mp4 [max_workers]")
    sys.exit(1)

input_path = sys.argv[1]
max_workers = int(sys.argv[2]) if len(sys.argv) > 2 else 4

# Determine if single file or directory
if os.path.isfile(input_path) and input_path.lower().endswith(".mp4"):
    video_files = [input_path]
    test_mode = True
else:
    video_files = glob.glob(os.path.join(input_path, "**", "*.mp4"), recursive=True)
    test_mode = False

print(f"Found {len(video_files)} mp4 files. Test mode: {test_mode}")

def fix_video_pyav(video_path):
    if test_mode:
        output_path = os.path.splitext(video_path)[0] + "_fix.mp4"
    else:
        output_path = video_path + ".tmp.mp4"

    container = av.open(video_path)
    stream = container.streams.video[0]

    fps = Fraction(stream.average_rate) if stream.average_rate else Fraction(30, 1)
    width = stream.codec_context.width
    height = stream.codec_context.height

    out_container = av.open(output_path, mode='w')
    out_stream = out_container.add_stream('libx264', rate=fps)
    out_stream.width = width
    out_stream.height = height
    out_stream.pix_fmt = 'yuv420p'

    for frame in container.decode(stream):
        img_rgb_wrong = frame.to_ndarray(format='rgb24')  # Actually BGR order
        img_fixed = img_rgb_wrong[:, :, ::-1]  # Swap R <-> B
        new_frame = av.VideoFrame.from_ndarray(img_fixed, format='rgb24')
        out_container.mux(out_stream.encode(new_frame))

    out_container.mux(out_stream.encode())  # flush

    container.close()
    out_container.close()

    if not test_mode:
        os.replace(output_path, video_path)

    return f"✅ Fixed {video_path}"

if test_mode:
    print(fix_video_pyav(video_files[0]))
else:
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fix_video_pyav, vf): vf for vf in video_files}
        for future in as_completed(futures):
            print(future.result())
