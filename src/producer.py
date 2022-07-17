import time
import sys
import cv2

from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(bootstrap_servers="localhost:9092")
topic = "my-topic"


def emit_video(path_to_video):
    print("start")

    video = cv2.VideoCapture(path_to_video)

    while video.isOpened():
        success, frame = video.read()
        if not success:
            break

        # png might be too large to emit
        data = cv2.imencode(".jpeg", frame)[1].tobytes()

        future = producer.send(topic, data)
        try:
            future.get(timeout=10)
        except KafkaError as e:
            print(e)
            break

        print(".", end="", flush=True)
        time.sleep(0.1)
    print()
    video.release()
    print("Video Processed and Emitted")


emit_video(
    "https://one-click-camera-connect.s3.ap-south-1.amazonaws.com/41ee3044-b6d6-42ac-8ffa-8b0434ff766d/1/1/recording-cam1-0.mp4"
)
