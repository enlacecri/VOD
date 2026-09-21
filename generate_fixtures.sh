#!/bin/bash
set -e

cd tests/fixtures

# Create 320x240
ffmpeg -y -f lavfi -i testsrc=duration=1:size=320x240:rate=30 -f lavfi -i sine=frequency=1000:duration=1 -c:v libx264 -c:a aac 320x240.mp4
# Create 640x360
ffmpeg -y -f lavfi -i testsrc=duration=1:size=640x360:rate=30 -f lavfi -i sine=frequency=1000:duration=1 -c:v libx264 -c:a aac 640x360.mp4
# Create 1280x720
ffmpeg -y -f lavfi -i testsrc=duration=1:size=1280x720:rate=30 -f lavfi -i sine=frequency=1000:duration=1 -c:v libx264 -c:a aac 1280x720.mp4
# Create 1920x1080
ffmpeg -y -f lavfi -i testsrc=duration=1:size=1920x1080:rate=30 -f lavfi -i sine=frequency=1000:duration=1 -c:v libx264 -c:a aac 1920x1080.mp4
# Create 1080x1920
ffmpeg -y -f lavfi -i testsrc=duration=1:size=1080x1920:rate=30 -f lavfi -i sine=frequency=1000:duration=1 -c:v libx264 -c:a aac 1080x1920.mp4
# Create 1280x720-no-audio
ffmpeg -y -f lavfi -i testsrc=duration=1:size=1280x720:rate=30 -c:v libx264 1280x720-no-audio.mp4
