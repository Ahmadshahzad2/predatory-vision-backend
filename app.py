from flask import Flask, request, send_file,jsonify
from utils import send_image_to_lambda
import cv2
import tempfile
import os
import io
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask_cors import CORS
import boto3

# Initialize the S3 client
s3 = boto3.client('s3')

# Set your S3 bucket name
S3_BUCKET_NAME = 'predatoryanimalvision'



app = Flask(__name__)

CORS(app)


@app.route('/process-image', methods=['POST'])
def process_image():
    file = request.files['file']
    temp_dir = tempfile.mkdtemp()
    file_path = os.path.join(temp_dir, file.filename)
    file.save(file_path)
    
    color_image = cv2.imread(file_path)
    processed_image = send_image_to_lambda(color_image)
    
    _, buffer = cv2.imencode(".jpg", processed_image)
    io_buffer = io.BytesIO(buffer)
    
    response = send_file(
        io_buffer,
        mimetype='image/jpeg',
        as_attachment=True,
        download_name='processed_' + file.filename  # Corrected parameter for filename
    )
    
    os.remove(file_path)
    os.rmdir(temp_dir)
    return response
    

# @app.route('/process-video', methods=['POST'])
# def process_video():
#     file = request.files['file']
#     temp_dir = tempfile.mkdtemp()
#     input_video_path = os.path.join(temp_dir, file.filename)
#     output_video_path = os.path.join(temp_dir, 'processed_' + file.filename)
    
#     file.save(input_video_path)
#     count=0
#     cap = cv2.VideoCapture(input_video_path)
#     fourcc = cv2.VideoWriter_fourcc(*'mp4v')
#     out = cv2.VideoWriter(output_video_path, fourcc, 20.0, (int(cap.get(3)), int(cap.get(4))), True)
    
#     while cap.isOpened():
       
#         ret, frame = cap.read()
#         count+=1
#         print(count)
#         if not ret:
#             break
#         start=time.time()
#         processed_frame = send_image_to_lambda(frame)
#         diff=time.time()-start
#         print(diff)
#         out.write(processed_frame)
    
#     cap.release()
#     out.release()
    
#     return send_file(output_video_path, mimetype='video/mp4', as_attachment=True, attachment_filename='processed_' + file.filename)

# @app.route('/process-video', methods=['POST'])
# def process_video():
#     file = request.files['file']
#     temp_dir = tempfile.mkdtemp()
#     input_video_path = os.path.join(temp_dir, file.filename)
#     output_video_path = os.path.join(temp_dir, 'processed_' + file.filename)
    
#     file.save(input_video_path)
    
#     cap = cv2.VideoCapture(input_video_path)
#     fps = cap.get(cv2.CAP_PROP_FPS)  # Get the FPS from the input video
#     frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
#     frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
#     fourcc = cv2.VideoWriter_fourcc(*'mp4v')
#     out = cv2.VideoWriter(output_video_path, fourcc, fps, (frame_width, frame_height), True)
    
#     frames = []
#     frame_ids = []
#     count = 0

#     while True:
#         ret, frame = cap.read()
#         if not ret:
#             break
#         frames.append(frame)
#         frame_ids.append(count)
#         count += 1
        
#         # Process in batches of 30
#         if len(frames) == 60:
#             print(count)
#             start=time.time()

#             processed_frames = process_frames_concurrently(frames)
#             diff=time.time()-start
#             print(diff)
#             for processed_frame in processed_frames:
#                 out.write(processed_frame)
#             frames = []
#             frame_ids = []

#     # Process any remaining frames
#     if frames:
#         processed_frames = process_frames_concurrently(frames)
#         for processed_frame in processed_frames:
#             out.write(processed_frame)

#     cap.release()
#     out.release()
    
#     return send_file(output_video_path, mimetype='video/mp4', as_attachment=True, download_name='processed_' + file.filename)



@app.route('/process-video', methods=['POST'])
def process_video():
    file = request.files['file']
    temp_dir = tempfile.mkdtemp()
    input_video_path = os.path.join(temp_dir, file.filename)
    output_video_path = os.path.join(temp_dir, 'processed_' + file.filename)
    
    file.save(input_video_path)
    
    cap = cv2.VideoCapture(input_video_path)
    fps = cap.get(cv2.CAP_PROP_FPS)  # Get the FPS from the input video
    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(output_video_path, fourcc, fps, (frame_width, frame_height), True)
    
    frames = []
    frame_ids = []
    count = 0

    while True:
        ret, frame = cap.read()
        if not ret:
            break
        frames.append(frame)
        frame_ids.append(count)
        count += 1
        
        # Process in batches of 30
        if len(frames) == 60:
            print(count)
            start=time.time()

            processed_frames = process_frames_concurrently(frames)
            diff=time.time()-start
            print(diff)
            for processed_frame in processed_frames:
                out.write(processed_frame)
            frames = []
            frame_ids = []

    # Process any remaining frames
    if frames:
        processed_frames = process_frames_concurrently(frames)
        for processed_frame in processed_frames:
            out.write(processed_frame)

    cap.release()
    out.release()

    # Upload the processed video to S3
    s3_file_key = f"processed_videos/processed_{file.filename}"  # Define the path inside your S3 bucket
    s3.upload_file(output_video_path, S3_BUCKET_NAME, s3_file_key)

    # Generate the S3 URL
    s3_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{s3_file_key}"

    # Clean up the temporary directory
    os.remove(input_video_path)
    os.remove(output_video_path)
    os.rmdir(temp_dir)
    
    return jsonify({'url': s3_url})



# def process_frames_concurrently(frames):
#     """
#     This function takes a list of frames, sends them to Lambda concurrently,
#     and returns the processed frames in the order they were submitted.
#     """
#     with ThreadPoolExecutor(max_workers=30) as executor:
#         # Submit all frames to the executor
#         future_to_frame = {executor.submit(send_image_to_lambda, frame): frame for frame in frames}
#         processed_frames = []
#         for future in as_completed(future_to_frame):
#             processed_frame = future.result()
#             processed_frames.append(processed_frame)
#     return processed_frames

def process_frames_concurrently(frames):
    """
    This function takes a list of frames, sends them to Lambda concurrently,
    and returns the processed frames in the order they were submitted.
    """
    with ThreadPoolExecutor(max_workers=60) as executor:
        # Submit all frames to the executor and keep track of the order using the index
        future_to_index = {executor.submit(send_image_to_lambda, frame): idx for idx, frame in enumerate(frames)}
        processed_frames = [None] * len(frames)  # Preallocate the list to maintain order
        
        # Collect results as they complete and place them in the correct order
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            try:
                processed_frame = future.result()
                processed_frames[index] = processed_frame  # Place the frame at the correct index
            except Exception as e:
                print(f"Error processing frame {index}: {str(e)}")
                processed_frames[index] = None  # Handle errors if needed

    return processed_frames  # This list is now in the correct order

if __name__ == '__main__':
    app.run(debug=True,port=8000,host='0.0.0.0')