import json
import os
import requests
import spotipy
import boto3
from datetime import datetime


def get_access_token():

    response = requests.post(
        "https://accounts.spotify.com/api/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": os.environ["SPOTIFY_REFRESH_TOKEN"],
            "client_id": os.environ["SPOTIFY_CLIENT_ID"],
            "client_secret": os.environ["SPOTIFY_CLIENT_SECRET"],
        },
    )

    response.raise_for_status()

    return response.json()["access_token"]


def lambda_handler(event, context):

    access_token = get_access_token()

    sp = spotipy.Spotify(auth=access_token)

    playlist_link = "https://open.spotify.com/playlist/0d8KH0AXcqUMoqVsYEWfBx?si=5r4WTCmcRkWo_qIUpN07bQ"
    playlist_id = playlist_link.split("/")[-1].split("?")[0]

    spotify_data = sp.playlist_items(
        playlist_id,
        limit=100
    )

    s3 = boto3.client("s3")

    today = datetime.utcnow().strftime("%Y-%m-%d")

    filename = (
        f"spotify_raw_"
        f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    )

    s3.put_object(
        Bucket="spotify-etl-project-duc",
        Key=(
            f"raw_data/to_processed/"
            f"extraction_date={today}/"
            f"{filename}"
        ),
        Body=json.dumps(spotify_data)
    )

    glue = boto3.client("glue")
    glue_job_name = "spotify_transformation_job"

    try:
        response = glue.start_job_run(
            JobName=glue_job_name
        )

        print(
            f"Glue job started successfully. "
            f"Run ID: {response['JobRunId']}"
        )

    except Exception as e:
        print(
            f"Failed to start Glue job: {e}"
        )
        raise e

    return {
        "statusCode": 200,
        "message": "Spotify data uploaded to S3 and Glue started"
    }