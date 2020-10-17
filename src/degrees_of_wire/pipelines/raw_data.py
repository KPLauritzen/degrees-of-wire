from kedro.pipeline import Pipeline, node
import requests

def download_file(url):
    file = requests.get(url)
    return file.content

def create_raw_pipeline():
    return Pipeline([
        node(
            download_file,
            "params:raw_movies_url",
            "movies_raw"
        )
    ])

