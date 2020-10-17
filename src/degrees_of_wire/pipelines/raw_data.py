from kedro.pipeline import Pipeline, node


def download_file(url):
    return url

def create_raw_pipeline():
    return Pipeline([
        node(
            download_file,
            "params:raw_movies_url",
            "movies_raw"
        )
    ])