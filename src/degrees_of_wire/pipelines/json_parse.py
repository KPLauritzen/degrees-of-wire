import json
from typing import List

from kedro.pipeline import Pipeline, node


def json_parse(lines: List[str], n_lines) -> List[dict]:
    if n_lines is not None:
        lines = lines[:n_lines]
    return [json.loads(l) for l in lines]


def clean_single_name(name: str) -> str:
    if name[0] == "[" and name[-1] == "]":
        is_link = True
        name = name.strip("[]")
    else:
        is_link = False

    if is_link and "|" in name:
        # This is a labelled link. Use first part. That is canonical link
        name = name.split("|")[0]
    return name

def clean_names_in_line(line: dict):
    name_keys = ["cast", "directors"]
    for key in name_keys:
        try:
            cleaned = [clean_single_name(name) for name in line[key]]
            line[key] = cleaned
        except KeyError:
            pass
    return line


def clean_names(lines: List[dict]) -> List[dict]:
    return [clean_names_in_line(l) for l in lines]


def create_json_pipeline():
    return Pipeline([
        node(
            json_parse,
            inputs=["movies_raw","params:parsed_movie_lines"],
            outputs="movies_json_lines"
        ),
        node(
            clean_names,
            "movies_json_lines",
            "movies_json_lines_cleaned_names"
        )

    ])