import github  # type: ignore
import base64
import os
import json
import logging
import time
from datetime import datetime

import vk.exceptions  # type: ignore
from vk import API  # type: ignore


def parse_comment_ids(
        comment_id_source_repo: str,
        github_access_token: str,
        paths_to_data: list[str]
) -> dict:
    """
    Parses the VoynaSlov repository and returns the comment ids
    :param comment_id_source_repo: Reference to a Github repo with comment IDs
    :param github_access_token: Github access token
    :param paths_to_data: List of paths inside the repo that store the data
    :return: comment_ids
    """
    comment_id_file_name = comment_id_source_repo.replace('/', '_')
    if os.path.exists(f'/data/{comment_id_file_name}.json'):
        logging.info("Loading comment IDs from existing file...")
        with open(f'/data/{comment_id_file_name}.json') as json_file:
            comment_ids = json.load(json_file)
    else:
        logging.info(f"No existing data found. Parsing comment IDs "
                     f"from repository {comment_id_source_repo}.")
        comment_ids = {}
        g = github.Github(github_access_token)
        repo = g.get_repo(comment_id_source_repo)
        for path in paths_to_data:
            logging.info(f"Parsing path {path}")
            try:
                contents = repo.get_contents(path)
                while contents:
                    cur_file = contents.pop(0)
                    if cur_file.type == "dir":
                        contents.extend(repo.get_contents(cur_file.path))
                    else:
                        logging.info(f"Parsing file {cur_file}")
                        content = cur_file.content
                        content_str = base64.b64decode(content).decode('utf-8')
                        comment_ids[cur_file.path] = content_str.split('\n')
            except github.GithubException:
                pass
        with open(f'data/{comment_id_file_name}.json', 'w') as fp:
            json.dump(comment_ids, fp)
    logging.info("Comment IDs parsing finished")
    return comment_ids


def parse_comment_data(
        api: API
) -> list:
    """
    Parses comments on Vkontakte based on comment_ids.
    Writes the output to data/output dir.
    :param api: API to parse from
    :return:
    """
    for root, dirs, files in os.walk("./data"):
        if root not in [
            './data',
            './data/independent',
            './data/state-affiliated',
            './data/output'
        ]:
            media_name = root.split('/')[-1]
            media_id = api.utils.resolveScreenName(
                screen_name=media_name, v='5.131'
            )['object_id']
            logging.info(f"Parsing media {media_name}, id {media_id}")
            for file in files:
                if file != '.DS_Store':
                    comments = {}
                    filepath = os.path.join(root, file)
                    with open(filepath, 'r') as f:
                        comment_ids = f.read().split('\n')
                        for comment_id in comment_ids:
                            time.sleep(1)
                            try:
                                comment = api.wall.getComment(
                                    owner_id=-media_id,
                                    comment_id=comment_id,
                                    v='5.131',
                                    extended=1
                                )
                                logging.info(f"Parsed comment {comment_id}")
                                comments[comment_id] = comment
                                yield comment
                            except vk.exceptions.VkAPIError:
                                pass
            logging.info(f"Parsed media {media_name}, id {media_id}")


def delete_old_files() -> None:
    """
    Utility function to delete files older than 24.02.2022
    from the data directory.
    :return:
    """
    for root, dirs, files in os.walk("./data"):
        if root not in [
            './data',
            './data/independent',
            './data/state-affiliated',
            './data/output'
        ]:
            for file in files:
                if file != '.DS_Store':
                    date = file.split('.')[0]
                    d = datetime.strptime(date, '%Y-%m-%d')
                    date_24_02 = datetime.strptime('24-02-2022', '%d-%m-%Y')
                    if d < date_24_02:
                        os.remove(root+'/'+file)
                        logging.info(f'Removed file {file}')
