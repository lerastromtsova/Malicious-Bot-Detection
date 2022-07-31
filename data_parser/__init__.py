from github import Github  # type: ignore
import base64
import os.path
import json
import logging


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
        g = Github(github_access_token)
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
            except Github.RateLimitExceededException:
                pass
        with open(f'data/{comment_id_file_name}.json', 'w') as fp:
            json.dump(comment_ids, fp)
    logging.info("Comment IDs parsing finished")
    return comment_ids


# TODO: Implement
def parse_comment_data():
    # Gets comment data from VK based on comment IDs
    return
