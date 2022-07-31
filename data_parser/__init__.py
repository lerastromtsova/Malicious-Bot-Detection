from github import Github  # type: ignore
import base64


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
    comment_ids = {}
    g = Github(github_access_token)
    repo = g.get_repo(comment_id_source_repo)
    for path in paths_to_data:
        contents = repo.get_contents(path)
        while contents:
            cur_file = contents.pop(0)
            if cur_file.type == "dir":
                contents.extend(repo.get_contents(cur_file.path))
            else:
                content = cur_file.content
                content_str = base64.b64decode(content).decode('utf-8')
                comment_ids[cur_file.path] = content_str.split('\n')
    # TODO: Save comment_ids to a file (JSON)
    return comment_ids


# TODO: Implement
def parse_comment_data():
    # Gets comment data from VK based on comment IDs
    return
