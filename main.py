from dotenv import dotenv_values
from data_parser import parse_comment_ids, parse_comment_data

config = dotenv_values(".env")

if __name__ == '__main__':
    comment_ids = parse_comment_ids(
        comment_id_source_repo=config['COMMENT_ID_SOURCE_REPO'],
        github_access_token=config['GITHUB_ACCESS_TOKEN'],
        paths_to_data=[
            'VK/public-reaction/independent',
            'VK/public-reaction/state-affiliated'
        ]
    )
    parse_comment_data()
