from utill import get_twitter_id, get_files_with_dir_path
from WriterWrapper import WriterWrapper
from time import sleep
import os
import csv


DATA_PATH = './'
MEDIA_PATH = os.path.join(DATA_PATH, 'media')


def add_user_id(path: str):
    media_alignment_reader = csv.DictReader(open(path, 'r', encoding='utf-8'))
    new_lines = []
    new_field = 'user_id'
    for i, line_dict in enumerate(media_alignment_reader):
        line_dict[new_field] = get_twitter_id(line_dict['twitter_accounts'].split('/')[-1])
        new_lines.append(line_dict)
        print(' | '.join([line_dict['domain'], line_dict['twitter_accounts'], str(line_dict[new_field])]))
        sleep(0.3)

    writer = WriterWrapper(
        os.path.join(MEDIA_PATH, 'reviewed_media_alignment_with_twitter_id'),
        media_alignment_reader.fieldnames + [new_field]
    )
    for line in new_lines:
        writer.write_row(line)
    writer.close()


if __name__ == '__main__':
    add_user_id(get_files_with_dir_path(MEDIA_PATH, 'reviewed_media_alignment')[0])
