import argparse
import uuid

parser = argparse.ArgumentParser()
parser.add_argument('-fr', '--files-root', help='Path where the file')
parser.add_argument('-fn', '--file-name', help='Name of the file to read')
args = vars(parser.parse_args())

f = open(args['files_root'] + '/' + args['file_name'], 'r')
for line in f:
    filename = uuid.uuid4().hex
    new_file = open(args['files_root'] + '/' + filename, 'w')
    new_file.write(args['file_name'] + ' ' + line)
    new_file.close()

    print filename
