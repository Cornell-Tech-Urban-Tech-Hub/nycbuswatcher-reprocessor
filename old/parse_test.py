import ijson


def parse_json(json_filename):
    with open(json_filename, 'rb') as input_file:
        # load json iteratively
        parser = ijson.parse(input_file, multiple_values=True)
        for prefix, event, value in parser:
            print('prefix={}, event={}, value={}'.format(prefix, event, value))


if __name__ == '__main__':
    parse_json('./data/daily-2021-01-26.gz.json')