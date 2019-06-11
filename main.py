from argparse import ArgumentParser
from wikidata_endpoint import WikidataEndpoint, WikidataEndpointConfiguration
from pathlib import Path


def main():
    parser = ArgumentParser()
    parser.add_argument('wikidata_ids', metavar='N', type=int, nargs='+')
    args = parser.parse_args()
    wikidata_ids = args.wikidata_ids
    wikidata_ids = range(1000)
    endpoint = WikidataEndpoint(WikidataEndpointConfiguration(Path("resources/wikidata_endpoint_config.ini")))

    query = open('resources/query.rq').read() % '\n'.join(f'wd:Q{wikidata_id}' for wikidata_id in wikidata_ids)

    with endpoint.request() as request:
        for answer in request.post(query):
            print(answer)


if __name__ == '__main__':
    main()
