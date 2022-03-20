import json
import boto3
import requests


def lambda_handler(event, context):
    # TODO implement
    #url = create_url()
    timeout = 0
    while True:
        # connect_to_endpoint(url)
        rules = get_rules()
        delete = delete_all_rules(rules)
        set = set_rules(delete)
        get_stream(set)
        timeout += 1


def create_url():
    return "https://api.twitter.com/2/tweets/sample/stream"


def bearer_oauth(r):
    bearer_token = get_bearer_token()
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_bearer_token():
    secrets_client = boto3.client('secretsmanager')
    response = secrets_client.get_secret_value(
        SecretId='twitter-api-secret'
    )
    secret = json.loads(response['SecretString'])
    return secret['bearer_token']


def push_to_firehose(record):
    client = boto3.client('firehose')
    client.put_record(
        DeliveryStreamName='twitter-live-stream',
        Record={
            'Data': record
        }
    )


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth)
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(
                response.status_code,
                response.text))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )


def set_rules(delete):
    # You can adjust the rules if needed
    sample_rules = [
        {"value": "covid coronavirus covid-19 -is:retweet lang:en -has:media", "tag": "covid"},
        {"value": "ukraine -is:retweet lang:en -has:media", "tag": "ukraine"},
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(
                response.status_code,
                response.text))


def get_stream(set):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at",
        auth=bearer_oauth,
        stream=True,
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            push_to_firehose(json.dumps(json_response))
