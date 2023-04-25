from requests.exceptions import ConnectionError
import logging
import pandas as pd
from io import StringIO


def fetch(
    conn_id,
    output_filepath,
    endpoint=None,
    query=None,
    headers={},
    limit=None,
    format="csv",
):
    from airflow.providers.http.hooks.http import HttpHook

    output_filepath.parent.mkdir(parents=True, exist_ok=True)
    http = HttpHook(method="GET", http_conn_id=conn_id)
    responses = []
    if limit:
        offset = 0
        while True:
            paginated_query = query + f"&offset={offset}&limit={limit}"
            response = get_response(http, endpoint, paginated_query, headers)
            response.raise_for_status()
            nb_results = get_nb_results(response, format)
            if nb_results:
                responses.append(response)
                offset += nb_results
                if nb_results < limit:
                    break
    else:
        responses.append(get_response(http, endpoint, query, headers))
    if format == "csv":
        dfs = [pd.read_csv(StringIO(response.text)) for response in responses]
        df = pd.concat(dfs, ignore_index=True)
        df.to_csv(output_filepath, sep=",")
        return str(output_filepath)
    else:
        raise ValueError(format)


def get_response(http, endpoint, query, headers):
    try:
        response = http.run(
            endpoint=endpoint,
            data=query,
            headers=headers,
        )
    except ConnectionError as e:
        logging.error(e)
    else:
        return response


def get_nb_results(response, format="csv"):
    if format == "csv":
        results = pd.read_csv(StringIO(response.text), sep=",")
        return results.shape[0]
    raise ValueError(format)
