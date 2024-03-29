# research-similar-users
Find the most similar users to a given user on Wikipedia based on edit history.

This repository has two primary components: the API endpoint (on Cloud VPS) and interface (also running on Cloud VPS but could be separated out)

## Running the service

Locally, you can run an instance of the service reflecting the current state of the repo by running `make docker`. This requires [Blubber](https://wikitech.wikimedia.org/wiki/Blubber).

We use [Blubber](https://wikitech.wikimedia.org/wiki/Blubber/Download) to create `Dockerfile`s. 
Installation documentation can be found at https://wikitech.wikimedia.org/wiki/Blubber/Download.

## API
See [the API template](https://github.com/wikimedia/research-api-endpoint-template) for more details on how to start and update the instance, though updates for this repository are much more manual than desirable at the moment until the config is updated. The instance has a nginx web server that sends requests via uWSGI to a Flask app.

### Documentation
Swagger documentation is available at the `/apidocs` endpoint.

A Json OpenAPI spec file can be generated with:
```bash
make apidocs
```

A zero-dependency HTML-file documenting the api can be generated with
```bash
make htmldocs
```
The output files of both `apidocs` and `htmldocs` commands will be available under `doc/build`.


### Privacy / Access
The default logging by nginx builds an access log located at `/var/log/nginx/access.log` that logs IP, timestamp, referer, request, and user_agent information.
I have overridden that in this repository (`model.nginx`) to remove IP and user-agent so as not to retain private data unnecessariliy.
This can be [updated easily](https://docs.nginx.com/nginx/admin-guide/monitoring/logging/#setting-up-the-access-log).

The user-interface is protected behind a simple username and password (not included here) to reduce the chance
of someone accessing the API through that mechanism.

### Relevant Data
There are three data files that are currently maintained as Python dictionaries in memory that do the bulk of the heavy lifting of this application (`USER_METADATA`, `TEMPORAL_DATA`, `COEDIT_DATA`).
They are loaded by the `load_data` function in `wsgi.py`. These data files are generated via PySpark notebooks that run on the analytics cluster and can be updated monthly when new data dumps are available. Notably,
they are not just present for READ operations but are also updated by the application with a user's new edits if that user is queried (as a form of caching). This is to reduce latency
on future queries about that user. It complicates the logic around this data though as it becomes both READ and WRITE and so could be removed if needed, but this would largely eliminate our ability to cache results, which I consider important because the number of users under investigation for sockpuppet is a fairly small set so I expect duplicate queries to be common.
Details on each below:
* `COEDIT_DATA` (1.1G): for every user that has edited a relevant page in 2020, this contains up to 250 most-similar users in terms of the number of edits in which they overlapped. This can be more than 250 if e.g., the 230th-280th most-similar editors all have the same overlap with a user.
* `TEMPORAL_DATA` (150MB): for every user in `COEDIT_DATA`, this contains information on which days and which hours this user most often edits. While this data is stored in the file sparsely (only data on the days/hours that are actually edited by a user), in the application the data is stored as dense vectors so that cosine similarity calculations used for temporal overlap are simple.
* `USER_METADATA` (203MB): for every user in `COEDIT_DATA`, this contains basic metadata about them (total number of edits in data, total number of pages edited, user or IP, timestamp range of edits).

Note, the sizes listed are for the raw data files -- in practice, the data takes up more space in memory because of how it is stored within the application to allow for easy updating etc. and may grow with queries (albeit quite slowly).
The raw files are not contained within this repository as they are quite large and there is little value to version control for them.

#### Database ingestion
A script to load the service models into a database is provided under `migrations`.
It can be executed  with:
```bash
cd migrations
PYTHONPATH=../ python ingest.py -r ../similar_users/resources --db-connection-string sqlite:////app.db --create-tables --dry-run 
```
When executing with `--dry-run`, ingestion statistics will be provided without actually modifying the
database. `--create-tables` instructs the script to create schemas before attempting insertion. 
Note that `ingest.py` performs a refresh at each  run: old records are deleted before inserting
new ones.

The service can then query data from a database by setting `SQLALCHEMY_DATABASE_URI` in `flask_config.yaml`.

### Relevant Files
* `flask_config.yaml`: this file contains key parameters for the API and the username/password so it is not currently included in this repository. Reach out to Isaac if you need access.
* `wsgi.py`: this contains the entirety of the flask API. The logic happens in the `get_similar_users` function. There are several stages:
    * `validate_api_args`: gather and validate arguments passed via URL.
    * `get_additional_edits`: if the user has edited since the last time of the co-edit data was updated (currently 30 Sept), gather their new edit history
    * `update_coedit_data`: for each new edit made by the user, update co-edit data. This can potentially be a large number of API calls and limits should probably be added to cap latency.
    * `build_result`: for the top-k most-similar users (users with greatest edit overlap), gather information on edit overlap and temporal overlap

## UI
A simple user interface for querying the API is also hosted at on the instance. It is currently password-protected.
It is also a simple flask app that is managed via `wsgi.py` but the logic all happens in `index.html`.
It consists of a form for collecting a username and how many results should be returned. A submit button then triggers some javascript in the page that formulates the correct
URL for the API, calls the API, and the formats the JSON results in a nice data table (and adds some additional links for easier exploration).
