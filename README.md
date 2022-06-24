# cloudpmc-proto-firestore-loader
Cloud PMC prototype to load json documents into Firestore Database.

## Installing
```
$ git clone https://github.com/kolotev/cloudpmc-proto-firestore-loader.git
$ cd cloudpmc-proto-firestore-loader
$ python -m venv .venv
$ source .venv/bin/activate
$ pip install -e .
```

## Running

Currently supported list of commands:
```
$ cloudpmc-proto-firestore-loader  --help
Usage: cloudpmc-proto-firestore-loader [OPTIONS] COMMAND [ARGS]...

Options:
  --debug  Debug this application.
  --help   Show this message and exit.

Commands:
  get               get document from Firestore collection.
  list-collections  get list of top-level collections from Firestore...
  load              load JSON_FILES into Firestore database.
  query             find document(s) in Firestore collection.
```

Load article instance(s):
```
$ cloudpmc-proto-firestore-loader load --collection "article_instances" 13901.json 14901.json ...
```

Get article instance(s):
```
$ cloudpmc-proto-firestore-loader get --collection "article_instances"  13901 14901 ...
```
Then you can see the retrieved documents in files 13901.json, 14901.json in local folder.

List collections:
```
$ cloudpmc-proto-firestore-loader list-collections
```

Query documents from collection
```
$ cloudpmc-proto-firestore-loader query --collection "article_instances" 'pmcid == PMC13901' 'is_oa!=False' ...
```
The would be performed a logical AND between provided CONDITIONS (arguments).

## Additional info
If you want to be able to run this package's script without being asked 
for approval of your API requests you may setup environment as following
```
export GOOGLE_APPLICATION_CREDENTIALS=~/.service-account.json
```

Another possible alternative to setup the environment is the python code
```
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/path/to/credentials.json'
```

You should have a corresponding gcp-service-account.json file at your disposal.
It looks like this:
```
{
  "type": "service_account",
  "project_id": "ncbi-research-pmc",
  "private_key_id": "eccdbf716527b91b9d352015bb84a67c76a100d2",
  "private_key": "-----BEGIN PRIVATE KEY-----\n ... \n-----END PRIVATE KEY-----\n",
  "client_email": "...0751-compute@developer.gserviceaccount.com",
  "client_id": "...3818",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/...0751-compute%40developer.gserviceaccount.com"
}
```
