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
```
$ cloudpmc-proto-firestore-loader load --collection "article_instances" 15901.json
```

## Additional info
If you want to be able to run this package's script without being asked 
for approval of your API requests you may setup environment as following
```
export GOOGLE_APPLICATION_CREDENTIALS=~/gcp-service-account.json
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
