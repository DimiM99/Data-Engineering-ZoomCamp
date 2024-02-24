todos:
- [ ] set up secrets.toml file in .dlt directory with the following (replace the placeholders with your own values):
```toml
[destination.filesystem]
bucket_url = "gs://{bucket_name}"

[destination.filesystem.credentials]
project_id = "set me up"
private_key = "set me up"
client_email = "set me up"
```