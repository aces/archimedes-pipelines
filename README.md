#  LORIS PHP Client

A lightweight PHP client for the [LORIS](https://github.com/aces/Loris) REST API.  
It provides tools for authentication, token management,
and ingestion of clinical and imaging data through automated pipelines.

---

## Installation

```bash
# Clone repository
cd /opt
git clone https://github.com/aces/loris-php-client.git
cd loris-php-client

# Install dependencies
composer install

## Configuration
Copy the example config file and edit your LORIS credentials:
cp config/loris_client_config.json.example config/loris_client_config.json
nano config/loris_client_config.json
````


# Test API connection
```php examples/test_authentication.php```

# Dry-run mode
```php examples/run_clinical_pipeline.php --all --dry-run --verbose```

# Process all data
```php examples/run_clinical_pipeline.php --all```

# Process specific project
```php examples/run_clinical_pipeline.php --collection=example_collection --project=PROJECT_A```

# View logs
```tail -f logs/clinical_$(date +%Y-%m-%d).log```
