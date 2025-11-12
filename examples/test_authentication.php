#!/usr/bin/env php
<?php
/**
 * Simple LORIS API Authentication Diagnostic Tool
 *
 * Tests authentication for multiple API versions and verifies token.
 */

require __DIR__ . '/../vendor/autoload.php';

use GuzzleHttp\Client;

echo "\n=================================================\n";
echo "  LORIS API Authentication Diagnostic Tool\n";
echo "=================================================\n\n";

// Load config
$configFile = __DIR__ . '/../config/loris_client_config.json';
if (!file_exists($configFile)) {
    echo "Configuration file not found: {$configFile}\n";
    exit(1);
}

$config = json_decode(file_get_contents($configFile), true);
if (json_last_error() !== JSON_ERROR_NONE) {
    echo "Invalid JSON in config file: " . json_last_error_msg() . "\n";
    exit(1);
}

$baseUrl  = rtrim($config['api']['base_url'], '/');
$username = $config['api']['username'];
$password = $config['api']['password'];
$versions = ['api/v0.0.3', 'api/v0.0.4', 'api/v0.0.4-dev'];

echo "Configuration:\n";
echo "  Base URL : {$baseUrl}\n";
echo "  Username : {$username}\n";
echo "  Password : " . str_repeat('*', strlen($password)) . "\n\n";

// Step 1: Check base URL
echo "Test 1: Checking if base URL is reachable...\n";
try {
    $client = new Client(['timeout' => 10, 'verify' => false, 'http_errors' => false]);
    $response = $client->get($baseUrl);
    $status = $response->getStatusCode();
    echo "  Status Code: {$status}\n";
    if ($status >= 200 && $status < 400) {
        echo "  Base URL reachable.\n\n";
    } else {
        echo "  Warning: Non-success status returned.\n\n";
    }
} catch (Exception $e) {
    echo "  Error: Cannot reach base URL: " . $e->getMessage() . "\n";
    exit(1);
}

// Step 2: Try authentication for each API version
echo "Test 2: Attempting authentication for each API version...\n\n";

foreach ($versions as $version) {
    $loginUrl = "{$baseUrl}/{$version}/login";
    echo "Testing {$loginUrl}\n";

    try {
        $client = new Client(['timeout' => 20, 'verify' => false, 'http_errors' => false]);
        $response = $client->post($loginUrl, [
            'headers' => [
                'Content-Type' => 'application/json',
                'Accept' => 'application/json'
            ],
            'json' => [
                'username' => $username,
                'password' => $password
            ]
        ]);

        $status = $response->getStatusCode();
        $body = $response->getBody()->getContents();

        echo "  HTTP Status: {$status}\n";

        if ($status !== 200) {
            echo "  Authentication failed.\n\n";
            continue;
        }

        $data = json_decode($body, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            echo "  Invalid JSON response: " . json_last_error_msg() . "\n\n";
            continue;
        }

        if (!isset($data['token'])) {
            echo "  No token in response.\n\n";
            continue;
        }

        $token = $data['token'];
        echo "  Authentication successful.\n";
        echo "  Token (first 20 chars): " . substr($token, 0, 20) . "...\n";

        // Step 3: Test token with /candidates
        $candidatesUrl = "{$baseUrl}/{$version}/candidates";
        echo "  Testing token on {$candidatesUrl}\n";

        $authResponse = $client->get($candidatesUrl, [
            'headers' => [
                'Authorization' => 'Bearer ' . $token,
                'Accept' => 'application/json'
            ]
        ]);

        $authStatus = $authResponse->getStatusCode();
        echo "  Authenticated Request Status: {$authStatus}\n";

        if ($authStatus === 200) {
            echo "\nSUCCESS! Working API version: {$version}\n";
            echo "=================================================\n";
            exit(0);
        } else {
            echo "  Token valid but request failed (HTTP {$authStatus}).\n\n";
        }

    } catch (Exception $e) {
        echo "  Error: " . $e->getMessage() . "\n\n";
    }
}

echo "=================================================\n";
echo "All authentication attempts failed.\n";
echo "=================================================\n\n";

echo "Try manually with curl:\n";
echo "curl -X POST '{$baseUrl}/api/v0.0.3/login' \\\n";
echo "  -H 'Content-Type: application/json' \\\n";
echo "  -d '{\"username\":\"{$username}\",\"password\":\"{$password}\"}' \\\n";
echo "  -v\n\n";

exit(1);
