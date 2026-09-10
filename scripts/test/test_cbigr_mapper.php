#!/usr/bin/env php
<?php
/**
 * Test CBIGR Mapper - Check if ExternalID exists in LORIS
 *
 * Usage:
 *   php test_cbigr_mapper.php MRAC-NORM-003
 *   php test_cbigr_mapper.php MRAC-NORM-003,MRAC-NORM-0023
 */

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use GuzzleHttp\Client as GuzzleClient;

// Parse arguments
if ($argc < 2) {
    echo "Usage: php test_cbigr_mapper.php <external_id>\n";
    echo "Example: php test_cbigr_mapper.php MRAC-NORM-003\n";
    echo "Example: php test_cbigr_mapper.php MRAC-NORM-003,MRAC-NORM-0023\n";
    exit(1);
}

$externalIDs = $argv[1];

// Load config
$configFile = __DIR__ . '/../config/loris_client_config.json';
if (!file_exists($configFile)) {
    // Try alternative paths
    $configFile = __DIR__ . '/../loris_client_config.json';
}
if (!file_exists($configFile)) {
    $configFile = '/opt/archimedes-pipelines/config/loris_client_config.json';
}
if (!file_exists($configFile)) {
    $configFile = '/opt/archimedes-pipelines/loris_client_config.json';
}
if (!file_exists($configFile)) {
    fwrite(STDERR, "Configuration file not found.\n");
    fwrite(STDERR, "Tried:\n");
    fwrite(STDERR, "  - " . __DIR__ . "/../config/loris_client_config.json\n");
    fwrite(STDERR, "  - " . __DIR__ . "/../loris_client_config.json\n");
    fwrite(STDERR, "  - /opt/archimedes-pipelines/config/loris_client_config.json\n");
    fwrite(STDERR, "  - /opt/archimedes-pipelines/loris_client_config.json\n");
    exit(1);
}

$config = json_decode(file_get_contents($configFile), true);
if (json_last_error() !== JSON_ERROR_NONE) {
    fwrite(STDERR, "Invalid JSON in configuration: " . json_last_error_msg() . "\n");
    exit(1);
}

// Authenticate
$lorisConfig = $config['loris'] ?? $config['api'] ?? null;
if (!$lorisConfig) {
    fwrite(STDERR, "Error: 'loris' or 'api' section not found in config\n");
    exit(1);
}

$baseUrl = rtrim($lorisConfig['base_url'], '/');
$username = $lorisConfig['username'];
$password = $lorisConfig['password'];
$version = $lorisConfig['api_version'] ?? 'v0.0.4-dev';

echo str_repeat("═", 70) . "\n";
echo "  CBIGR ExternalID Mapper Test\n";
echo str_repeat("═", 70) . "\n";
echo "LORIS URL    : {$baseUrl}\n";
echo "ExternalID(s): {$externalIDs}\n";
echo str_repeat("─", 70) . "\n\n";

$httpClient = new GuzzleClient([
    'verify' => false,
    'timeout' => 30,
]);

// Step 1: Login
echo "1. Authenticating with LORIS...\n";
try {
    $response = $httpClient->request('POST', "{$baseUrl}/api/{$version}/login", [
        'json' => [
            'username' => $username,
            'password' => $password,
        ],
    ]);

    $result = json_decode((string)$response->getBody(), true);
    $token = $result['token'] ?? null;

    if (!$token) {
        fwrite(STDERR, "✗ Authentication failed: No token in response\n");
        exit(1);
    }

    echo "   ✓ Authenticated successfully\n\n";

} catch (\Exception $e) {
    fwrite(STDERR, "✗ Authentication failed: " . $e->getMessage() . "\n");
    exit(1);
}

// Step 2: Call CBIGR mapper
echo "2. Calling CBIGR ExternalID mapper...\n";
echo "   Endpoint: {$baseUrl}/cbigr_api/externalToInternalIdMapper\n";
echo "   Payload: [\"$externalIDs\"]\n\n";

try {
    $response = $httpClient->request('POST', "{$baseUrl}/cbigr_api/externalToInternalIdMapper", [
        'headers' => [
            'Authorization' => "Bearer {$token}",
            'Content-Type' => 'application/json',
        ],
        'json' => [$externalIDs],
    ]);

    $statusCode = $response->getStatusCode();
    $body = (string)$response->getBody();

    echo "3. Response:\n";
    echo "   Status Code: {$statusCode}\n";
    echo "   Body:\n";
    echo str_repeat("─", 70) . "\n";
    echo $body;
    echo "\n" . str_repeat("─", 70) . "\n\n";

    if ($statusCode === 200) {
        // Parse CSV response
        echo "4. Parsed Results:\n";
        $lines = explode("\n", trim($body));

        if (count($lines) >= 2) {
            // Header
            echo "   " . $lines[0] . "\n";
            echo "   " . str_repeat("-", 50) . "\n";

            // Data rows
            for ($i = 1; $i < count($lines); $i++) {
                if (trim($lines[$i]) === '') continue;

                $parts = str_getcsv($lines[$i]);
                $extID = trim($parts[0] ?? '', '="');  // Remove Excel formatting
                $pscid = trim($parts[1] ?? '');

                echo "   ExtID: {$extID}\n";
                echo "   PSCID: {$pscid}\n";

                if ($pscid && $pscid !== 'unauthorized_access') {
                    echo "   ✓ FOUND in LORIS\n";
                } elseif ($pscid === 'unauthorized_access') {
                    echo "   ✗ UNAUTHORIZED ACCESS\n";
                } else {
                    echo "   ✗ NOT FOUND\n";
                }
                echo "\n";
            }
        }
    }

    echo str_repeat("═", 70) . "\n";
    echo "✓ Test completed successfully\n";
    echo str_repeat("═", 70) . "\n";

} catch (\GuzzleHttp\Exception\ClientException $e) {
    $statusCode = $e->getCode();
    $responseBody = $e->getResponse() ? (string)$e->getResponse()->getBody() : '';

    echo "\n3. Error Response:\n";
    echo "   Status Code: {$statusCode}\n";
    echo "   Error Message: " . $e->getMessage() . "\n";

    if ($responseBody) {
        echo "   Response Body:\n";
        echo str_repeat("─", 70) . "\n";
        echo $responseBody;
        echo "\n" . str_repeat("─", 70) . "\n";
    }

    echo "\n";

    if ($statusCode === 400) {
        echo "✓ Result: ExternalID NOT FOUND in LORIS (400 BadRequest)\n";
        echo "  → This means: No entries in candidate_project_extid_rel table\n";
    } elseif ($statusCode === 404) {
        echo "✓ Result: ExternalID NOT FOUND in LORIS (404 NotFound)\n";
        echo "  → This means: ID mismatch or not in database\n";
    } else {
        echo "✗ Unexpected error code: {$statusCode}\n";
    }

    echo str_repeat("═", 70) . "\n";

} catch (\Exception $e) {
    fwrite(STDERR, "\n✗ CBIGR mapper call failed: " . $e->getMessage() . "\n");
    exit(1);
}