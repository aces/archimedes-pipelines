#!/usr/bin/env php
<?php
/**
 * LORIS API Authentication Diagnostic Tool
 * 
 * Tests authentication against LORIS API and provides detailed debugging info
 * Compare this with curl to identify issues
 */

require __DIR__ . '/../vendor/autoload.php';

use GuzzleHttp\Client;

// Colors for terminal output
function colorize($text, $color) {
    $colors = [
        'red' => "\033[31m",
        'green' => "\033[32m",
        'yellow' => "\033[33m",
        'blue' => "\033[34m",
        'reset' => "\033[0m"
    ];
    return $colors[$color] . $text . $colors['reset'];
}

echo "\n";
echo colorize("=================================================\n", 'blue');
echo colorize("  LORIS API Authentication Diagnostic Tool\n", 'blue');
echo colorize("=================================================\n", 'blue');
echo "\n";

// Load configuration
$configFile = __DIR__ . '/../config/loris_client_config.json';

if (!file_exists($configFile)) {
    echo colorize("✗ Configuration file not found: {$configFile}\n", 'red');
    echo "Please copy loris_client_config.json.example to loris_client_config.json\n";
    exit(1);
}

$config = json_decode(file_get_contents($configFile), true);

if (json_last_error() !== JSON_ERROR_NONE) {
    echo colorize("✗ Invalid JSON in config file: " . json_last_error_msg() . "\n", 'red');
    exit(1);
}

// Extract API config
$baseUrl = rtrim($config['api']['base_url'], '/');
$username = $config['api']['username'];
$password = $config['api']['password'];

echo colorize("Configuration:\n", 'yellow');
echo "  Base URL: {$baseUrl}\n";
echo "  Username: {$username}\n";
echo "  Password: " . str_repeat('*', strlen($password)) . "\n";
echo "\n";

// Test 1: Check if base URL is reachable
echo colorize("Test 1: Checking if base URL is reachable...\n", 'yellow');
try {
    $client = new Client([
        'timeout' => 10,
        'verify' => false,
        'http_errors' => false
    ]);
    
    $response = $client->get($baseUrl);
    $statusCode = $response->getStatusCode();
    
    echo "  Status Code: {$statusCode}\n";
    
    if ($statusCode >= 200 && $statusCode < 400) {
        echo colorize("  ✓ Base URL is reachable\n", 'green');
    } else {
        echo colorize("  ⚠ Base URL returned non-success status code\n", 'yellow');
    }
} catch (Exception $e) {
    echo colorize("  ✗ Cannot reach base URL: " . $e->getMessage() . "\n", 'red');
    exit(1);
}
echo "\n";

// Test 2: Test /login endpoint with different configurations
echo colorize("Test 2: Testing authentication endpoint...\n", 'yellow');

$testConfigs = [
    'Standard (with base_uri)' => [
        'base_uri' => $baseUrl,
        'endpoint' => '/login',
        'timeout' => 30,
        'verify' => false,
        'http_errors' => false
    ],
    'Without base_uri' => [
        'endpoint' => $baseUrl . '/login',
        'timeout' => 30,
        'verify' => false,
        'http_errors' => false
    ],
    'With explicit Content-Type' => [
        'base_uri' => $baseUrl,
        'endpoint' => '/login',
        'timeout' => 30,
        'verify' => false,
        'http_errors' => false,
        'headers' => ['Content-Type' => 'application/json']
    ]
];

foreach ($testConfigs as $configName => $testConfig) {
    echo "\n  Testing: {$configName}\n";
    
    // Extract endpoint and headers
    $endpoint = $testConfig['endpoint'];
    unset($testConfig['endpoint']);
    
    $headers = $testConfig['headers'] ?? [];
    unset($testConfig['headers']);
    
    try {
        $testClient = new Client($testConfig);
        
        $requestOptions = [
            'json' => [
                'username' => $username,
                'password' => $password
            ]
        ];
        
        if (!empty($headers)) {
            $requestOptions['headers'] = $headers;
        }
        
        echo "    Request URL: " . (isset($testConfig['base_uri']) ? $testConfig['base_uri'] . $endpoint : $endpoint) . "\n";
        echo "    Request Body: " . json_encode($requestOptions['json']) . "\n";
        
        $response = $testClient->post($endpoint, $requestOptions);
        
        $statusCode = $response->getStatusCode();
        $body = $response->getBody()->getContents();
        
        echo "    Response Status: {$statusCode}\n";
        echo "    Response Headers:\n";
        foreach ($response->getHeaders() as $name => $values) {
            echo "      {$name}: " . implode(', ', $values) . "\n";
        }
        echo "    Response Body: " . substr($body, 0, 500) . "\n";
        
        if ($statusCode === 200) {
            $data = json_decode($body, true);
            if (isset($data['token'])) {
                echo colorize("    ✓ Authentication SUCCESSFUL!\n", 'green');
                echo "    Token (first 20 chars): " . substr($data['token'], 0, 20) . "...\n";
                
                // Test 3: Test using the token
                echo "\n" . colorize("Test 3: Testing token with authenticated request...\n", 'yellow');
                $token = $data['token'];
                
                try {
                    $authResponse = $testClient->get(
                        isset($testConfig['base_uri']) ? '/candidates' : $baseUrl . '/candidates',
                        [
                            'headers' => [
                                'Authorization' => 'Bearer ' . $token,
                                'Accept' => 'application/json'
                            ]
                        ]
                    );
                    
                    $authStatusCode = $authResponse->getStatusCode();
                    echo "  Request: GET " . (isset($testConfig['base_uri']) ? $testConfig['base_uri'] . '/candidates' : $baseUrl . '/candidates') . "\n";
                    echo "  Status Code: {$authStatusCode}\n";
                    
                    if ($authStatusCode === 200) {
                        echo colorize("  ✓ Token works! Authenticated request successful\n", 'green');
                    } else {
                        $authBody = $authResponse->getBody()->getContents();
                        echo colorize("  ⚠ Token obtained but request failed\n", 'yellow');
                        echo "  Response: " . substr($authBody, 0, 200) . "\n";
                    }
                } catch (Exception $e) {
                    echo colorize("  ✗ Error using token: " . $e->getMessage() . "\n", 'red');
                }
                
                echo "\n";
                echo colorize("=================================================\n", 'green');
                echo colorize("  SUCCESS! Use this configuration:\n", 'green');
                echo colorize("  Config: {$configName}\n", 'green');
                echo colorize("=================================================\n", 'green');
                exit(0);
            } else {
                echo colorize("    ✗ No token in response\n", 'red');
            }
        } else {
            echo colorize("    ✗ Authentication failed (HTTP {$statusCode})\n", 'red');
        }
        
    } catch (Exception $e) {
        echo colorize("    ✗ Exception: " . $e->getMessage() . "\n", 'red');
    }
}

echo "\n";
echo colorize("=================================================\n", 'red');
echo colorize("  All authentication attempts failed\n", 'red');
echo colorize("=================================================\n", 'red');
echo "\n";

// Provide curl command for comparison
echo colorize("For comparison, try this curl command:\n", 'yellow');
echo "\n";
echo "curl -X POST '{$baseUrl}/login' \\\n";
echo "  -H 'Content-Type: application/json' \\\n";
echo "  -d '{\"username\":\"{$username}\",\"password\":\"{$password}\"}' \\\n";
echo "  -v\n";
echo "\n";

exit(1);
