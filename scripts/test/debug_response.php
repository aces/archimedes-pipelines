#!/usr/bin/env php
<?php
/**
 * Debug Authentication Response
 * Shows EXACTLY what the API is returning
 */

echo "\n========================================\n";
echo "DEBUG: Raw API Response\n";
echo "========================================\n\n";

$config = [
    'base_url' => 'https://msruthy-dev.loris.ca/api/v0.0.3',
    'username' => 'admin',
    'password' => 'admin'
];

echo "Testing URL: {$config['base_url']}/login\n\n";

// Test 1: Using curl command line
echo "1. CURL Command Line Test\n";
echo "----------------------------\n";
$curlCmd = sprintf(
    'curl -v -X POST "%s/login" -H "Content-Type: application/json" -d \'{"username":"%s","password":"%s"}\' 2>&1',
    $config['base_url'],
    $config['username'],
    $config['password']
);
echo "Command: $curlCmd\n\n";
$output = shell_exec($curlCmd);
echo "Response:\n";
echo $output;
echo "\n\n";

// Test 2: Check if endpoint exists
echo "2. Testing Base URL\n";
echo "----------------------------\n";
$baseCheck = shell_exec("curl -s -I '{$config['base_url']}' 2>&1");
echo $baseCheck;
echo "\n\n";

// Test 3: PHP with full details
if (file_exists(__DIR__ . '/../vendor/autoload.php')) {
    require __DIR__ . '/../vendor/autoload.php';
    
    use GuzzleHttp\Client;
    
    echo "3. PHP GuzzleHttp with Full Details\n";
    echo "----------------------------\n";
    
    try {
        $client = new Client([
            'base_uri' => $config['base_url'],
            'timeout' => 30,
            'verify' => false,
            'http_errors' => false,
            'headers' => [
                'Accept' => 'application/json',
                'User-Agent' => 'LORIS-PHP-Client/1.0'
            ]
        ]);
        
        echo "Sending POST to /login...\n";
        
        $response = $client->post('/login', [
            'json' => [
                'username' => $config['username'],
                'password' => $config['password']
            ],
            'headers' => [
                'Content-Type' => 'application/json'
            ]
        ]);
        
        $statusCode = $response->getStatusCode();
        $body = $response->getBody()->getContents();
        
        echo "\nStatus Code: $statusCode\n";
        echo "\nResponse Headers:\n";
        foreach ($response->getHeaders() as $name => $values) {
            echo "  $name: " . implode(', ', $values) . "\n";
        }
        
        echo "\nResponse Body (raw):\n";
        echo "Length: " . strlen($body) . " bytes\n";
        echo "---START OF BODY---\n";
        echo $body;
        echo "\n---END OF BODY---\n\n";
        
        // Try to detect what format it is
        echo "Response Analysis:\n";
        if (strpos($body, '<html') !== false || strpos($body, '<!DOCTYPE') !== false) {
            echo "  ⚠ Response appears to be HTML, not JSON!\n";
            echo "  This usually means:\n";
            echo "    - Wrong URL (getting web page instead of API)\n";
            echo "    - API endpoint doesn't exist\n";
            echo "    - Redirect to login page\n";
        } elseif (strpos($body, '{') === 0) {
            echo "  ✓ Response appears to be JSON\n";
            $decoded = json_decode($body, true);
            if (json_last_error() === JSON_ERROR_NONE) {
                echo "  ✓ Valid JSON!\n";
                echo "  Keys: " . implode(', ', array_keys($decoded)) . "\n";
            } else {
                echo "  ✗ Invalid JSON: " . json_last_error_msg() . "\n";
            }
        } else {
            echo "  ⚠ Response is neither HTML nor JSON\n";
            echo "  First 100 chars: " . substr($body, 0, 100) . "\n";
        }
        
        // Test different URL variations
        echo "\n\n4. Testing URL Variations\n";
        echo "----------------------------\n";
        
        $urlVariations = [
            $config['base_url'] . '/login',
            'https://msruthy-dev.loris.ca/api/v0.0.4-dev/login',
            'https://msruthy-dev.loris.ca/api/login',
            rtrim($config['base_url'], '/') . '/login'
        ];
        
        foreach ($urlVariations as $url) {
            echo "\nTrying: $url\n";
            try {
                $testClient = new Client([
                    'timeout' => 10,
                    'verify' => false,
                    'http_errors' => false
                ]);
                
                $testResp = $testClient->post($url, [
                    'json' => [
                        'username' => $config['username'],
                        'password' => $config['password']
                    ],
                    'headers' => ['Content-Type' => 'application/json']
                ]);
                
                $testStatus = $testResp->getStatusCode();
                $testBody = $testResp->getBody()->getContents();
                
                echo "  Status: $testStatus\n";
                echo "  Content-Type: " . $testResp->getHeaderLine('Content-Type') . "\n";
                echo "  First 200 chars: " . substr($testBody, 0, 200) . "\n";
                
                if ($testStatus === 200 && strpos($testBody, 'token') !== false) {
                    echo "  ✓✓✓ THIS URL WORKS! Use: $url\n";
                }
            } catch (Exception $e) {
                echo "  ✗ Error: " . $e->getMessage() . "\n";
            }
        }
        
    } catch (Exception $e) {
        echo "Error: " . $e->getMessage() . "\n";
        echo "Stack trace:\n" . $e->getTraceAsString() . "\n";
    }
} else {
    echo "Skipping PHP test - composer dependencies not installed\n";
}

echo "\n========================================\n";
echo "Diagnosis Complete\n";
echo "========================================\n\n";

echo "Next steps:\n";
echo "1. Check if the API URL is correct\n";
echo "2. Verify the API version (v0.0.3 vs v0.0.4-dev)\n";
echo "3. Check LORIS Apache error logs: sudo tail -f /var/log/apache2/loris-error.log\n";
echo "4. Try accessing the API in browser: {$config['base_url']}/projects\n\n";
