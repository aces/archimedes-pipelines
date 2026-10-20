#!/usr/bin/env php
<?php
/**
 * Debug the LORIS API login response.
 *
 * Host, credentials and API version come from config/loris_client_config.json
 * (the "api" block, or "loris"). Nothing is hard-coded. The password is never
 * printed.
 *
 * Usage:
 *   php scripts/test/debug_response.php
 *   php scripts/test/debug_response.php --config=/path/loris_client_config.json
 *   php scripts/test/debug_response.php --api-version=v0.0.3
 */

if (!function_exists('curl_init')) {
    fwrite(STDERR, "PHP curl extension missing: apt install php-curl\n");
    exit(1);
}

$opts = getopt('', ['config:', 'api-version:', 'help']);
if (isset($opts['help'])) {
    echo "Usage: php scripts/test/debug_response.php [--config=FILE] [--api-version=VER]\n";
    exit(0);
}

$configFile = $opts['config'] ?? __DIR__ . '/../../config/loris_client_config.json';
if (!is_file($configFile)) {
    fwrite(STDERR, "Config not found: {$configFile}\n");
    exit(1);
}
$cfg  = json_decode((string) file_get_contents($configFile), true) ?: [];
$api  = $cfg['api'] ?? $cfg['loris'] ?? [];

$host     = rtrim((string) ($api['base_url'] ?? ''), '/');
$username = (string) ($api['username'] ?? '');
$password = (string) ($api['password'] ?? '');
$version  = (string) ($opts['api-version'] ?? $api['api_version'] ?? 'v0.0.4-dev');
$verify   = (bool) ($cfg['verify_ssl'] ?? true);

if ($host === '' || $username === '' || $password === '') {
    fwrite(STDERR, "api.base_url, api.username and api.password must be set in {$configFile}\n");
    exit(1);
}

$apiRoot = "{$host}/api/{$version}";

echo "\n========================================\n";
echo "DEBUG: LORIS API login\n";
echo "========================================\n";
echo "Config     : {$configFile}\n";
echo "Host       : {$host}\n";
echo "API root   : {$apiRoot}\n";
echo "User       : {$username}\n";
echo "Verify SSL : " . ($verify ? 'yes' : 'no') . "\n\n";

/**
 * POST JSON and return [status, content-type, body, error].
 */
function post(string $url, array $json, bool $verify): array
{
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_POST           => true,
        CURLOPT_POSTFIELDS     => json_encode($json),
        CURLOPT_HTTPHEADER     => ['Content-Type: application/json', 'Accept: application/json'],
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT        => 30,
        CURLOPT_SSL_VERIFYPEER => $verify,
        CURLOPT_SSL_VERIFYHOST => $verify ? 2 : 0,
    ]);
    $body  = curl_exec($ch);
    $error = $body === false ? curl_error($ch) : null;
    $out   = [
        (int) curl_getinfo($ch, CURLINFO_HTTP_CODE),
        (string) curl_getinfo($ch, CURLINFO_CONTENT_TYPE),
        $body === false ? '' : (string) $body,
        $error,
    ];
    curl_close($ch);
    return $out;
}

// 1. Host reachable at all?
echo "1. Host reachable\n----------------------------\n";
$ch = curl_init($host . '/');
curl_setopt_array($ch, [
    CURLOPT_NOBODY => true, CURLOPT_RETURNTRANSFER => true, CURLOPT_TIMEOUT => 15,
    CURLOPT_SSL_VERIFYPEER => $verify, CURLOPT_SSL_VERIFYHOST => $verify ? 2 : 0,
]);
curl_exec($ch);
$err = curl_error($ch);
echo $err !== ''
    ? "  ✗ {$err}\n\n"
    : '  HTTP ' . curl_getinfo($ch, CURLINFO_HTTP_CODE) . "\n\n";
curl_close($ch);

// 2. Login at the configured API root, full detail.
echo "2. POST {$apiRoot}/login\n----------------------------\n";
[$status, $type, $body, $error] = post("{$apiRoot}/login", ['username' => $username, 'password' => $password], $verify);

if ($error !== null) {
    echo "  ✗ {$error}\n";
} else {
    echo "  Status       : {$status}\n";
    echo "  Content-Type : {$type}\n";
    echo "  Body length  : " . strlen($body) . " bytes\n";

    $decoded = json_decode($body, true);
    if (stripos($body, '<html') !== false || stripos($body, '<!DOCTYPE') !== false) {
        echo "  ⚠ HTML, not JSON: wrong URL, missing endpoint, or a redirect to a login page\n";
        echo "  First 200 chars: " . substr($body, 0, 200) . "\n";
    } elseif (is_array($decoded)) {
        echo "  ✓ JSON, keys: " . implode(', ', array_keys($decoded)) . "\n";
        echo isset($decoded['token'])
            ? "  ✓ token received (" . strlen((string) $decoded['token']) . " chars, not shown)\n"
            : "  ✗ no token in response: " . substr($body, 0, 200) . "\n";
    } else {
        echo "  ⚠ Neither HTML nor JSON. First 200 chars: " . substr($body, 0, 200) . "\n";
    }
}

// 3. Other API versions on the same host, to spot a version mismatch.
echo "\n3. Other API versions on {$host}\n----------------------------\n";
$tried = [];
foreach ([$version, 'v0.0.4-dev', 'v0.0.3'] as $v) {
    if (isset($tried[$v])) {
        continue;
    }
    $tried[$v] = true;
    [$s, , $b, $e] = post("{$host}/api/{$v}/login", ['username' => $username, 'password' => $password], $verify);
    $ok = $e === null && $s === 200 && str_contains($b, 'token');
    printf("  %-12s %s\n", $v, $e !== null ? "✗ {$e}" : "HTTP {$s}" . ($ok ? '  ✓ works' : ''));
}

echo "\n========================================\n";
echo "Next steps if login failed:\n";
echo "  - Check api.base_url (LORIS root, no /api suffix) and api.api_version in {$configFile}\n";
echo "  - LORIS Apache error log, e.g. sudo tail -f /var/log/apache2/loris-error.log\n";
echo "  - Browser: {$apiRoot}/projects\n\n";
