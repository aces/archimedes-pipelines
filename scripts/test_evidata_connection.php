#!/usr/bin/env php
<?php
declare(strict_types=1);

/**
 * EviData connection smoke test.
 *
 * Runs the full auth + API handshake without uploading any CSV or
 * generating a report. Useful after Andy provisions the VM and you've
 * updated config/evidata_config.json + env vars — confirms each leg
 * of the connection independently so a failure points at exactly
 * which piece is wrong.
 *
 * Reads ONLY config/evidata_config.json. The LORIS API config in
 * loris_client_config.json isn't relevant here; this script tests
 * the EviData service, not the pipeline as a whole.
 *
 * Tests, in order:
 *   1. Config loaded and required fields present
 *   2. Required env vars are set
 *   3. TCP reachability to api_base_url   (port 8000)
 *   4. TCP reachability to token_url       (port 3000)
 *   5. /api/health endpoint responds       (no auth)
 *   6. Keycloak token endpoint issues a bearer token
 *   7. /api/auth/me works with that token  (auth round-trip)
 *
 * Usage:
 *   php scripts/test_evidata_connection.php
 *   php scripts/test_evidata_connection.php --config /alt/path/evidata_config.json
 *
 * Exits 0 on full success, 1 on any failure.
 */

require_once __DIR__ . '/../vendor/autoload.php';

// ── Args ─────────────────────────────────────────────────────────────
// Default path mirrors run_clinical_pipeline.php's evidata config
// location so both scripts read the same file.
$configPath = __DIR__ . '/../config/evidata_config.json';
foreach ($argv as $i => $arg) {
    if ($arg === '--config' && isset($argv[$i + 1])) {
        $configPath = $argv[$i + 1];
    }
}

echo "── EviData connection test ──\n";
echo "Time         : " . date('Y-m-d H:i:s T') . "\n";
echo "Host         : " . gethostname() . "\n";
echo "Config file  : {$configPath}\n";
echo str_repeat('─', 64) . "\n";

$passCount = 0;
$failCount = 0;

/** Pretty pass/fail line printer. */
$check = function (string $label, bool $ok, string $detail = '') use (&$passCount, &$failCount): bool {
    $mark = $ok ? '✓' : '✗';
    echo " {$mark} {$label}";
    if ($detail !== '') {
        echo " — {$detail}";
    }
    echo "\n";
    if ($ok) { $passCount++; } else { $failCount++; }
    return $ok;
};

// ── Test 1: load config ──────────────────────────────────────────────
echo "\n[1] Load config\n";
if (!is_file($configPath)) {
    $check('config file exists', false, "not found: {$configPath}");
    echo "\nCreate config/evidata_config.json from the template, "
        . "or pass --config /path/to/file to point at a different location.\n";
    exit(1);
}
$evi = json_decode(file_get_contents($configPath), true);
if ($evi === null) {
    $check('config is valid JSON', false, json_last_error_msg());
    exit(1);
}
$check('config file exists and parses', true);

if (empty($evi['enabled'])) {
    $check('evidata.enabled', false,
        "set to false — preflight is disabled in the pipeline, but we'll still test the connection");
} else {
    $check('evidata.enabled', true);
}

// Required keys for connecting.
$required = [
    'api_base_url', 'token_url', 'client_id',
    'client_secret_env', 'username_env', 'password_env', 'qis',
];
$missing = array_filter($required, fn($k) => empty($evi[$k]));
if (!empty($missing)) {
    $check('required config keys present', false,
        'missing: ' . implode(', ', $missing));
    exit(1);
}
$check('required config keys present', true);

echo "     api_base_url : {$evi['api_base_url']}\n";
echo "     token_url    : {$evi['token_url']}\n";
echo "     client_id    : {$evi['client_id']}\n";
echo "     qis          : [" . implode(', ', $evi['qis']) . "]\n";

// ── Test 2: env vars ─────────────────────────────────────────────────
echo "\n[2] Environment variables\n";

$secretEnv = $evi['client_secret_env'];
$userEnv   = $evi['username_env'];
$passEnv   = $evi['password_env'];

$secret = getenv($secretEnv);
$user   = getenv($userEnv);
$pass   = getenv($passEnv);

$check("{$secretEnv} is set", !empty($secret),
    !empty($secret) ? "len=" . strlen($secret) : "missing — `export {$secretEnv}=...`");
$check("{$userEnv} is set",   !empty($user),
    !empty($user)   ? "value={$user}"          : "missing — `export {$userEnv}=...`");
$check("{$passEnv} is set",   !empty($pass),
    !empty($pass)   ? "len=" . strlen($pass)   : "missing — `export {$passEnv}=...`");

if (empty($secret) || empty($user) || empty($pass)) {
    echo "\nCannot continue without credentials. Set the missing env vars and re-run.\n";
    exit(1);
}

// ── Test 3 & 4: TCP reachability ─────────────────────────────────────
echo "\n[3] TCP reachability\n";

/** Parse host + port from a URL. */
$parseEndpoint = function (string $url): array {
    $p = parse_url($url);
    $scheme = $p['scheme'] ?? 'http';
    return [
        'host' => $p['host'] ?? '',
        'port' => $p['port'] ?? ($scheme === 'https' ? 443 : 80),
    ];
};

/** Try a TCP connection with a short timeout. */
$tcpProbe = function (string $host, int $port, int $timeoutSec = 5): array {
    $errNo  = 0;
    $errStr = '';
    $sock   = @fsockopen($host, $port, $errNo, $errStr, $timeoutSec);
    if ($sock === false) {
        return [false, "{$errStr} (errno {$errNo})"];
    }
    fclose($sock);
    return [true, "open"];
};

$apiEp = $parseEndpoint($evi['api_base_url']);
[$apiOk, $apiMsg] = $tcpProbe($apiEp['host'], $apiEp['port']);
$check("TCP to {$apiEp['host']}:{$apiEp['port']} (API)", $apiOk, $apiMsg);

$tokEp = $parseEndpoint($evi['token_url']);
[$tokOk, $tokMsg] = $tcpProbe($tokEp['host'], $tokEp['port']);
$check("TCP to {$tokEp['host']}:{$tokEp['port']} (Keycloak)", $tokOk, $tokMsg);

if (!$apiOk || !$tokOk) {
    echo "\nNetwork path is broken. Check:\n";
    echo "  - Is the EviData VM up?\n";
    echo "  - Is this host whitelisted on the EviData firewall for the failing port?\n";
    echo "  - Does DNS resolve the hostname correctly? Try: getent hosts {$apiEp['host']}\n";
    exit(1);
}

// ── Test 5: /api/health (no auth) ────────────────────────────────────
echo "\n[4] /api/health (unauthenticated)\n";

$apiBase   = rtrim($evi['api_base_url'], '/');
$healthUrl = "{$apiBase}/health";

$ch = curl_init($healthUrl);
curl_setopt_array($ch, [
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_TIMEOUT        => 15,
    CURLOPT_CONNECTTIMEOUT => 5,
]);
$healthBody = curl_exec($ch);
$healthCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
$healthErr  = curl_error($ch);
curl_close($ch);

if ($healthBody === false) {
    $check('GET /api/health responds', false, "cURL error: {$healthErr}");
    exit(1);
}

$check('GET /api/health responds', $healthCode === 200, "HTTP {$healthCode}");
if ($healthCode === 200) {
    $h = json_decode($healthBody, true);
    if (is_array($h)) {
        $status     = $h['status']              ?? '(missing)';
        $keycloakOk = $h['keycloak_configured'] ?? false;
        $check("health status=healthy",         $status === 'healthy', "got '{$status}'");
        $check("health keycloak_configured",    (bool)$keycloakOk,     $keycloakOk ? 'true' : 'false');
    }
}

// ── Test 6: Keycloak token ──────────────────────────────────────────
echo "\n[5] Keycloak token endpoint\n";

$ch = curl_init($evi['token_url']);
curl_setopt_array($ch, [
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_POST           => true,
    CURLOPT_POSTFIELDS     => http_build_query([
        'grant_type'    => 'password',
        'client_id'     => $evi['client_id'],
        'client_secret' => $secret,
        'username'      => $user,
        'password'      => $pass,
    ]),
    CURLOPT_HTTPHEADER     => ['Content-Type: application/x-www-form-urlencoded'],
    CURLOPT_TIMEOUT        => 30,
]);
$tokBody = curl_exec($ch);
$tokCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
$tokErr  = curl_error($ch);
curl_close($ch);

if ($tokBody === false) {
    $check('token request returns', false, "cURL error: {$tokErr}");
    exit(1);
}

if ($tokCode !== 200) {
    $excerpt = strlen($tokBody) > 300 ? substr($tokBody, 0, 300) . '…' : $tokBody;
    $check('token request HTTP 200', false, "HTTP {$tokCode}: {$excerpt}");
    echo "\nCommon causes:\n";
    echo "  - Wrong client_secret (check APP_KEYCLOAK_CLIENT_SECRET on EviData server)\n";
    echo "  - Wrong username/password\n";
    echo "  - User not yet created in Keycloak (ask Jefferson to add you)\n";
    echo "  - client_id mismatch (config says '{$evi['client_id']}')\n";
    exit(1);
}
$check('token request HTTP 200', true);

$tokData = json_decode($tokBody, true);
$token   = $tokData['access_token'] ?? null;
if (empty($token)) {
    $check('access_token in response', false, 'field missing');
    exit(1);
}
$check('access_token in response', true, 'len=' . strlen($token));

// Decode JWT payload for sanity (no signature check — just shows what
// we got). Format: header.payload.signature, all base64url-encoded.
$jwtParts = explode('.', $token);
if (count($jwtParts) === 3) {
    $payload = json_decode(
        base64_decode(strtr($jwtParts[1], '-_', '+/')),
        true
    );
    if (is_array($payload)) {
        $exp = isset($payload['exp']) ? date('H:i:s', $payload['exp']) : '?';
        echo "     token user   : " . ($payload['preferred_username'] ?? '?') . "\n";
        echo "     token expires: {$exp}\n";
    }
}

// ── Test 7: /api/auth/me (authenticated round-trip) ─────────────────
echo "\n[6] /api/auth/me (authenticated)\n";

$ch = curl_init("{$apiBase}/auth/me");
curl_setopt_array($ch, [
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_HTTPHEADER     => ["Authorization: Bearer {$token}"],
    CURLOPT_TIMEOUT        => 15,
]);
$meBody = curl_exec($ch);
$meCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
$meErr  = curl_error($ch);
curl_close($ch);

if ($meBody === false) {
    $check('GET /api/auth/me responds', false, "cURL error: {$meErr}");
    exit(1);
}

$check('GET /api/auth/me HTTP 200', $meCode === 200, "HTTP {$meCode}");
if ($meCode === 200) {
    $me = json_decode($meBody, true);
    if (is_array($me)) {
        echo "     authenticated as : " . ($me['username']       ?? '?') . "\n";
        echo "     email            : " . ($me['email']          ?? '?') . "\n";
        echo "     email_verified   : " . (($me['email_verified'] ?? false) ? 'yes' : 'no') . "\n";
    }
}

// ── Summary ─────────────────────────────────────────────────────────
echo "\n" . str_repeat('═', 64) . "\n";
echo " Passed: {$passCount}   Failed: {$failCount}\n";
echo str_repeat('═', 64) . "\n";

if ($failCount === 0) {
    echo "\n ✓ EviData connection is fully operational from this host.\n";
    echo "   You can now flip evidata.enabled=true in config/evidata_config.json\n";
    echo "   and run the pipeline.\n\n";
    exit(0);
}

echo "\n ✗ Connection has " . $failCount . " failure(s). Address the items marked ✗ above.\n\n";
exit(1);
