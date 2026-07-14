#!/usr/bin/env php
<?php
declare(strict_types=1);

/**
 * EviData connection smoke test.
 *
 * Runs the full auth + API handshake without uploading any CSV or
 * generating a report. Useful after the EviData VM is provisioned and
 * you've updated config/evidata_config.json + env vars — confirms each
 * leg of the connection independently so a failure points at exactly
 * which piece is wrong.
 *
 * Reads ONLY config/evidata_config.json. The LORIS API config in
 * loris_client_config.json isn't relevant here; this script tests
 * the EviData service, not the pipeline as a whole.
 *
 * Tests, in order:
 *   1. Config loaded and required fields present
 *   2. Required env vars are set
 *   3. TCP reachability to api_base_url host
 *   4. TCP reachability to token_url host
 *   5. /api/health endpoint responds       (no auth)
 *   6. Keycloak token endpoint issues a bearer token (with openid scope)
 *   7. /api/datasets works with that token  (authenticated round-trip)
 *   8. /api/auth/login + /api/auth/me session round-trip
 *
 * Note on the token scope: the EviData API rejects bearer tokens
 * issued without the 'openid' scope (HTTP 401 "Failed to authenticate
 * user"), even though Keycloak itself returns the token with HTTP 200.
 * Both this script and EviDataClient request 'openid profile email'.
 *
 * Credentials: this script auto-loads the EviData env file (same as
 * run_clinical_pipeline.php) so a forgotten `source` will not cause a
 * spurious failure. Real environment variables, if already set, win.
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

// ── Auto-load EviData credentials from the env file ─────────────────
// Path resolution order (most specific wins):
//   1. EVIDATA_ENV_FILE environment variable
//   2. "env_file" key in the evidata config file ($configPath)
//   3. hardcoded default
// SECURITY: the env file holds secrets and must stay chmod 600 and
// gitignored. Only the PATH is in config/code. Real env vars always
// win — the file never overrides an already-set variable.
$evidataEnvFile = (function (string $configPath): string {
    $path = getenv('EVIDATA_ENV_FILE') ?: null;

    if ($path === null && is_readable($configPath)) {
        $cfg  = json_decode(file_get_contents($configPath), true);
        $path = (is_array($cfg) && !empty($cfg['env_file']))
            ? $cfg['env_file']
            : null;
    }

    $path = $path ?: '/home/lorisadmin/evidata/evidata.env';

    if (is_readable($path)) {
        foreach (file($path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) as $line) {
            $line = trim($line);
            if ($line === '' || $line[0] === '#') {
                continue;
            }
            $line = preg_replace('/^export\s+/', '', $line);
            if (!str_contains($line, '=')) {
                continue;
            }
            [$k, $v] = explode('=', $line, 2);
            $k = trim($k);
            $v = trim($v, " \t\n\r\0\x0B\"'");
            if ($k !== '' && getenv($k) === false) {
                putenv("{$k}={$v}");
            }
        }
    }

    return $path;
})($configPath);

// OAuth2 scope requested in the token grant. Must include 'openid' —
// see the note in the file header. Overridable via the optional
// 'scope' key in evidata_config.json.
$defaultScope = 'openid profile email';

echo "── EviData connection test ──\n";
echo "Time         : " . date('Y-m-d H:i:s T') . "\n";
echo "Host         : " . gethostname() . "\n";
echo "Config file  : {$configPath}\n";
echo "Env file     : " . (is_readable($evidataEnvFile) ? $evidataEnvFile : "(not found — using shell env)") . "\n";
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

// Keys required to CONNECT. 'qis' is deliberately NOT required: an
// empty/absent qis means ALL-HEADERS mode in ClinicalPipeline, and this
// script never uploads a dataset, so it needs no QI list at all.
$required = [
    'api_base_url', 'token_url', 'client_id',
    'client_secret_env', 'username_env', 'password_env',
];
$missing = array_filter($required, fn($k) => empty($evi[$k]));
if (!empty($missing)) {
    $check('required config keys present', false,
        'missing: ' . implode(', ', $missing));
    exit(1);
}
$check('required config keys present', true);

$scope = $evi['scope'] ?? $defaultScope;

echo "     api_base_url : {$evi['api_base_url']}\n";
echo "     token_url    : {$evi['token_url']}\n";
echo "     client_id    : {$evi['client_id']}\n";
// qis may be absent (all-headers mode), a flat list, or a per-file map.
$qis = $evi['qis'] ?? [];
if (is_array($qis) && $qis !== []) {
    $isFlatList = array_keys($qis) === range(0, count($qis) - 1);
    echo "     qis          : " . ($isFlatList
            ? '[' . implode(', ', $qis) . ']'
            : 'per-file map (' . count($qis) . ' entries)') . "\n";
} else {
    echo "     qis          : (none — pipeline will use ALL HEADERS mode)\n";
}
echo "     scope        : {$scope}\n";

// ── Test 2: env vars ─────────────────────────────────────────────────
echo "\n[2] Environment variables\n";

$secretEnv = $evi['client_secret_env'];
$userEnv   = $evi['username_env'];
$passEnv   = $evi['password_env'];

$secret = getenv($secretEnv);
$user   = getenv($userEnv);
$pass   = getenv($passEnv);

$check("{$secretEnv} is set", !empty($secret),
    !empty($secret) ? "len=" . strlen($secret) : "missing — set it in {$evidataEnvFile} or `export {$secretEnv}=...`");
$check("{$userEnv} is set",   !empty($user),
    !empty($user)   ? "value={$user}"          : "missing — set it in {$evidataEnvFile} or `export {$userEnv}=...`");
$check("{$passEnv} is set",   !empty($pass),
    !empty($pass)   ? "len=" . strlen($pass)   : "missing — set it in {$evidataEnvFile} or `export {$passEnv}=...`");

if (empty($secret) || empty($user) || empty($pass)) {
    echo "\nCannot continue without credentials. Set the missing values in\n";
    echo "{$evidataEnvFile} (or export them) and re-run.\n";
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
        // Required: without the openid scope the issued token is
        // rejected by the EviData API with HTTP 401.
        'scope'         => $scope,
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
    $tokJson = json_decode($tokBody, true);
    $errCode = is_array($tokJson) ? ($tokJson['error'] ?? '') : '';

    // Keycloak reports bad credentials as 400 invalid_grant (wrong
    // username/password) or 401 invalid_client (wrong client_id/secret).
    // Say "authentication failed" outright rather than leaving an
    // HTTP code to be decoded.
    $isAuthFailure = in_array($tokCode, [400, 401], true)
        && in_array($errCode, ['invalid_grant', 'invalid_client', 'unauthorized_client'], true);

    if ($isAuthFailure) {
        $check('AUTHENTICATION', false,
            "FAILED — Keycloak rejected the credentials (HTTP {$tokCode} {$errCode})");
        echo "\n ✗ AUTHENTICATION FAILED — the credentials are wrong.\n\n";
        if ($errCode === 'invalid_grant') {
            echo "  Wrong username or password:\n";
            echo "    - check {$userEnv} / {$passEnv} in {$evidataEnvFile}\n";
            echo "    - or the user does not exist in Keycloak (ask Jefferson to add you)\n";
        } else {
            echo "  Wrong client credentials:\n";
            echo "    - {$secretEnv} must match APP_API_CLIENT_ID_SECRET on the EviData server\n";
            echo "    - client_id in the config is '{$evi['client_id']}'\n";
        }
        echo "\n";
        exit(1);
    }

    $check('token request HTTP 200', false, "HTTP {$tokCode}: {$excerpt}");
    echo "\nCommon causes:\n";
    echo "  - Wrong client_secret (check APP_API_CLIENT_ID_SECRET on EviData server)\n";
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
        echo "     token scope  : " . ($payload['scope'] ?? '?') . "\n";
        echo "     token expires: {$exp}\n";
    }
}

// ── Test 7: /api/datasets (authenticated round-trip) ─────────────────
// /api/datasets is a real programmatic endpoint that accepts the
// bearer token — unlike /api/auth/me, which is a browser/cookie helper
// and rejects bearer tokens. This is the honest "can I make an
// authenticated API call" check.
echo "\n[6] /api/datasets (authenticated)\n";

$ch = curl_init("{$apiBase}/datasets");
curl_setopt_array($ch, [
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_HTTPHEADER     => ["Authorization: Bearer {$token}"],
    CURLOPT_TIMEOUT        => 15,
]);
$dsBody = curl_exec($ch);
$dsCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
$dsErr  = curl_error($ch);
curl_close($ch);

if ($dsBody === false) {
    $check('GET /api/datasets responds', false, "cURL error: {$dsErr}");
    exit(1);
}

$check('GET /api/datasets HTTP 200', $dsCode === 200, "HTTP {$dsCode}");
if ($dsCode === 200) {
    $ds = json_decode($dsBody, true);
    if (is_array($ds)) {
        $total = $ds['total'] ?? (isset($ds['datasets']) ? count($ds['datasets']) : '?');
        echo "     datasets visible : {$total}\n";
    }
} else {
    $excerpt = strlen($dsBody) > 300 ? substr($dsBody, 0, 300) . '…' : $dsBody;
    echo "     response: {$excerpt}\n";
}

// ── Test 8: EviData session auth (/api/auth/login + /api/auth/me) ───
// Tests 6-7 cover programmatic bearer-token access. This exercises
// the app's browser/cookie login instead — /api/auth/me only accepts
// the session cookie, so this is the one way to smoke-test it.
echo "\n[7] Session auth (/api/auth/login + /api/auth/me)\n";

$ch = curl_init("{$apiBase}/auth/login");
curl_setopt_array($ch, [
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_POST           => true,
    CURLOPT_POSTFIELDS     => json_encode(['username' => $user, 'password' => $pass]),
    CURLOPT_HTTPHEADER     => ['Content-Type: application/json'],
    CURLOPT_COOKIEFILE     => '',   // enable in-memory cookie engine
    CURLOPT_TIMEOUT        => 15,
]);
$loginBody = curl_exec($ch);
$loginCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
$loginErr  = curl_error($ch);

// Some builds expose login as form-encoded rather than JSON — retry
// once before declaring failure.
if ($loginBody !== false && in_array($loginCode, [400, 415, 422], true)) {
    curl_setopt_array($ch, [
        CURLOPT_URL        => "{$apiBase}/auth/login",
        CURLOPT_POSTFIELDS => http_build_query(['username' => $user, 'password' => $pass]),
        CURLOPT_HTTPHEADER => ['Content-Type: application/x-www-form-urlencoded'],
    ]);
    $loginBody = curl_exec($ch);
    $loginCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $loginErr  = curl_error($ch);
}

if ($loginBody === false) {
    $check('POST /api/auth/login responds', false, "cURL error: {$loginErr}");
    curl_close($ch);
} else {
    if (in_array($loginCode, [401, 403], true)) {
        // Same wording as the token test: a rejected login is an
        // authentication failure, not a generic bad status code.
        $check('POST /api/auth/login accepts credentials', false,
            "AUTHENTICATION FAILED — session login rejected the credentials (HTTP {$loginCode})");
    } else {
        $check('POST /api/auth/login HTTP 200', $loginCode === 200, "HTTP {$loginCode}");
    }

    if ($loginCode === 200) {
        // Reuse the same handle so the session cookie carries over.
        curl_setopt_array($ch, [
            CURLOPT_URL        => "{$apiBase}/auth/me",
            CURLOPT_HTTPGET    => true,
            CURLOPT_HTTPHEADER => [],
        ]);
        $meBody = curl_exec($ch);
        $meCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $meErr  = curl_error($ch);

        if ($meBody === false) {
            $check('GET /api/auth/me responds', false, "cURL error: {$meErr}");
        } else {
            $check('GET /api/auth/me HTTP 200', $meCode === 200, "HTTP {$meCode}");
            if ($meCode === 200) {
                $me = json_decode($meBody, true);
                if (is_array($me)) {
                    $who = $me['username']
                        ?? $me['preferred_username']
                        ?? $me['email']
                        ?? '?';
                    $check('session identity matches login user',
                        $who === $user, "server says '{$who}'");
                }
            } else {
                $excerpt = strlen($meBody) > 300 ? substr($meBody, 0, 300) . '…' : $meBody;
                echo "     response: {$excerpt}\n";
            }
        }
    } else {
        $excerpt = strlen($loginBody) > 300 ? substr($loginBody, 0, 300) . '…' : $loginBody;
        echo "     response: {$excerpt}\n";
    }
    curl_close($ch);
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