<?php

/**
 * PipelineSetup
 *
 * What every pipeline script does the same way, whatever the modality:
 * strict options (--all | --collection [--project], --confirm, --dry-run,
 * --config, -v, --help; an unknown option stops with exit 2 through
 * Shared\Cli, the option check every script uses), the config,
 * project selection (enabled in the config, or named explicitly, and
 * "Imaging" in project.json - ImagingProject), project.json, the LORIS
 * adapter set up for the project, the declared visits, and the --confirm
 * line printed after a dry run.
 *
 * Used by the PNG scripts (PngCli extends it with the PNG paths); written so
 * BIDS and the next modality can use it as is - a modality adds only what is
 * its own (paths, manifest).
 *
 * PHP Version 8.1
 *
 * @package LORIS\Pipelines
 */

declare(strict_types=1);

namespace LORIS\Pipelines\Shared;

use LORIS\Endpoints\LorisApiClientAdapter;
use Monolog\Handler\RotatingFileHandler;
use Monolog\Handler\StreamHandler;
use Monolog\Level;
use Monolog\Logger;

class PipelineSetup
{
    public const EXIT_OK      = 0;
    public const EXIT_FAILURE = 1;
    public const EXIT_USAGE   = 2;
    public const EXIT_ABORTED = 3;

    public const DEFAULT_CONFIG = __DIR__ . '/../../../config/loris_client_config.json';

    /** Options every pipeline script accepts. */
    public const COMMON_LONG = ['all', 'collection:', 'project:', 'confirm', 'dry-run', 'config:', 'verbose', 'help'];

    /**
     * Parse options strictly (an unknown option stops the script with exit 2,
     * as in every DICOM/BIDS script).
     *
     * @param string[] $extraLong Script-specific long options (getopt syntax).
     *
     * @return array{options: array, confirm: bool, verbose: bool, config_path: string}
     */
    public static function options(array $extraLong, string $usage): array
    {
        $short = 'v';
        $long  = array_merge(self::COMMON_LONG, $extraLong);
        $opts  = getopt($short, $long);
        Cli::exitOnUnknownOptions($short, $long);

        if ($opts === false || isset($opts['help'])) {
            fwrite(isset($opts['help']) ? STDOUT : STDERR, $usage);
            exit(isset($opts['help']) ? self::EXIT_OK : self::EXIT_USAGE);
        }
        if (!isset($opts['all']) && !isset($opts['collection'])) {
            fwrite(STDERR, "Specify --all or --collection=NAME\n\n" . $usage);
            exit(self::EXIT_USAGE);
        }
        if (isset($opts['project']) && !isset($opts['collection'])) {
            fwrite(STDERR, "--project requires --collection\n\n" . $usage);
            exit(self::EXIT_USAGE);
        }
        if (isset($opts['confirm']) && isset($opts['dry-run'])) {
            fwrite(STDERR, "Both --confirm and --dry-run given - treating as dry run.\n\n");
        }

        return [
            'options'     => $opts,
            'confirm'     => isset($opts['confirm']) && !isset($opts['dry-run']),
            'verbose'     => isset($opts['v']) || isset($opts['verbose']),
            'config_path' => (string) ($opts['config'] ?? self::DEFAULT_CONFIG),
        ];
    }

    public static function config(string $path): array
    {
        try {
            return self::readJson($path);
        } catch (\RuntimeException $e) {
            fwrite(STDERR, 'ERROR ' . $e->getMessage() . "\n");
            exit(self::EXIT_USAGE);
        }
    }

    /**
     * A JSON object from a file (config, project.json, a manifest ...).
     *
     * @throws \RuntimeException File missing or unreadable, or not a JSON object.
     */
    public static function readJson(string $path): array
    {
        if (!is_file($path)) {
            throw new \RuntimeException("File not found: {$path}");
        }
        $raw = @file_get_contents($path);
        if ($raw === false) {
            throw new \RuntimeException("Could not read {$path}");
        }
        $data = json_decode($raw, true);
        if (!is_array($data)) {
            throw new \RuntimeException("Invalid JSON in {$path}: " . json_last_error_msg());
        }
        return $data;
    }

    /**
     * logs/<modality>/ of a project, created when missing; null when it cannot be.
     */
    public static function logDir(string $projectPath, string $modality): ?string
    {
        $dir = rtrim($projectPath, '/') . '/logs/' . strtolower($modality);
        return (is_dir($dir) || @mkdir($dir, 0775, true)) ? $dir : null;
    }

    /**
     * Logger of a pipeline step: logs/<modality>/<modality>_<step>.log (daily,
     * 30 days kept, INFO and up) when the folder can be written, and stderr
     * (DEBUG with -v, otherwise WARNING and up).
     */
    public static function logger(string $projectPath, string $modality, string $step, bool $verbose): Logger
    {
        $name   = strtolower($modality) . '_' . $step;
        $logger = new Logger($name);
        $dir    = self::logDir($projectPath, $modality);
        if ($dir !== null) {
            $logger->pushHandler(new RotatingFileHandler("{$dir}/{$name}.log", 30, Level::Info));
        }
        $logger->pushHandler(new StreamHandler('php://stderr', $verbose ? Level::Debug : Level::Warning));
        return $logger;
    }

    /**
     * Projects to process (ImagingProject::targets()); none selected = exit 2.
     *
     * @return array<int, array{collection: string, project: string, path: string}>
     */
    public static function targets(array $config, array $opts): array
    {
        $targets = ImagingProject::targets($config, $opts);

        if ($targets === []) {
            fwrite(STDERR, "No enabled projects matched the selection.\n");
            exit(self::EXIT_USAGE);
        }
        return $targets;
    }

    public static function projectConfig(string $projectPath): ?array
    {
        $file = $projectPath . '/project.json';
        $data = is_file($file) ? json_decode((string) file_get_contents($file), true) : null;
        return is_array($data) ? $data : null;
    }

    /**
     * LORIS adapter set up for a project (site, project, sex mappings).
     */
    public static function api(array $config, array $projectConfig, ?object $logger = null): LorisApiClientAdapter
    {
        $api = new LorisApiClientAdapter($config, $logger);
        $api->setSite($projectConfig['candidate_defaults']['site'] ?? null);
        $api->resolveProject($projectConfig, $config);
        $api->setSexMappings($projectConfig['sex_mappings'] ?? []);
        return $api;
    }

    /**
     * Declared visit labels (project.json "visits"), as the DICOM bundle reads them.
     */
    public static function visitLabels(array $projectConfig): callable
    {
        return static fn (): array => array_values(array_filter(
            array_map(static fn ($v) => trim((string) $v), (array) ($projectConfig['visits'] ?? []))
        ));
    }

    /**
     * The command just run, with --confirm instead of --dry-run.
     */
    public static function confirmCommand(array $argv): string
    {
        $args   = array_values(array_filter(array_slice($argv, 1),
            static fn (string $a): bool => $a !== '--dry-run' && $a !== '--confirm'));
        $args[] = '--confirm';
        return 'php ' . $argv[0] . ' ' . implode(' ', array_map(
            static fn (string $a): string => preg_match('~^[A-Za-z0-9=_./,-]+$~', $a) === 1 ? $a : escapeshellarg($a),
            $args
        ));
    }

    public static function err(string $line): void
    {
        fwrite(STDERR, $line . "\n");
    }
}
