<?php

declare(strict_types=1);

namespace LORIS\Pipelines\Shared;

/**
 * Command-line helpers shared by the BIDS and DICOM scripts.
 *
 * @category Pipelines
 * @package  LORIS\Pipelines\Shared
 */
final class Cli
{
    /**
     * Exit 2 when the command line has an option the script does not declare.
     *
     * getopt() silently ignores options it does not know. A typo, or an option
     * that was removed (--no-import, --stop-after, ...), would then run with
     * the default instead - with --confirm, a real import. Call this right
     * after getopt() with the same short and long option lists.
     *
     * Accepted forms: --name, --name=value, --name value (for "name:"), -v,
     * and anything after a bare "--". A stray word that is not an option
     * value is reported too.
     *
     * @param string   $short Short options, as given to getopt().
     * @param string[] $long  Long options, as given to getopt().
     * @param string[] $argv  Arguments including the script name; defaults
     *                        to $_SERVER['argv'].
     *
     * @return void
     */
    public static function exitOnUnknownOptions(string $short, array $long, ?array $argv = null): void
    {
        $problems = self::unknownOptions($short, $long, $argv ?? ($_SERVER['argv'] ?? []));

        if (empty($problems)) {
            return;
        }

        foreach ($problems as $problem) {
            fwrite(STDERR, $problem . "\n");
        }
        fwrite(STDERR, "Run with --help for the options.\n");
        exit(2);
    }

    /**
     * The problems exitOnUnknownOptions() would report, one line each.
     *
     * @param string   $short Short options, as given to getopt().
     * @param string[] $long  Long options, as given to getopt().
     * @param string[] $argv  Arguments including the script name.
     *
     * @return string[]
     */
    public static function unknownOptions(string $short, array $long, array $argv): array
    {
        $takesValue = [];
        foreach ($long as $opt) {
            $name              = rtrim($opt, ':');
            $takesValue[$name] = substr($opt, -1) === ':' && substr($opt, -2) !== '::';
        }

        $shortValue = [];
        $len        = strlen($short);
        for ($i = 0; $i < $len; $i++) {
            if ($short[$i] === ':') {
                continue;
            }
            $shortValue[$short[$i]] = ($short[$i + 1] ?? '') === ':' && ($short[$i + 2] ?? '') !== ':';
        }

        $problems = [];
        $args     = array_slice(array_values($argv), 1);
        $count    = count($args);

        for ($i = 0; $i < $count; $i++) {
            $arg = (string) $args[$i];

            if ($arg === '--') {
                break;
            }

            if (strncmp($arg, '--', 2) === 0) {
                $name = substr($arg, 2);
                $eq   = strpos($name, '=');
                if ($eq !== false) {
                    $name = substr($name, 0, $eq);
                }
                if (!array_key_exists($name, $takesValue)) {
                    $problems[] = "Unknown option: --{$name}";
                } elseif ($eq === false && $takesValue[$name]) {
                    $i++;   // --name value
                }
                continue;
            }

            if ($arg !== '-' && $arg[0] === '-') {
                $letters = substr($arg, 1);
                $n       = strlen($letters);
                for ($j = 0; $j < $n; $j++) {
                    $letter = $letters[$j];
                    if (!array_key_exists($letter, $shortValue)) {
                        $problems[] = "Unknown option: -{$letter}";
                        break;
                    }
                    if ($shortValue[$letter]) {
                        if ($j === $n - 1) {
                            $i++;   // -x value
                        }
                        break;      // -xvalue
                    }
                }
                continue;
            }

            $problems[] = "Unexpected argument: {$arg}";
        }

        return $problems;
    }
}
