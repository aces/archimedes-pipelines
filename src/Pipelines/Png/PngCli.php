<?php

/**
 * PngCli
 *
 * The PNG scripts' plumbing: everything generic comes from the shared
 * PipelineSetup (options, selection, config, LORIS adapter, --confirm line);
 * this adds only what is PNG's own - the participants.tsv row lookup from the
 * scan manifest, and logs/png/.
 *
 * PHP Version 8.1
 *
 * @package LORIS\Pipelines
 */

declare(strict_types=1);

namespace LORIS\Pipelines\Png;

use LORIS\Pipelines\Shared\PipelineSetup;

final class PngCli extends PipelineSetup
{
    /**
     * participants.tsv row for a subject (enriched), from a scan manifest.
     */
    public static function rowLookup(array $manifest, array $projectConfig): callable
    {
        $roster = null;
        $tsv    = $manifest['participants_tsv'] ?? null;
        if ($tsv !== null && is_file($tsv)) {
            try {
                $roster = \LORIS\Pipelines\Shared\ParticipantsTsv::load(
                    $tsv, $projectConfig['candidate_defaults'] ?? [], $projectConfig);
            } catch (\Throwable $e) {
                $roster = null;
            }
        }
        return static fn (string $subject): ?array => $roster?->get('sub-' . $subject) ?? $roster?->get($subject);
    }
}
