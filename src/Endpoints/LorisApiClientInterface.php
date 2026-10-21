<?php
/**
 * LorisApiClientInterface
 *
 * The LORIS operations a reidentification stage needs: resolve or create a
 * candidate from a site identifier, list the project's configured visit
 * labels, and ensure a session exists.
 *
 * Kept in its own file so Composer's PSR-4 autoloader can find it, and so any
 * reidentifier (DICOM today; BIDS, PNG, ... next) can depend on the same
 * contract and reuse LorisApiClientAdapter.
 *
 * PHP Version 8.1
 *
 * @category Pipeline
 * @package  Archimedes
 */

namespace LORIS\Endpoints;

/**
 * The LORIS operations a reidentification stage needs.
 *
 * Implement this as an adapter over the existing ARCHIMEDES API client rather
 * than changing that client to match.
 */
interface LorisApiClientInterface
{
    /**
     * Exception code of the RuntimeException ensureSession() throws when LORIS
     * refuses the visit for the candidate's cohort ("This visit label is not
     * valid for this cohort."): a data problem for that visit only, so the
     * caller holds back its studies and carries on with the rest.
     */
    public const VISIT_REFUSED = 4001;

    /**
     * Find a candidate by the site's own identifier.
     *
     * @param string $externalId Site identifier (the sub- value).
     *
     * @return array{pscid: string, cand_id: string}|null Null when not found.
     */
    public function findCandidateByExternalId(string $externalId): ?array;

    /**
     * Create a candidate.
     *
     * @param string      $pscid      PSCID to assign.
     * @param string      $externalId Site identifier, stored as ExtStudyID.
     * @param string|null $dob        Normalised to YYYY-MM-DD before sending
     *                                (day kept if given, else 01).
     * @param string|null $sex        Male, Female or Other.
     *
     * @return array{pscid: string, cand_id: string}
     */
    public function createCandidate(
        string $pscid,
        string $externalId,
        ?string $dob,
        ?string $sex
    ): array;

    /**
     * Visit labels configured for the project.
     *
     * @return string[]
     */
    public function getConfiguredVisitLabels(): array;   // project.json "visits" (the LORIS API cannot list configured visits)

    /**
     * Ensure a session exists for this candidate and visit.
     *
     * @param string      $candId     CandID.
     * @param string      $visitLabel Visit label.
     * @param string|null $cohort     Cohort title (LORIS visit "Cohort"); null =
     *                                project.json candidate_defaults.cohort.
     *
     * @return void
     */
    public function ensureSession(string $candId, string $visitLabel, ?string $cohort = null): void;

    /**
     * Where participants.tsv dob / sex differ from the candidate in LORIS.
     *
     * Read-only: LORIS is never changed. Values are compared after the same
     * normalisation used when creating a candidate.
     *
     * @param string      $candId CandID.
     * @param string|null $dob    participants.tsv dob (raw).
     * @param string|null $sex    participants.tsv sex (raw).
     *
     * @return string[] One line per differing field; empty when they agree or
     *                  LORIS could not be read.
     */
    public function demographicsDiffer(string $candId, ?string $dob, ?string $sex): array;
}