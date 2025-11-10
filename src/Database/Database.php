<?php
declare(strict_types=1);

namespace LORIS\Client\Database;

use PDO;
use PDOException;
use Psr\Log\LoggerInterface;

/**
 * Database Connection and Query Helper
 * Provides direct SQL access when API endpoints don't exist
 * 
 * Usage Priority:
 * 1. Use API endpoints (preferred)
 * 2. Use LORIS PHP library functions
 * 3. Use this class for direct SQL (fallback)
 */
class Database
{
    private ?PDO $connection = null;
    private array $config;
    private LoggerInterface $logger;

    public function __construct(array $config, LoggerInterface $logger)
    {
        $this->config = $config;
        $this->logger = $logger;
    }

    /**
     * Get PDO connection (lazy initialization)
     */
    public function getConnection(): PDO
    {
        if ($this->connection === null) {
            $this->connect();
        }
        return $this->connection;
    }

    /**
     * Establish database connection
     */
    private function connect(): void
    {
        $dbConfig = $this->config['database'];
        
        $dsn = sprintf(
            "mysql:host=%s;dbname=%s;charset=utf8mb4",
            $dbConfig['host'],
            $dbConfig['name']
        );

        try {
            $this->connection = new PDO(
                $dsn,
                $dbConfig['user'],
                $dbConfig['password'],
                [
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                    PDO::ATTR_EMULATE_PREPARES => false,
                ]
            );
            
            $this->logger->debug("Database connection established");
            
        } catch (PDOException $e) {
            $this->logger->error("Database connection failed: " . $e->getMessage());
            throw new \RuntimeException("Cannot connect to database: " . $e->getMessage());
        }
    }

    /**
     * Execute SELECT query and return single row
     * 
     * @param string $query SQL query with named parameters
     * @param array $params Parameters for query
     * @return array|null Single row or null
     */
    public function pselectRow(string $query, array $params = []): ?array
    {
        try {
            $stmt = $this->getConnection()->prepare($query);
            $stmt->execute($params);
            $result = $stmt->fetch();
            
            return $result ?: null;
            
        } catch (PDOException $e) {
            $this->logger->error("Query failed: " . $e->getMessage());
            throw new \RuntimeException("Database query failed: " . $e->getMessage());
        }
    }

    /**
     * Execute SELECT query and return single value
     * 
     * @param string $query SQL query
     * @param array $params Parameters
     * @return mixed Single value or null
     */
    public function pselectOne(string $query, array $params = [])
    {
        $row = $this->pselectRow($query, $params);
        return $row ? reset($row) : null;
    }

    /**
     * Execute SELECT query and return all rows
     * 
     * @param string $query SQL query
     * @param array $params Parameters
     * @return array Array of rows
     */
    public function pselect(string $query, array $params = []): array
    {
        try {
            $stmt = $this->getConnection()->prepare($query);
            $stmt->execute($params);
            
            return $stmt->fetchAll();
            
        } catch (PDOException $e) {
            $this->logger->error("Query failed: " . $e->getMessage());
            throw new \RuntimeException("Database query failed: " . $e->getMessage());
        }
    }

    /**
     * Insert record into table
     * 
     * @param string $table Table name
     * @param array $data Associative array of column => value
     * @return int Last insert ID
     */
    public function insert(string $table, array $data): int
    {
        $columns = array_keys($data);
        $placeholders = array_map(fn($col) => ":$col", $columns);
        
        $query = sprintf(
            "INSERT INTO %s (%s) VALUES (%s)",
            $table,
            implode(', ', $columns),
            implode(', ', $placeholders)
        );

        try {
            $stmt = $this->getConnection()->prepare($query);
            $stmt->execute($data);
            
            return (int) $this->getConnection()->lastInsertId();
            
        } catch (PDOException $e) {
            $this->logger->error("Insert failed: " . $e->getMessage());
            throw new \RuntimeException("Database insert failed: " . $e->getMessage());
        }
    }

    /**
     * Update record in table
     * 
     * @param string $table Table name
     * @param array $data Data to update
     * @param array $where WHERE conditions
     * @return int Number of affected rows
     */
    public function update(string $table, array $data, array $where): int
    {
        $setClauses = [];
        foreach (array_keys($data) as $col) {
            $setClauses[] = "$col = :set_$col";
        }
        
        $whereClauses = [];
        foreach (array_keys($where) as $col) {
            $whereClauses[] = "$col = :where_$col";
        }
        
        $query = sprintf(
            "UPDATE %s SET %s WHERE %s",
            $table,
            implode(', ', $setClauses),
            implode(' AND ', $whereClauses)
        );

        // Merge params with prefixes
        $params = [];
        foreach ($data as $key => $value) {
            $params["set_$key"] = $value;
        }
        foreach ($where as $key => $value) {
            $params["where_$key"] = $value;
        }

        try {
            $stmt = $this->getConnection()->prepare($query);
            $stmt->execute($params);
            
            return $stmt->rowCount();
            
        } catch (PDOException $e) {
            $this->logger->error("Update failed: " . $e->getMessage());
            throw new \RuntimeException("Database update failed: " . $e->getMessage());
        }
    }

    /**
     * Check if candidate exists by external study ID
     * Uses: candidate_project_extid_rel table
     */
    public function candidateExistsByExtID(string $extStudyID): ?array
    {
        return $this->pselectRow(
            "SELECT ExtStudyID, CandID FROM candidate_project_extid_rel WHERE ExtStudyID = :extID",
            ['extID' => $extStudyID]
        );
    }

    /**
     * Get project ID by name
     */
    public function getProjectID(string $projectName): ?int
    {
        $result = $this->pselectOne(
            "SELECT ProjectID FROM Project WHERE Name = :name",
            ['name' => $projectName]
        );
        
        return $result ? (int) $result : null;
    }

    /**
     * Get site/center ID by name
     */
    public function getCenterID(string $siteName): ?int
    {
        $result = $this->pselectOne(
            "SELECT CenterID FROM psc WHERE Name = :name",
            ['name' => $siteName]
        );
        
        return $result ? (int) $result : null;
    }

    /**
     * Get session ID for candidate
     */
    public function getSessionID(int $candID, ?string $visitLabel = null): ?int
    {
        $query = "SELECT ID FROM session WHERE CandID = :candID";
        $params = ['candID' => $candID];
        
        if ($visitLabel !== null) {
            $query .= " AND Visit_label = :visit";
            $params['visit'] = $visitLabel;
        }
        
        $query .= " LIMIT 1";
        
        $result = $this->pselectOne($query, $params);
        return $result ? (int) $result : null;
    }

    /**
     * Close connection
     */
    public function close(): void
    {
        $this->connection = null;
    }
}
