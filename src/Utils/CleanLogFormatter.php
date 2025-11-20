<?php
declare(strict_types=1);

namespace LORIS\Utils;

use Monolog\Formatter\LineFormatter;
use Monolog\LogRecord;

/**
 * Clean log formatter - removes empty context/extra arrays
 */
class CleanLogFormatter extends LineFormatter
{
    public function __construct()
    {
        // Format: [datetime] level: message
        // No context [] or extra [] arrays
        parent::__construct(
            "[%datetime%] %channel%.%level_name%: %message%\n",
            "Y-m-d\TH:i:s.uP",
            true,
            true
        );
    }
    
    /**
     * Override to not include context/extra when empty
     */
    public function format(LogRecord $record): string
    {
        $output = parent::format($record);
        
        // Remove trailing empty arrays [] []
        $output = preg_replace('/\s*\[\]\s*\[\]\s*$/', '', $output);
        
        return $output . "\n";
    }
}
