<?php
// Database connection settings
$host = "localhost";
$dbname = "dw_project";
$user = "postgres";
$password = "123456789";

// Enable error reporting
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

// Connect to PostgreSQL
try {
    $pdo = new PDO("pgsql:host=$host;dbname=$dbname", $user, $password);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    // Test query to check connection
    $stmt = $pdo->query("SELECT 1");
    if ($stmt) {
        $responseMessage = "Database connection successful.";
    } else {
        $responseMessage = "Database connection failed.";
    }
} catch (PDOException $e) {
    error_log("Database connection error: " . $e->getMessage(), 3, __DIR__ . '/error.log');
    die(json_encode(['message' => "Could not connect to the database: " . $e->getMessage()]));
}

// Directory to save uploaded files
$uploadDir = __DIR__ . '/uploads/';
if (!file_exists($uploadDir)) {
    mkdir($uploadDir, 0775, true); // Create directory if it doesn't exist
}

// Initialize an array to store file paths
$filePaths = [];

// Check if files were uploaded
if (!empty($_FILES['csv_files']['name'][0])) {
    foreach ($_FILES['csv_files']['tmp_name'] as $index => $tmpFilePath) {
        if ($tmpFilePath != "") {
            $fileName = basename($_FILES['csv_files']['name'][$index]);
            $destination = $uploadDir . $fileName;

            // Move the file to the upload directory
            if (move_uploaded_file($tmpFilePath, $destination)) {
                $filePaths[] = $destination; // Add file path to array
            } else {
                die(json_encode(['message' => "Failed to move file: $fileName"]));
            }
        }
    }
} else {
    die(json_encode(['message' => "No files uploaded."]));
}

// Convert file paths to a comma-separated string
$filePathsString = implode(',', $filePaths);

// Create a temporary table to capture RAISE NOTICE messages
$pdo->exec("CREATE TEMP TABLE temp_log (message TEXT)");

// Create a custom function to log messages to the temporary table
$pdo->exec("
    CREATE OR REPLACE FUNCTION log_message(message TEXT) RETURNS VOID AS $$
    BEGIN
        INSERT INTO temp_log (message) VALUES (message);
    END;
    $$ LANGUAGE plpgsql;
");

// Call the stored procedure with the comma-separated string
if (!empty($filePathsString)) {
    try {
        $stmt = $pdo->prepare("CALL data_extraction(:file_paths)");
        $stmt->bindParam(':file_paths', $filePathsString, PDO::PARAM_STR);
        $stmt->execute();

        // Fetch and log RAISE NOTICE messages
        $logMessages = $pdo->query("SELECT message FROM temp_log")->fetchAll(PDO::FETCH_COLUMN);
        $logFile = __DIR__ . '/message.log';
        file_put_contents($logFile, "Files uploaded and processed successfully!\n", FILE_APPEND);
        file_put_contents($logFile, "Log Messages:\n", FILE_APPEND);
        foreach ($logMessages as $message) {
            file_put_contents($logFile, $message . "\n", FILE_APPEND);
        }

        // Check if data is loaded onto cleaned_normalized table
        $stmt = $pdo->query("SELECT COUNT(*) FROM cleaned_normalized");
        $rowCount = $stmt->fetchColumn();
        if ($rowCount > 0) {
            $responseMessage = "Data loaded onto cleaned_normalized table successfully.";
        } else {
            $responseMessage = "Data not loaded onto cleaned_normalized table.";
        }
    } catch (PDOException $e) {
        error_log("Error executing stored procedure: " . $e->getMessage(), 3, __DIR__ . '/error.log');
        $responseMessage = "Error executing stored procedure: " . $e->getMessage();
    }
} else {
    $responseMessage = "No files uploaded or paths are empty.";
}

// Send response message as JSON
echo json_encode(['message' => $responseMessage]);

// Close the database connection
$pdo = null;
?>
