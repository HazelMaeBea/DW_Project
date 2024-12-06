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
        echo "Database connection successful.<br>";
    } else {
        echo "Database connection failed.<br>";
    }
} catch (PDOException $e) {
    error_log("Database connection error: " . $e->getMessage(), 3, __DIR__ . '/error.log');
    die("Could not connect to the database: " . $e->getMessage());
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
                echo "Failed to move file: $fileName";
            }
        }
    }
} else {
    echo "No files uploaded.";
}

// Convert file paths to a comma-separated string
$filePathsString = implode(',', $filePaths);

// Call the stored procedure with the comma-separated string
if (!empty($filePathsString)) {
    try {
        $stmt = $pdo->prepare("CALL data_extraction(:file_paths)");
        $stmt->bindParam(':file_paths', $filePathsString, PDO::PARAM_STR);
        $stmt->execute();

        echo "Files uploaded and processed successfully!";
    } catch (PDOException $e) {
        error_log("Error executing stored procedure: " . $e->getMessage(), 3, __DIR__ . '/error.log');
        echo "Error executing stored procedure: " . $e->getMessage();
    }
} else {
    echo "No files uploaded or paths are empty.";
}

// Close the database connection
$pdo = null;
?>
