<?php
// Database connection settings
$host = "localhost";
$dbname = "DW_Test";
$user = "postgres";
$password = "1234";

// Connect to PostgreSQL
try {
    $pdo = new PDO("pgsql:host=$host;dbname=$dbname", $user, $password);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
} catch (PDOException $e) {
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
            }
        }
    }
}

// Convert file paths to a comma-separated string
$filePathsString = implode(',', $filePaths);
// Call the stored procedure with the comma-separated string
if (!empty($filePathsString)) {
    try {
        $stmt = $pdo->prepare("CALL data_extraction(:filePaths)");
        $stmt->bindParam(':filePaths', $filePathsString, PDO::PARAM_STR);
        $stmt->execute();

        echo "Files uploaded and processed successfully!";
    } catch (PDOException $e) {
        echo "Error executing stored procedure: " . $e->getMessage();
    }
} else {
    echo "No files uploaded or paths are empty.";
}

// Close the database connection
$pdo = null;
?>
