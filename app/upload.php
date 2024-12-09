<?php
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

    // Listen for notifications on log_channel
    $pdo->exec("LISTEN log_channel");

    // Test query to check connection
    $stmt = $pdo->query("SELECT 1");
    if ($stmt) {
        $responseMessage = "Database connection successful.";
    } else {
        $responseMessage = "Database connection failed.";
    }
} catch (PDOException $e) {
    error_log("Database connection error: " . $e->getMessage() . "\n\n", 3, __DIR__ . '/error.log');
    die(json_encode(['message' => "Could not connect to the database: " . $e->getMessage()]));
}

// Directory to save uploaded files
$uploadDir = __DIR__ . '/uploads/';
if (!file_exists($uploadDir)) {
    mkdir($uploadDir, 0775, true); // Create directory if it doesn't exist
}

// Initialize an array to store file paths for the stored procedure
$filePaths = [];

// Check if files were uploaded
if (!empty($_FILES['csv_files']['name'][0])) {
    // Save uploaded files and store their paths
    foreach ($_FILES['csv_files']['tmp_name'] as $key => $tmpName) {
        $fileName = basename($_FILES['csv_files']['name'][$key]);
        $filePath = $uploadDir . $fileName;
        if (move_uploaded_file($tmpName, $filePath)) {
            $filePaths[] = $filePath;
        }
    }

    // Retrieve the start time from the form data
    $startTime = isset($_POST['start_time']) ? (float)$_POST['start_time'] : microtime(true);

    // Call the stored procedure to process the uploaded files
    $filePathsString = implode(',', $filePaths);
    try {
        $stmt = $pdo->prepare("CALL data_extraction(:file_paths)");
        $stmt->bindParam(':file_paths', $filePathsString);
        $stmt->execute();

        // Fetch notifications from log_channel
        while ($notification = $pdo->pgsqlGetNotify(PDO::FETCH_ASSOC, 1000)) {
            $logData = json_encode(['message' => $notification['message']]);
            $ch = curl_init('log_message.php');
            curl_setopt($ch, CURLOPT_POST, 1);
            curl_setopt($ch, CURLOPT_POSTFIELDS, $logData);
            curl_setopt($ch, CURLOPT_HTTPHEADER, ['Content-Type: application/json']);
            curl_exec($ch);
            curl_close($ch);
        }

        // Check if data is loaded onto cleaned_normalized table
        $stmt = $pdo->query("SELECT COUNT(*) FROM sales_data_cube");
        $rowCount = $stmt->fetchColumn();
        if ($rowCount > 0) {
            $responseMessage = "Files uploaded and processed successfully!\nData loaded onto sales_data_cube table successfully.";
        } else {
            $responseMessage = "Files uploaded and processed successfully!\nData not loaded onto sales_data_cube table.";
        }

        // Log the success message
        $logData = json_encode(['message' => $responseMessage]);
        $ch = curl_init('log_message.php');
        curl_setopt($ch, CURLOPT_POST, 1);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $logData);
        curl_setopt($ch, CURLOPT_HTTPHEADER, ['Content-Type: application/json']);
        curl_exec($ch);
        curl_close($ch);

    } catch (PDOException $e) {
        error_log("Error executing stored procedure: " . $e->getMessage(), 3, __DIR__ . '/error.log');
        $responseMessage = "Error executing stored procedure: " . $e->getMessage();
    }
} else {
    $responseMessage = "No files uploaded or paths are empty.";
}

// Calculate the elapsed time
$endTime = microtime(true);
$elapsedTime = $endTime - $startTime;

// Send response message as JSON
$response = ['message' => $responseMessage . "\nElapsed time: " . gmdate("H\h i\m s\s", $elapsedTime)];
if (isset($_POST['start_time'])) {
    $response['start_time'] = $_POST['start_time'];
}
echo json_encode($response);

// Close the database connection
$pdo = null;
?>