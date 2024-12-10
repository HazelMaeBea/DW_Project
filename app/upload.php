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
    // Ensure outcome.log exists
    $outcomeLog = __DIR__ . '/outcome.log';
    if (!file_exists($outcomeLog)) {
        file_put_contents($outcomeLog, '');
    }

    // Ensure message.log exists
    $messageLog = __DIR__ . '/message.log';
    if (!file_exists($messageLog)) {
        file_put_contents($messageLog, '');
    }

    // Log the response message to message.log
    error_log($responseMessage . "\n", 3, $messageLog);
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
        $notifications = [];
        while ($notification = $pdo->pgsqlGetNotify(PDO::FETCH_ASSOC, 1000)) {
            $notifications[] = $notification['payload'];
        }

        // Log all notifications at once
        foreach ($notifications as $message) {
            error_log($message . "\n", 3, $messageLog);
        }
        error_log("\n", 3, $outcomeLog);

        // Log the uploaded file names
        $uploadedFilesMessage = "Uploaded files: " . implode(', ', array_map('basename', $filePaths));

        // Check if data is loaded onto cleaned_normalized table
        $stmt = $pdo->query("SELECT COUNT(*) FROM sales_data_cube");
        $rowCount = $stmt->fetchColumn();
        if ($rowCount > 0) {
            $responseMessage = "Files uploaded and processed successfully!\nData loaded onto sales_data_cube table successfully.";
        } else {
            $responseMessage = "Files uploaded and processed successfully!\nData not loaded onto sales_data_cube table.";
        }

        // Include uploaded files in the response message
        $responseMessage .= "\n" . $uploadedFilesMessage;

        // Calculate the processing time
        $endTime = microtime(true);
        $processingTime = $endTime - $startTime;
        $formattedProcessingTime = sprintf('%02dh %02dm %02ds %03dms', ($processingTime / 3600), ($processingTime / 60 % 60), $processingTime % 60, ($processingTime * 1000) % 1000);

        // Log the success message with processing time
        $finalMessage = $responseMessage . "\nProcessing Time: " . $formattedProcessingTime;
        error_log($finalMessage . "\n", 3, $outcomeLog);

        // Include the formatted processing time in the response
        $response = ['message' => $finalMessage];

    } catch (PDOException $e) {
        error_log("Error executing stored procedure: " . $e->getMessage(), 3, __DIR__ . '/error.log');
        $responseMessage = "Error executing stored procedure: " . $e->getMessage();
        $response = ['message' => $responseMessage];
    }
} else {
    $responseMessage = "No files uploaded or paths are empty.";
    // Log the response message
    error_log($responseMessage . "\n", 3, $outcomeLog);
    $response = ['message' => $responseMessage];
}

// Send response message as JSON
if (isset($_POST['start_time'])) {
    $response['start_time'] = $_POST['start_time'];
}
echo json_encode($response);

// Close the database connection
$pdo = null;
?>