<?php
$data = json_decode(file_get_contents('php://input'), true);
if (isset($data['message'])) {
    $logFile = __DIR__ . '/message.log';
    file_put_contents($logFile, $data['message'] . "\n", FILE_APPEND);
    echo json_encode(['status' => 'success']);
} else {
    echo json_encode(['status' => 'error', 'message' => 'No message provided']);
}
?>