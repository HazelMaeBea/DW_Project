<?php
// for testing code 

    phpinfo();

    $host = "localhost";

    $dbname = "elestudy";
    $user = "postgres";
    $pass = "123456789";
    $conn = pg_connect("host=$host dbname=$dbname user=$user password=$pass");

    if (!$conn) {
        echo "error";
        exit;
    }

    $result = pg_query($conn, "Select * FROM employees");
    if (!$result) {
        echo "query error";
        exit;
    }
   
        
    while($row = pg_fetch_assoc($result))
    echo "
    <tr>
        <td>$row[id]</td>
        <td>$row[first_name]</td>
        <td>$row[last_name]</td>
    </tr>
    ";
        
    
?>