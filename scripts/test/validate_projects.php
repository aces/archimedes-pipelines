<?php
$to = "sruthy.mcin@gmail.com";
$subject = "Mail Test";
$message = "This is a test mail using native mail().";
$headers = "From: noreply@loris.ca";

if (mail($to, $subject, $message, $headers)) {
    echo "mail() says: SENT";
} else {
    echo "mail() says: FAILED";
}