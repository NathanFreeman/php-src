--TEST--
mysqli_real_escape_string() - gb2312
--EXTENSIONS--
mysqli
--SKIPIF--
<?php
require_once __DIR__ . '/test_setup/test_helpers.inc';
$link = mysqli_connect_or_skip();
if (!mysqli_set_charset($link, 'gb2312'))
    die(sprintf("skip Cannot set charset 'gb2312'"));
?>
--FILE--
<?php

    require_once 'connect.inc';
    if (!$link = my_mysqli_connect($host, $user, $passwd, $db, $port, $socket))
{
        printf("[001] Cannot connect to the server using host=%s, user=%s,
passwd=***, dbname=%s, port=%s, socket=%s - [%d] %s\n", $host, $user, $db,
$port, $socket, mysqli_connect_errno(), mysqli_connect_error());
    }

    if (!mysqli_query($link, 'DROP TABLE IF EXISTS test')) {
        printf("Failed to drop old test table: [%d] %s\n", mysqli_errno($link),
mysqli_error($link));
    }

    if (!mysqli_query($link, 'CREATE TABLE test(id INT, label CHAR(1), PRIMARY
KEY(id)) ENGINE=' . $engine . " DEFAULT CHARSET=gb2312")) {
        printf("Failed to create test table: [%d] %s\n", mysqli_errno($link),
mysqli_error($link));
    }

    var_dump(mysqli_set_charset($link, "gb2312"));

    if ('首先\\\\首先' !== ($tmp = mysqli_real_escape_string($link, '首先\\首先')))
        printf("[004] Expecting \\\\, got %s\n", $tmp);

    if ('首先\"首先' !== ($tmp = mysqli_real_escape_string($link, '首先"首先')))
        printf("[005] Expecting \", got %s\n", $tmp);

    if ("首先\'首先" !== ($tmp = mysqli_real_escape_string($link, "首先'首先")))
        printf("[006] Expecting ', got %s\n", $tmp);

    if ("首先\\n首先" !== ($tmp = mysqli_real_escape_string($link, "首先\n首先")))
        printf("[007] Expecting \\n, got %s\n", $tmp);

    if ("首先\\r首先" !== ($tmp = mysqli_real_escape_string($link, "首先\r首先")))
        printf("[008] Expecting \\r, got %s\n", $tmp);

    if ("首先\\0首先" !== ($tmp = mysqli_real_escape_string($link, "首先" . chr(0) . "首先")))
        printf("[009] Expecting %s, got %s\n", "首先\\0首先", $tmp);

    var_dump(mysqli_query($link, "INSERT INTO test(id, label) VALUES (100, '首')"));

    mysqli_close($link);
    print "done!";
?>
--CLEAN--
<?php
    require_once 'clean_table.inc';
?>
--EXPECT--
bool(true)
bool(true)
done!
