<?php

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\QueryResults;

require_once __DIR__ . '/vendor/autoload.php';

date_default_timezone_set('America/Los_Angeles');


if(isset($_GET['term'])){

	$term = $_GET['term'];

	$projectId = "wise-hub-175615";
	$bigQuery = new BigQueryClient([
            'projectId' => $projectId,
        ]);

    $query = "SELECT * FROM [wise-hub-175615:gcp.products] where product like '%{$term}%'";
    $options = ['useLegacySql' => true];
    $queryResults = $bigQuery->runQuery($query, $options);

    if ($queryResults->isComplete()) {

    $rows = $queryResults->rows();
    $array = array();
    foreach ($rows as $row) {
        //printf('--- Row %s ---' . PHP_EOL, ++$i);
        foreach ($row as $column => $value) {
            if ($column == 'product') {
           //printf('%s: %s' . PHP_EOL, $column, $value);
            //printf('%s: ' . PHP_EOL, $value);
            $array[] = $value;
        }
        }
        //printf($row['email']);
    }

    } else {
        throw new Exception('The query failed to complete');
    }

    echo json_encode($array);



}


?>
