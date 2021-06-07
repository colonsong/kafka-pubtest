<?php

use Illuminate\Support\Facades\Route;

/*
|--------------------------------------------------------------------------
| Web Routes
|--------------------------------------------------------------------------
|
| Here is where you can register web routes for your application. These
| routes are loaded by the RouteServiceProvider within a group which
| contains the "web" middleware group. Now create something great!
|
*/

Route::get('/', function () {

    $conf = new \RdKafka\Conf();
    $conf->set('compression.type', 'snappy');
    // 繫結服務節點
    $conf->set('metadata.broker.list', 'kafka:9092');
    $conf->set('log_level', LOG_DEBUG);
    $conf->set('debug', 'all');
    // 建立生產者
    $kafka = new \RdKafka\Producer($conf);

    // 建立主題例項
    $topic = $kafka->newTopic('aaa');
    // 生產主題資料，此時訊息在緩衝區中，並沒有真正被推送
    $topic->produce(RD_KAFKA_PARTITION_UA, 0, 'Message');
    // 阻塞時間(毫秒)， 0為非阻塞
    $kafka->poll(0);

    // 推送訊息，如果不呼叫此函式，訊息不會被髮送且會丟失
    $result = $kafka->flush(10000);

    if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
        throw new \RuntimeException('Was unable to flush, messages might be lost!');
    }
    return view('welcome');
});

Route::get('/consumer', function () {

    $conf = new \RdKafka\Conf();
    $conf->set('group.id', '1');
    $rk = new \RdKafka\Consumer($conf);
$rk->addBrokers("kafka:9092");
    $topicConf = new \RdKafka\TopicConf();
    $topicConf->set('auto.commit.interval.ms', 100);
    $topicConf->set('offset.store.method', 'broker');
    $topicConf->set('auto.offset.reset', 'smallest');
    $topic = $rk->newTopic('aaa', $topicConf);
    $topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);
    while (true) {
        $message = $topic->consume(0, 120*10000);
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                var_dump($message);
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
   echo "No more messages; will wait for more\n";
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                echo "Timed out\n";
                break;
            default:
                throw new \Exception($message->errstr(), $message->err);
                break;
        }
    }
});
