메타코드 데이터 엔지니어 7기 · 이유석

```
repo/
├─ docker-compose.yml   # Kafka (KRaft 단일 브로커)
├─ create_topics.py     # 토픽 생성 (user-events, user-events-dlq)
├─ producer.py          # events.csv 리플레이 -> Kafka  
├─ consumer.py          # 콘솔 consumer, 확인용      
├─ preprocess.py        # Spark 전처리 · 집계           
├─ dag.py               # Airflow DAG                  
├─ schema.sql           # PostgreSQL 스키마            
├─ data/events.csv      # Kaggle 에서 다운로드 (git 제외)
└─ docs/pipeline_architecture.png
```

---

## 1. 사용 데이터

**Retailrocket recommender system dataset** (Kaggle 공개, CC BY-NC-SA 4.0)
https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset

실제 이커머스 사이트에서 2015-05-03부터 4.5개월 동안 수집한 유저 행동 로그. 이 중 `events.csv` 사용

### 스키마 (`events.csv`)

| 필드 | 타입 | 설명 | 예시 |
|---|---|---|---|
| `timestamp` | int64 | 이벤트 발생 시각 (Unix epoch, 밀리초) | `1433221332117` |
| `visitorid` | int64 | 유저 ID (해시) | `257597` |
| `event` | string | `view` / `addtocart` / `transaction` | `view` |
| `itemid` | int64 | 상품 ID (해시) | `355908` |
| `transactionid` | int64 (nullable) | 구매 이벤트일 때만 채워지는 주문 ID | `4000` |

```csv
timestamp,visitorid,event,itemid,transactionid
1433221332117,257597,view,355908,
1433224214164,992329,view,248676,
1433222276276,599528,transaction,356475,4000
```
## 2. 전체 파이프라인 구조도

![pipeline architecture](docs/pipeline_architecture.png)

| 단계                      | 흐르는 것                             | 주기 |
|-------------------------|-----------------------------------|---|
| 생성기 -> Kafka            | JSON 이벤트 1건씩, key = visitor_id    | 리플레이 배속에 따라 (최대 수천 건/초) |
| Kafka -> Spark          | `user-events` 토픽 micro-batch      | 1분 트리거 |
| Spark -> PostgreSQL     | 정제 이벤트 upsert(event_id), 1분 집계    | 1분 |
| Spark -> Parquet        | 원본 append                         | 1분 |
| Airflow -> Spark/PostgreSQL | Parquet -> 일 집계, 추천 테이블 갱신, 품질 체크 | 매일 03:00 |
| PostgreSQL -> API       | 유저별 추천 Top-N, 트렌딩 아이템 SQL 조회      | 요청 시 |

---
### 3. Topic 구성

| 항목 | 값 | 이유                                                                         |
|---|---|----------------------------------------------------------------------------|
| 토픽명 | `user-events` | view · addtocart · transaction 와 같이 이벤트 종류가 달라도 같은 유저 안에서 발생 순서대로 읽어야 하기떄문 |
| 파티션 수 | 3 | 유저가 30명이면 각 파티션에 10명씩 들어가서 병렬처리로 성능을 올린다            파티션은 0,1,2 가존재         |
| key | `visitor_id` | visitor id %3 으로 파티션에 넣는다                                                  |
| 리텐션 | 7일  | 집계를 실수해서 데이터를 다시 봐야하는 상황이 그렇게 많지는 않을거같아서 7일로 했다.                           |
| DLQ | `user-events-dlq` (파티션 1, 리텐션 30일) | 두번째 토픽으로 메세지가 꺠져서 읽기가 실패한 데이터들을 모아둔 토픽                                     |

```
$ kafka-topics.sh --describe --topic user-events
Topic: user-events  PartitionCount: 3  ReplicationFactor: 1  Configs: retention.ms=604800000
    Partition: 0  Leader: 1  Replicas: 1  Isr: 1
    Partition: 1  Leader: 1  Replicas: 1  Isr: 1
    Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

**리스너 구성** (`docker-compose.yml`): 내 PC의 Python은 `localhost:9092`, 같은 compose 네트워크의 Spark · Airflow 컨테이너는 `kafka:29092`로 접속하도록 리스너를 두 개 열어 뒀다.
   

## 4. Kafka 수집 


### 4-1. Producer (`producer.py`)

```
CSV 읽기 -> 시간순 정렬 ->  (속도 제어: sleep) ->  JSON 직렬화 ->  produce(key, value) ->  콜백
                                                                                           ├─ 성공: ok 카운트, 처음 3건은 partition/offset 로그
                                                                                           └─ 실패: fail 카운트, failed_messages.jsonl 에 기록
```
![producer_flow.png](docs/producer_flow.png)

**Producer 설정**

| 설정 | 값 |
|---|---|---|
| `enable.idempotence` | `true` 
| `acks` | `all` 
| `compression.type` | `lz4` 
| `linger.ms` | `5`
   

### 4-3. message examples

key = `"1149227"` (visitorid)

```json
{
  "event_id": "bf7a5319-89f5-45d6-b21a-758d81e99189",
  "event_type": "view",  
  "event_time": "2015-05-03T03:00:27.031Z",
  "produced_at": "2026-10-26T02:33:54.794Z",
  "schema_version": 1,
  "source": "replay-generator",
  "payload": {
    "visitor_id": 1149227,
    "item_id": 29757,
    "transaction_id": null
  }
}


```


**producer 실행 로그** 

```
INFO loading data/events.csv
INFO rows to send: 20000
INFO sent=5000 ok=0 fail=0  63280 msg/s
INFO sent=10000 ok=0 fail=0  62865 msg/s
INFO sent=20000 ok=0 fail=0  64246 msg/s
INFO delivered  topic=user-events partition=1 offset=0 key=693516
INFO delivered  topic=user-events partition=1 offset=1 key=652699
INFO delivered  topic=user-events partition=1 offset=2 key=693516
INFO DONE sent=20000 ok=20000 fail=0 unflushed=0 elapsed=1.1s avg=17899 msg/s
```


**`--rate 200` 실행** 

```
INFO rows to send: 1000
INFO sent=500 ok=498 fail=0  200 msg/s
INFO sent=1000 ok=999 fail=0  200 msg/s
INFO DONE sent=1000 ok=1000 fail=0 unflushed=0 elapsed=5.0s avg=200 msg/s
```

**consumer 로 읽은 결과** 

```
[p2 off=0] key=1149227  view        item=29757   event_time=2015-05-03T03:00:27.031Z
[p2 off=1] key=1150418  view        item=374675  event_time=2015-05-03T03:00:38.570Z
[p2 off=2] key=1143053  view        item=463330  event_time=2015-05-03T03:00:48.174Z
[p2 off=3] key=1222029  view        item=75831   event_time=2015-05-03T03:00:51.805Z
[p2 off=4] key=990356   view        item=161722  event_time=2015-05-03T03:00:54.461Z
```

