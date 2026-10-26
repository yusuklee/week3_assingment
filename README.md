# 이커머스 클릭스트림 추천 파이프라인

메타코드 데이터 엔지니어 7기 · A주제 「안정적인 파이프라인 구축 및 로드 테스트」 · 이유석

쇼핑몰 유저의 조회 · 장바구니 · 구매 이벤트를 Kafka로 수집하고, Spark로 정제 · 집계한 뒤 PostgreSQL에 저장해 "함께 본 상품 / 지금 뜨는 상품" 추천 결과를 쇼핑몰 운영자에게 제공하는 파이프라인.

```
repo/
├─ docker-compose.yml   # Kafka (KRaft 단일 브로커)
├─ create_topics.py     # 토픽 생성 (user-events, user-events-dlq)
├─ producer.py          # events.csv 리플레이 → Kafka  (3회차)
├─ consumer.py          # 콘솔 consumer, 확인용        (3회차)
├─ preprocess.py        # Spark 전처리 · 집계           (4회차 예정)
├─ dag.py               # Airflow DAG                  (5회차 예정)
├─ schema.sql           # PostgreSQL 스키마            (4회차 예정)
├─ data/events.csv      # Kaggle 에서 다운로드 (git 제외)
└─ docs/pipeline_architecture.png
```

---

## 1. 사용 데이터

**Retailrocket recommender system dataset** (Kaggle 공개, CC BY-NC-SA 4.0)
https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset

실제 이커머스 사이트에서 2015-05-03부터 4.5개월 동안 수집한 유저 행동 로그. 유저 · 아이템 ID는 해시 처리되어 개인정보 없음. 이 중 `events.csv`를 메인 소스로 사용한다.

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

### 크기와 발생 속도 (실제 파일로 계산)

| 항목 | 값 |
|---|---|
| 총 이벤트 수 | 2,756,101 건 (view 96.7% · addtocart 2.5% · transaction 0.8%) |
| 파일 크기 | 94.2 MB |
| 고유 유저 / 상품 | 1,407,580 명 / 235,061 개 |
| 평균 발생 속도 | 약 20,000 건/일 ≒ 분당 14 건 ≒ 0.23 건/초 |
| 피크 | 분당 최대 87 건 ≒ 1.4 건/초 (가장 바빴던 1분 기준) |
| 메시지 1건 크기 | 약 0.3 KB (JSON 직렬화 후, 아래 메시지 예시 기준) |

원본 속도는 평시 기준이고, 로드 테스트에서 리플레이 배속을 100x~1000x로 올려 초당 수백~수천 건 스트림을 재현한다.
초당 1,000건 × 0.3KB × 86,400초 ≒ 하루 26GB, 7일 보관 시 약 180GB → 로드 테스트는 시간 단위로 짧게 돌리고 리텐션은 7일로 둔다.

---

## 2. 전체 파이프라인 구조도

![pipeline architecture](docs/pipeline_architecture.png)

| 단계 | 흐르는 것 | 주기 |
|---|---|---|
| 생성기 → Kafka | JSON 이벤트 1건씩, key = visitor_id | 리플레이 배속에 따라 (최대 수천 건/초) |
| Kafka → Spark | `user-events` 토픽 micro-batch | 1분 트리거 |
| Spark → PostgreSQL | 정제 이벤트 upsert(event_id), 1분 집계 | 1분 |
| Spark → Parquet | 원본 append (날짜 파티션) | 1분 |
| Airflow → Spark/PostgreSQL | Parquet → 일 집계, 추천 테이블 갱신, 품질 체크 | 매일 03:00 |
| PostgreSQL → API | 유저별 추천 Top-N, 트렌딩 아이템 SQL 조회 | 요청 시 |

---

## 3. 플랫폼 선정 이유

### Kafka

- 이 프로젝트는 실제로는 CSV를 읽지만, 스트림을 재현하기 위해 Kafka를 사용한다.
- **같은 데이터를 여러 곳에서 읽어야 한다**: Spark가 집계 목적으로 읽고, 또 다른 컨슈머는 모니터링 목적으로 읽어야 한다. Kafka는 컨슈머마다 offset을 따로 관리해서 읽는 쪽을 추가해도 기존 소비에 영향이 없다.
- **Producer / Consumer 분리**: 생성기가 초당 1,000건을 보내도 Spark는 자기 속도로 처리하고, 못 따라간 만큼은 Kafka에 쌓인다(lag). 이 수치로 병목 위치를 알 수 있다. 생성기와 Spark가 직접 붙어 있으면 처리가 느려질 때 생성기도 같이 느려져서 분리 측정이 안 된다.
- **대안**: 파일로 쌓아 배치 처리 → 1분 단위 집계가 어렵고, 재처리 시 파일 관리 부담.
- **트레이드오프**: 로컬 리소스 사용 증가, 운영할 컴포넌트가 하나 늘어남.

### Spark

- **Kafka 연동**: Structured Streaming이 Kafka source · checkpoint · exactly-once 쓰기를 기본 제공한다.
- **실시간 집계**: 1분 윈도우 집계처럼 배치 하나를 넘어가는 상태 관리, 장애 복구, 병렬 확장을 pandas는 직접 짜야 하고 Spark는 내장이다.
- **대용량 대비**: 1000x 배속이면 초당 수백~수천 건이고, 동시 조회 쌍은 이벤트 수의 제곱으로 늘어난다.
- **대안**: pandas 컨슈머 루프 → 단순 정제는 가능하지만 윈도우 상태 · checkpoint · 확장을 전부 수동 구현.
- **트레이드오프**: JVM 기반이라 로컬 메모리 부담, 작은 데이터에는 오버헤드.

### Airflow

- **작업 의존성**: 일별 집계 → 추천 테이블 갱신 → 품질 체크 순서가 있고, 앞이 실패하면 뒤를 실행하면 안 된다.
- **backfill**: 특정 날짜 구간만 멱등하게 재처리할 수 있고, 실행 상태를 UI로 확인한다.
- **대안**: cron → 의존성 표현 불가, 실패 시 재시도 없음.
- **트레이드오프**: 메타 DB · 스케줄러 · 웹서버까지 띄워야 해서 무겁다.

### 저장소 — PostgreSQL (조회용) · Parquet (원본 보관용)

- **PostgreSQL**: 가공이 끝난 최종 결과(정제 이벤트, 아이템별 집계, 추천 목록)를 넣는 조회용 저장소. 유저별 조회 · 아이템별 집계에 인덱스와 JOIN이 필요하다. `event_id`를 PK로 두고 `ON CONFLICT DO NOTHING`으로 멱등 저장.
- **Parquet**: Kafka에서 받은 이벤트를 가공 전 모습 그대로 날짜별 파일로 저장. 집계 로직 버그 발견 시 재처리용 원본. Kafka 리텐션(7일) 이후까지 보관.
- **대안**: MongoDB → 필드 5개로 스키마가 고정이라 유연한 스키마의 이점이 없고, JOIN이 약함.

---

## 4. Kafka 수집 (3회차)

### 4-1. 실행 방법

```bash
# 0) 데이터: Kaggle 에서 events.csv 를 받아 data/ 에 둔다
#    https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset
pip install -r requirements.txt

# 1) Kafka 실행 (KRaft 단일 브로커)
docker compose up -d
docker compose ps                     # healthy 확인

# 2) 토픽 생성
python create_topics.py

# 3) producer 로 보내기
python producer.py --limit 20000 --speed 0        # 2만 건을 최대 속도로
python producer.py --speed 100                    # 원본 간격 100배속 리플레이
python producer.py --rate 500 --limit 60000       # 초당 500건 고정 (로드 테스트용)

# 4) 콘솔 consumer 로 읽어서 확인
python consumer.py --max 5
```

### 4-2. Producer 코드 흐름 (`producer.py`)

```
CSV 읽기 ──▶ 시간순 정렬 ──▶ (속도 제어: sleep) ──▶ JSON 직렬화 ──▶ produce(key, value) ──▶ 콜백
                                                                                           ├─ 성공: ok 카운트, 처음 3건은 partition/offset 로그
                                                                                           └─ 실패: fail 카운트, failed_messages.jsonl 에 기록
```

| 단계 | 내용 |
|---|---|
| 데이터 읽기 | pandas로 `events.csv` 로드 → `timestamp` 기준 정렬 (원본 CSV는 시간순이 아님). `--offset`, `--limit`으로 구간 선택 |
| 속도 제어 | `--speed N`: 원본 이벤트 간 간격을 N배속으로 재생 (0 = 최대 속도). `--rate N`: 초당 N건 고정 전송 (로드 테스트용, `--speed` 무시) |
| 직렬화 | 행 1개 → 공통 껍데기(event_id · event_type · event_time · produced_at · schema_version · source) + payload. `event_id`는 원본에 없어서 UUID 생성 |
| 전송 | `produce(topic, key=visitorid, value=json)`. 로컬 버퍼가 꽉 차면(`BufferError`) `poll()`로 콜백을 처리하며 대기 |
| 콜백 | 브로커 ack 시점에 호출. 실패 시 로그 + `failed_messages.jsonl`에 key/value/에러 기록 (재전송용) |
| 종료 | `flush(30)`으로 남은 메시지 전송 완료 대기 후 집계 출력 |

**Producer 안정성 설정**

| 설정 | 값 | 이유 |
|---|---|---|
| `enable.idempotence` | `true` | 재시도해도 브로커에 같은 메시지가 두 번 저장되지 않음 (`acks=all`, `retries` 자동 설정) |
| `acks` | `all` | 브로커가 디스크에 쓴 뒤 ack. 단일 브로커라 복제본은 1개지만 설정은 그대로 둠 |
| `compression.type` | `lz4` | 네트워크 · 디스크 절약 |
| `linger.ms` | `5` | 5ms 모아서 배치 전송. 로드 테스트에서 처리량 vs 지연 튜닝 대상 |

### 4-3. 메시지 예시 (실제 전송 캡처)

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

| 필드 | 역할 |
|---|---|
| key (`visitor_id`) | 같은 유저 이벤트를 같은 파티션으로 → 유저별 순서 보장 |
| `event_id` | 중복 제거 · 멱등 저장 기준 (producer에서 UUID 부여) |
| `event_type` | `view` / `addtocart` / `transaction` |
| `event_time` | 원본 발생 시각 → 윈도우 집계 기준 |
| `produced_at` | 생성기가 보낸 시각 → end-to-end 지연 측정 기준 (`processed_time − produced_at`) |
| `schema_version` | 필드가 바뀌어도 consumer가 안 깨지게 |
| `payload` | 이벤트 종류별 실제 데이터. 구매가 아니면 `transaction_id`는 `null` |

**producer 실행 로그** (20,000건, 최대 속도)

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

`sent`는 로컬 버퍼에 넣은 수, `ok`는 브로커 ack를 콜백으로 받은 수. 중간 로그에서 `ok=0`인 건 버퍼링 중이라서고, `flush()` 후 `ok=20000`으로 전부 확인됐다. key=693516 유저의 두 이벤트가 같은 파티션 1에 offset 0, 2로 들어간 것이 보인다.

**`--rate 200` 실행** (초당 200건 고정)

```
INFO rows to send: 1000
INFO sent=500 ok=498 fail=0  200 msg/s
INFO sent=1000 ok=999 fail=0  200 msg/s
INFO DONE sent=1000 ok=1000 fail=0 unflushed=0 elapsed=5.0s avg=200 msg/s
```

**consumer 로 읽은 결과** (`python consumer.py --max 5`)

```
[p2 off=0] key=1149227  view        item=29757   event_time=2015-05-03T03:00:27.031Z
[p2 off=1] key=1150418  view        item=374675  event_time=2015-05-03T03:00:38.570Z
[p2 off=2] key=1143053  view        item=463330  event_time=2015-05-03T03:00:48.174Z
[p2 off=3] key=1222029  view        item=75831   event_time=2015-05-03T03:00:51.805Z
[p2 off=4] key=990356   view        item=161722  event_time=2015-05-03T03:00:54.461Z
```

**파티션별 적재 건수** (총 23,000건 전송 후)

```
user-events:0:7578
user-events:1:7721
user-events:2:7701
```

key 해시로 3개 파티션에 고르게 분산됐다.

### 4-4. Topic 구성

| 항목 | 값 | 이유 |
|---|---|---|
| 토픽명 | `user-events` | view · addtocart · transaction은 스키마가 같고 늘 같이 처리하므로 토픽 하나에 `event_type` 필드로 구분. 종류별로 나누면 유저 한 명의 순서가 토픽 간에 깨진다 |
| 파티션 수 | 3 | 로컬 단일 브로커 기준 시작값. 로드 테스트에서 1 / 3 / 6으로 바꿔 가며 처리량 비교 예정. 파티션 수 = 병렬 consumer 상한 |
| key | `visitor_id` | 같은 유저의 view → addtocart → transaction 순서가 유지돼야 세션 · 동시 조회 쌍 계산이 맞다. 유저 140만 명이라 특정 key 쏠림 없음 |
| 리텐션 | 7일 (`retention.ms=604800000`) | 집계 로직 수정 후 재처리할 수 있는 기간. 영구 보관은 Parquet이 담당 |
| DLQ | `user-events-dlq` (파티션 1, 리텐션 30일) | Spark에서 파싱 실패한 메시지를 격리. 양이 적어 파티션 1개, 원인 분석 시간을 주기 위해 보관 기간을 길게 |

```
$ kafka-topics.sh --describe --topic user-events
Topic: user-events  PartitionCount: 3  ReplicationFactor: 1  Configs: retention.ms=604800000
    Partition: 0  Leader: 1  Replicas: 1  Isr: 1
    Partition: 1  Leader: 1  Replicas: 1  Isr: 1
    Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

**리스너 구성** (`docker-compose.yml`): 내 PC의 Python은 `localhost:9092`, 같은 compose 네트워크의 Spark · Airflow 컨테이너는 `kafka:29092`로 접속하도록 리스너를 두 개 열어 뒀다.

### 4-5. 확인한 것 · 고민 중인 것

- 최대 속도로 보내면 로컬에서 초당 약 18,000건까지 ack 됐다. 로드 테스트 상한은 Kafka가 아니라 Spark 쪽에서 걸릴 것으로 예상.
- `--speed 1000`으로 2,000건을 보냈을 때 28.8초 걸렸다 (원본 8시간치). 원본 간격이 들쭉날쭉해서 배속 리플레이는 처리량이 일정하지 않다. 로드 테스트는 `--rate`로 고정 속도를 쓰는 게 측정에 맞다.
- 고민: 실패 메시지를 `failed_messages.jsonl`에만 남기고 있는데, producer 단계 실패도 DLQ 토픽으로 보낼지 (브로커 자체가 죽은 경우엔 DLQ도 못 보내므로 파일이 맞다고 보고 있음).
