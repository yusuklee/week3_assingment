"""콘솔 consumer — producer 가 보낸 메시지를 읽어서 확인용으로 출력한다.

예)
  python consumer.py --max 5                 # 처음부터 5건만 읽고 종료
  python consumer.py --group check2          # 다른 group id 로 처음부터 다시 읽기
"""
import argparse
import json
import sys

from confluent_kafka import Consumer, KafkaError


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="user-events")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--group", default="console-check")
    ap.add_argument("--max", type=int, default=None, help="읽을 최대 건수 (없으면 Ctrl+C 까지)")
    args = ap.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "auto.offset.reset": "earliest",   # 이 group 이 처음이면 토픽 맨 앞부터
        "enable.auto.commit": True,
    })
    consumer.subscribe([args.topic])
    print(f"subscribed topic={args.topic} group={args.group}  (Ctrl+C to stop)", file=sys.stderr)

    n = 0
    try:
        while args.max is None or n < args.max:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print("error:", msg.error(), file=sys.stderr)
                continue
            n += 1
            value = json.loads(msg.value())
            print(f"[p{msg.partition()} off={msg.offset()}] key={msg.key().decode()}  "
                  f"{value['event_type']:<11} item={value['payload']['item_id']}  event_time={value['event_time']}")
            if n <= 2:  # 처음 두 건은 전체 JSON 도 보여준다
                print(json.dumps(value, ensure_ascii=False, indent=2))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print(f"consumed {n} messages", file=sys.stderr)


if __name__ == "__main__":
    main()
