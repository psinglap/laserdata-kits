"""Shadow your agent's task and tool-call events from Kafka into the Apache Iggy task progress lane, read-only.

A plain Kafka consumer in its own group ("iggy-shadow"): the agent runtime, every producer and every existing
consumer (Flink jobs, the ClickHouse sink, QA scoring) are untouched. For each record it publishes one step,
keyed by call (or conversation), with a per-call sequence, the same shape reasoning_layer.py writes. Point
voice_layer.py at Iggy and compare when each step reaches it with when your conversational layer learned it.

Map your record in to_step(): the kit needs the call or conversation id, the task (an AOP step or tool call id),
the step number and step count if you have them, whether the task is still running or done, and what the caller
can be told. The defaults read JSON with fields conversation_id, task_id, step, steps, status and summary.
In a shadow run only the lane is timed; the clock starts when the bridge reads the record.
Needs: pip install confluent-kafka apache-iggy==0.9.0
Pass client settings with -X, for example:
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 -X sasl.username=... -X sasl.password=...
"""
import argparse
import asyncio
import json
from collections import defaultdict

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, DONE, PARTITIONS, RUNNING, STREAM, TASKS, ensure_topics, msg, part


def to_step(value: bytes, a):
    d = json.loads(value)
    status = DONE if str(d.get(a.status_field, "")).lower() in a.done_values else RUNNING
    return (str(d[a.call_field]), str(d[a.task_field]), int(d.get(a.step_field) or 0),
            int(d.get(a.steps_field) or 0), status, str(d.get(a.say_field, ""))[:200])


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    await ensure_topics(iggy, TASKS)
    producer = await iggy.producer(STREAM, TASKS, topic_partitions_count=PARTITIONS[TASKS])
    conf = {"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"}
    conf.update(kv.split("=", 1) for kv in a.X)
    kafka = Consumer(conf)
    kafka.subscribe([a.kafka_topic])
    seq = defaultdict(int)
    async with producer:
        while True:
            groups = defaultdict(list)
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if rec.error():
                    continue
                try:
                    call, task, step, of, status, say = to_step(rec.value(), a)
                except (KeyError, ValueError, TypeError):
                    continue                # not a task event this mapping understands
                seq[call] += 1
                groups[part(call, PARTITIONS[TASKS])].append(SendMessage(msg(
                    call=call, task=task, step=step or seq[call], of=of, status=status, say=say, seq=seq[call])))
            # One batch per partition, all sent at once, so a call's steps stay in order.
            await asyncio.gather(*(producer.send_with_partitioning(ms, Partitioning.partition_id(p))
                                   for p, ms in groups.items()))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True, help="your Kafka bootstrap brokers")
    p.add_argument("--kafka-topic", default="agent-events", help="the topic your task or tool-call events are on")
    p.add_argument("--call-field", default="conversation_id")
    p.add_argument("--task-field", default="task_id")
    p.add_argument("--step-field", default="step")
    p.add_argument("--steps-field", default="steps")
    p.add_argument("--status-field", default="status")
    p.add_argument("--done-values", nargs="+", default=["done", "completed", "succeeded", "failed"])
    p.add_argument("--say-field", default="summary")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("-X", action="append", default=[], metavar="key=value", help="extra Kafka client setting")
    asyncio.run(main(p.parse_args()))
