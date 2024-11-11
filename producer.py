import grpc
import random
from kafka import KafkaProducer

import test_data_pb2, test_data_pb2_grpc
import time
from concurrent import futures
import logging
import os
import threading

from kafka.partitioner.default import DefaultPartitioner


class RoundRobinPartitioner(DefaultPartitioner):
    def __init__(self):
        super().__init__()
        self.partition_index = 0

    def __call__(self, key, all_partitions, available_partitions):
        # 순차적으로 파티션에 메시지를 분배
        if not available_partitions:
            available_partitions = all_partitions
        partition = available_partitions[self.partition_index % len(available_partitions)]
        self.partition_index += 1
        return partition

# KafkaProducer 설정 시 커스텀 파티셔너 지정
producer = KafkaProducer(
    bootstrap_servers=['192.168.1.7:9092'],
    partitioner=RoundRobinPartitioner() 
)


# Client Server Generating Number

class ClientService(test_data_pb2_grpc.ClientServiceServicer):
    def __init__(self):
        self.port = 50003
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
        self.count = 0
        self.log = self.make_log('./log/client.log')
        self.response_stub = None
    
    def receive_data(self, request, context):
        self.log.debug(f"Received data: {request.data}")
        return test_data_pb2.NumberResponse(message="Successfully got message")
        
    def make_log(self, log_file_path=None):
        log = logging.getLogger("ProducerLogger")
        log.setLevel(logging.DEBUG)
        
        # StreamHandler setting (console output)
        stream_handler = logging.StreamHandler()
        stream_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        stream_handler.setFormatter(stream_formatter)
        
        log.addHandler(stream_handler)
        
        if log_file_path is not None:
            directory = os.path.dirname(log_file_path)
            os.makedirs(directory, exist_ok=True)
            
            # FileHandler setting (file output)
            file_handler = logging.FileHandler(log_file_path)
            file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            file_handler.setFormatter(file_formatter)
            log.addHandler(file_handler)    
    
        return log
        
    def send_data(self):            
        while self.count < 10:
            number = self.count
            try:
                producer.send('number_topic', value=str(number).encode('utf-8'))
                self.log.debug(f"클라이언트에서 생성된 숫자: {number}")
            except Exception as e:
                self.log.error(f"Kafka로 메시지 전송 실패: {e}")
            self.count += 1
            time.sleep(0.1)
    
    def run_thread(self):
        threading.Thread(target=self.send_data).start()
        
    def start_server(self):
        
        try:
            producer = KafkaProducer(
                bootstrap_servers=['192.168.1.7:9092'],
                partitioner=RoundRobinPartitioner()
            )
            self.log.debug("Kafka 브로커에 연결 성공")
        except Exception as e:
            import traceback
            self.log.error(f"Kafka 브로커 연결 실패: {traceback.format_exc()}")


        self.server.add_insecure_port(f'192.168.1.7:{self.port}')
        self.server.start()
        self.run_thread()
        self.log.debug(f"Client Waiting...Port: {self.port}")
        self.server.wait_for_termination()

if __name__ == "__main__":
    client_service = ClientService()
    client_service.start_server()