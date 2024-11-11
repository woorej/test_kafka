from kafka import KafkaConsumer
import logging
import os
import test_data_pb2, test_data_pb2_grpc
import grpc
from concurrent import futures
import threading

consumer = KafkaConsumer(
    'number_topic',
    bootstrap_servers=['192.168.1.7:9092'],
    enable_auto_commit=True,
    auto_offset_reset='earliest',
    group_id='processing_server_group'  # 고유한 그룹 ID를 설정합니다.
)


class ProcessingServer(test_data_pb2_grpc.ProcessingServiceServicer):
    def __init__(self, server_id):
        self.server_id = server_id
        self.client_address = '192.168.1.7:50003'
        
        #channel = grpc.insecure_channel(self.client_address)
        #self.client_stub = test_data_pb2_grpc.ClientServiceStub(channel)
        self.log = self.make_log('./log/server_2.log')
        
        
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
        
    def consume_data(self):
        # Kafka Consumer를 통해 데이터 수신 및 처리
        while True:        
            for message in consumer:
                
                try:
                    partitions = consumer.assignment()
                    self.log.debug(f"{self.server_id} 서버에 할당된 파티션: {partitions}")
                except Exception as e:
                    self.log.error(f"{e}")
                
                
                number = int(message.value.decode('utf-8'))
                response_message = "짝수입니다" if number % 2 == 0 else "홀수입니다"
                self.log.debug(f"{self.server_id} 서버가 처리한 데이터: {number}, 결과: {response_message}")
        
            
    def ProcessData(self, request, context):
        number = request.data
        response_message = "짝수입니다" if number % 2 == 0 else "홀수입니다"
        self.log.debug(f"{self.server_id} 서버가 처리한 데이터: {number}, {response_message}")
        
        # 클라이언트로 응답 전송
        client_request = test_data_pb2.NumberRequest(data=number)
        response = self.client_stub.ReceiveData(client_request)
        
        self.log.debug(f"클라이언트로 보낸 응답: {response.message}")
        return test_data_pb2.NumberResponse(message=response_message)
    
    def start_server(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
        test_data_pb2_grpc.add_ProcessingServiceServicer_to_server(self, server)
        server.add_insecure_port(f'192.168.1.6:{50001 if self.server_id == "1" else 50002}')
        server.start()
        threading.Thread(target=processing_server.consume_data).start()
        self.log.debug(f"{self.server_id} 서버가 시작되었습니다. 포트: {50001 if self.server_id == '1' else 50002}")
        server.wait_for_termination()
        
if __name__ == "__main__":
    server_id = "2"
    processing_server = ProcessingServer(server_id)
    processing_server.start_server()

