"""
Simple library for sending events to Splunk HTTP Event Collector (HEC)
"""
import json
import logging

import requests
import socket
import time


class SplunkHECWriter:
    """
    Simple SplunkHECWriter
    """

    def __init__(
            self,
            splunk_host: str,
            splunk_hec_token: str,
            splunk_port: int = 8088,
            sourcetype: str = "httpevent",
            index: str = "main",
            http_scheme: str = "https",
            source: str = socket.getfqdn(),
            host: str = socket.getfqdn(),
            verify_ssl: bool = True,
    ):
        self.hec_session = requests.Session()
        self.url = (
            f"{http_scheme}://{splunk_host}:{splunk_port}/services/collector/event"
        )
        self.headers = {"Authorization": f"Splunk {splunk_hec_token}"}
        self.sourcetype = sourcetype
        self.source = source
        self.host = host
        self.index = index

        self.verify_ssl = verify_ssl

    def __send_msg(self, msg: dict, event_time: float = time.time()) -> requests.Response:
        """
        Send event to Splunk HEC collector
        """
        payload = {
            "source": self.source,
            "sourcetype": self.sourcetype,
            "time": event_time,
            "host": self.host,
            "index": self.index,
            "event": json.dumps(msg),
        }

        response = self.hec_session.post(
            url=self.url, headers=self.headers, json=payload, verify=self.verify_ssl
        )

        if response.status_code != 200:
            err_msg = f"Send hec msg failed! response: {response.text}"
            logging.error(err_msg)

            raise IOError(err_msg)

        return response

    def send_msg(self, msg: dict, event_time: float = time.time()) -> requests.Response:
        """
        Send event to Splunk HEC collector

        Keyword arguments:
        msg -- dict message to sned
        time -- event time

        Return request Response
        """
        response = None
        for i in range(0, 9):
            try:
                response = self.__send_msg(msg=msg, event_time=event_time)
                break
            except IOError:
                time.sleep(10)
                continue

        return response

    def send_msgs(self, msgs: list, event_time: float = time.time(), limit: int = 100) -> requests.Response:
        """
        Send events to Splunk HEC collector
        """
        payload_str = ""
        last_response = None

        counter = 0
        for msg in msgs:
            payload = {
                "source": self.source,
                "sourcetype": self.sourcetype,
                "time": event_time,
                "host": self.host,
                "index": self.index,
                "event": json.dumps(msg),
            }

            payload_str += json.dumps(payload)

            counter += 1

            if counter >= limit:
                counter = 0
                for i in range(0, 9):
                    last_response = self.hec_session.post(
                        url=self.url,
                        headers=self.headers,
                        data=payload_str,
                        verify=self.verify_ssl,
                    )

                    if last_response.status_code == 400:
                        response_data = json.loads(last_response.text)

                        if response_data["text"] == "No data" and response_data["code"] == 5:
                            # OK event
                            break

                    if last_response.status_code != 200:
                        err_msg = f"Send hec msg failed! response: {last_response.text}"
                        logging.error(err_msg)

                        time.sleep(10)
                        continue

                    break

        return last_response
