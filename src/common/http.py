import time 
import requests
from typing import Optional
from common.logger import get_logger

logger = get_logger(__name__)

class HttpClient:
    def __init__(
            self,
            timeout: int = 10,
            max_retries: int = 3,
            sleep_between: float = 1.0
    ):
        self.session = requests.session
        self.timeout = timeout
        self.max_retries = max_retries
        self.sleep_beteween = sleep_between

        self.session.headers.update(
            {
                "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
                )
            }
        )
    
    def get(self,url: str, params: Optional[dict]= None) -> Optional[str]:
        for attempt in range(1,self.max_retries + 1):
            try:
                response = self.session.get(
                    url,
                    params = params,
                    timeout = self.timeout
                )
                
                if response.status_code == 200:
                    time.sleep(self.sleep_beteween)
                    return response.text
                
                logger.warning(
                    f"HTTP {response.status_code} | attempt = {attempt} | url = {url}."
                )

            except requests.RequestException as e:
                logger.warning(
                    f"Request Eror | attempt = {attempt} | {e}"
                )
            
            time.sleep(self.sleep_beteween)

        logger.error(f"Fail after retries | url = {url}")

        return None