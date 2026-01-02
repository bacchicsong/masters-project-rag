import time
from qdrant_client import QdrantClient

from domain.query.query import Query, QueryResults
from domain.query.delivery.dto.dto import HistoryResponseDTO
from domain.query.delivery.dto.dto import HistoryItemDTO
from domain.query.usecase.i_query_usecase import IQueryUsecase
from infrastructure.db.qdrand import get_embedded_model
#from config.config import config

import uuid      
import json      
import requests  
import urllib3   

MAX_TOKENS = 4096 

class QueryUsecase(IQueryUsecase):
    def __init__(self, qdrant: QdrantClient, logger, config):
        self.qdrant = qdrant
        self.logger = logger
        self.model = get_embedded_model()
        self.collections_name = config.QDRANT_COLLECTIONS_NAME
        self.history: list[HistoryResponseDTO] | list = []
        self.config = config

    def _get_giga_token(self):
        rq_uid = str(uuid.uuid4())
        url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
            'RqUID': rq_uid,
            'Authorization': f'Basic {self.config.GIGACHAT_AUTH_KEY}'
        }
        payload = {'scope': 'GIGACHAT_API_PERS'}

        try:
            # verify=False нужен для API Сбера
            response = requests.post(url, headers=headers, data=payload, verify=False)
            if response.status_code == 200:
                return response.json()['access_token']
            else:
                self.logger.error(f"GigaChat Token Error: {response.text}")
                return None
        except Exception as e:
            self.logger.error(f"Connection error (Token): {e}")
            return None

    def _call_gigachat_api(self, token, prompt):
        url = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
        
        payload = json.dumps({
            "model": "GigaChat",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.7,
            "top_p": 0.1,
            "n": 1,
            "stream": False,
            "max_tokens": 1024,
            "repetition_penalty": 1
        })
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'Bearer {token}'
        }

        try:
            response = requests.post(url, headers=headers, data=payload, verify=False)
            if response.status_code == 200:
                return response.json()['choices'][0]['message']['content']
            else:
                error_msg = f"Ошибка API Гигачата: {response.status_code} - {response.text}"
                self.logger.error(error_msg)
                return error_msg
        except Exception as e:
            self.logger.error(f"Ошибка соединения с Гигачатом: {e}")
            return "Не удалось связаться с нейросетью."
        
    async def _private_method_1_encode_topic(self, query_topic):
        if not query_topic:
            raise ValueError("Query topic cannot be empty.")
        return self.model.encode(query_topic)
        
    def _private_method_2_search_qdrant(self, embedding):
        if embedding is None:
            raise TypeError("Embedding cannot be None.")
        
        results = self.qdrant.query_points(
            collection_name=self.collections_name,
            query=embedding,
            limit=3,
        )
        self.logger.info(f"Найдено документов: {len(results.points)}")
        return [res.payload for res in results.points]

    
    async def _private_method_3_generate_answer(self, nearests_texts, query_topic):
        context_parts = []
        for i, doc in enumerate(nearests_texts, 1):
            text = doc.get('text', '') or doc.get('content', '') or str(doc)
            title = doc.get('title', 'Документ')
            context_parts.append(f"--- {title} ---\n{text}")
        
        full_context = "\n\n".join(context_parts)

        prompt = (
            f"Ты — финансовый ассистент. Ответь на вопрос пользователя, опираясь ТОЛЬКО на предоставленный контекст.\n"
            f"Если информации нет, скажи: 'В базе знаний нет ответа на этот вопрос'.\n\n"
            f"КОНТЕКСТ:\n{full_context}\n\n"
            f"ВОПРОС ПОЛЬЗОВАТЕЛЯ:\n{query_topic}"
        )

        print("\n" + "="*30)
        print(" [DEBUG] Generated Prompt: ")
        print(prompt)
        print("="*30 + "\n")

        token = self._get_giga_token()
        if not token:
            return {"answer": "Ошибка авторизации во внешней системе."}

        answer_text = self._call_gigachat_api(token, prompt)
        
        return {"answer": answer_text}

    async def processes_query(self, query: Query) -> QueryResults:
        """Processes the query topic and returns the final result."""
        start_time = time.time()

        embedding = await self._private_method_1_encode_topic(query.query_topic)

        nearests_texts = self._private_method_2_search_qdrant(embedding)

        model_answer = await self._private_method_3_generate_answer(nearests_texts, query.query_topic)

        self.history.append(HistoryItemDTO(
            query=query.query_topic,
            response=model_answer['answer'],
            timestamp=time.time(),
            duration=time.time() - start_time,
        ))
        
        return QueryResults(text=model_answer['answer'])
