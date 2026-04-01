import time
from qdrant_client import QdrantClient
from sentence_transformers import CrossEncoder

from domain.query.query import Query, QueryResults
from domain.query.delivery.dto.dto import HistoryResponseDTO, HistoryItemDTO, FeedbackRequestDTO
from domain.query.usecase.i_query_usecase import IQueryUsecase
from infrastructure.db.qdrand import get_embedded_model
from infrastructure.feedback.feedback_storage import FeedbackStorage, TripletRecord

import uuid
import json
import requests

MAX_TOKENS = 4096
CROSS_ENCODER_MODEL_NAME = "cross-encoder/mmarco-mMiniLMv2-L12-H384-v1"
CROSS_ENCODER_LIMIT = 10
QDRANT_SEARCH_LIMIT = 20


class QueryUsecase(IQueryUsecase):
    def __init__(self, qdrant: QdrantClient, logger, config):
        self.qdrant = qdrant
        self.logger = logger
        self.model = get_embedded_model()
        self.cross_encoder = CrossEncoder(CROSS_ENCODER_MODEL_NAME)
        self.collections_name = config.QDRANT_COLLECTION_NAME
        self.history = []
        self.config = config
        self.feedback_storage = FeedbackStorage()
        self._query_context: dict = {}  # query_id -> {query, candidates}

    def _get_giga_token(self):
        rq_uid = str(uuid.uuid4())
        url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "RqUID": rq_uid,
            "Authorization": f"Basic {self.config.GIGACHAT_AUTH_KEY}",
        }
        payload = {"scope": "GIGACHAT_API_PERS"}

        try:
            # verify=False нужен для API Сбера
            response = requests.post(url, headers=headers, data=payload, verify=False)
            if response.status_code == 200:
                return response.json()["access_token"]
            else:
                self.logger.error(f"GigaChat Token Error: {response.text}")
                return None
        except Exception as e:
            self.logger.error(f"Connection error (Token): {e}")
            return None

    def _call_gigachat_api(self, token, prompt):
        url = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"

        payload = json.dumps(
            {
                "model": "GigaChat",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.7,
                "top_p": 0.1,
                "n": 1,
                "stream": False,
                "max_tokens": 1024,
                "repetition_penalty": 1,
            }
        )

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }

        try:
            response = requests.post(url, headers=headers, data=payload, verify=False)
            if response.status_code == 200:
                return response.json()["choices"][0]["message"]["content"]
            else:
                error_msg = (
                    f"Ошибка API Гигачата: {response.status_code} - {response.text}"
                )
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
            limit=QDRANT_SEARCH_LIMIT,
        )
        self.logger.info(f"Найдено кандидатов: {len(results.points)}")
        return results.points

    def _rerank_with_cross_encoder(self, query: str, candidates: list) -> list:
        if not candidates:
            return []

        pairs = []
        for candidate in candidates:
            text = candidate.payload.get("text", "")
            pairs.append([query, text])

        scores = self.cross_encoder.predict(pairs)
        
        scored_candidates = list(zip(scores, candidates))
        scored_candidates.sort(key=lambda x: x[0], reverse=True)
        
        reranked = scored_candidates[:CROSS_ENCODER_LIMIT]
        self.logger.info(f"После кросс-энкодер ре-ранжирования: {len(reranked)} документов")
        
        return [c.payload for _, c in reranked]

    async def _private_method_3_generate_answer(self, nearests_texts, query_topic):
        context_parts = []
        for i, doc in enumerate(nearests_texts, 1):
            text = doc.get("text", "") or doc.get("content", "") or str(doc)
            title = doc.get("title", "Документ")
            context_parts.append(f"--- {title} ---\n{text}")

        full_context = "\n\n".join(context_parts)

        prompt = (
            f"Ты — финансовый ассистент. Ответь на вопрос пользователя, опираясь ТОЛЬКО на предоставленный контекст.\n"
            f"Если информации нет, скажи: 'В базе знаний нет ответа на этот вопрос'.\n\n"
            f"КОНТЕКСТ:\n{full_context}\n\n"
            f"ВОПРОС ПОЛЬЗОВАТЕЛЯ:\n{query_topic}"
        )

        print("\n" + "=" * 30)
        print(" [DEBUG] Generated Prompt: ")
        print(prompt)
        print("=" * 30 + "\n")

        token = self._get_giga_token()
        if not token:
            return {"answer": "Ошибка авторизации во внешней системе."}

        answer_text = self._call_gigachat_api(token, prompt)

        return {"answer": answer_text}

    async def processes_query(self, query: Query) -> QueryResults:
        """Processes the query topic and returns the final result."""
        start_time = time.time()
        query_id = str(uuid.uuid4())

        embedding = await self._private_method_1_encode_topic(query.query_topic)
        candidates = self._private_method_2_search_qdrant(embedding)
        reranked = self._rerank_with_cross_encoder(query.query_topic, candidates)

        # Save context for feedback processing
        self._query_context[query_id] = {
            "query": query.query_topic,
            "all_candidates": candidates,
            "reranked": reranked,
        }

        model_answer = await self._private_method_3_generate_answer(
            reranked, query.query_topic
        )

        doc_ids = list(range(len(reranked)))

        self.history.append(
            HistoryItemDTO(
                query=query.query_topic,
                response=model_answer["answer"],
                timestamp=time.time(),
                duration=time.time() - start_time,
            )
        )

        return QueryResults(
            text=model_answer["answer"],
            query_id=query_id,
            retrieved_doc_ids=doc_ids,
        )

    def save_feedback(self, feedback: FeedbackRequestDTO) -> int:
        """Saves feedback and creates training triplets. Returns number of triplets created."""
        ctx = self._query_context.get(feedback.query_id)
        if not ctx:
            self.logger.warning(f"No context found for query_id: {feedback.query_id}")
            return 0

        query = ctx["query"]
        reranked = ctx["reranked"]
        all_candidates = ctx.get("all_candidates", [])

        triplets_count = 0
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%S")

        if feedback.liked:
            # Positive: top reranked doc(s)
            positive = reranked[0] if reranked else None
            if not positive:
                return 0

            positive_text = positive.get("text", "")

            # Negative: low-ranking candidates from original search that didn't make top-K
            negative_candidates = []
            for c in all_candidates:
                text = c.payload.get("text", "")
                # Skip if this doc is in top reranked
                if any(d.get("text") == text for d in reranked[:3]):
                    continue
                negative_candidates.append(text)

            # Pick one negative
            negative = negative_candidates[-1] if negative_candidates else ""

            if positive_text and negative:
                triplet = TripletRecord(
                    query=query,
                    query_id=feedback.query_id,
                    positive_doc=positive_text,
                    negative_doc=negative,
                    timestamp=timestamp,
                )
                self.feedback_storage.save_triplet(triplet)
                triplets_count += 1

        else:
            # Disliked: top docs are negative examples
            for doc in reranked[:2]:
                negative_text = doc.get("text", "")

                # Positive from user-specified relevant docs or from lower-ranked candidates
                if feedback.relevant_doc_ids:
                    # User told us which docs are relevant
                    for c in all_candidates:
                        cid = getattr(c, "id", None)
                        if cid in feedback.relevant_doc_ids:
                            positive_text = c.payload.get("text", "")
                            if positive_text and positive_text != negative_text:
                                triplet = TripletRecord(
                                    query=query,
                                    query_id=feedback.query_id,
                                    positive_doc=positive_text,
                                    negative_doc=negative_text,
                                    timestamp=timestamp,
                                )
                                self.feedback_storage.save_triplet(triplet)
                                triplets_count += 1
                                break
                else:
                    # Use docs ranked 3-5 as positive relative to top 1-2
                    for doc_pos in reranked[2:4]:
                        positive_text = doc_pos.get("text", "")
                        if positive_text and positive_text != negative_text:
                            triplet = TripletRecord(
                                query=query,
                                query_id=feedback.query_id,
                                positive_doc=positive_text,
                                negative_doc=negative_text,
                                timestamp=timestamp,
                            )
                            self.feedback_storage.save_triplet(triplet)
                            triplets_count += 1
                            break

        # Clean up context
        self._query_context.pop(feedback.query_id, None)

        self.logger.info(f"Saved {triplets_count} triplet(s) for query_id: {feedback.query_id}")
        return triplets_count
